package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"

	"net/http"
	_ "net/http/pprof"
)

const (
	HdrKinesisStreamName       = "Kinesis-Stream-Name"
	HdrKinesisShardId          = "Kinesis-Shard-Id"
	HdrKinesisPartitionKey     = "Kinesis-Partition-Key"
	HdrKinesisSequenceNumber   = "Kinesis-Sequence-Number"
	HdrKinesisArrivalTimestamp = "Kinesis-Arrival-Timestamp"

	// Default to the max get batch size for each shard.
	defaultShardBatchSize = 10_000

	// Default to 100 messages per async publish per shard.
	defaultPublishBatchSize = 100

	// Default bucket name.
	defaultBucketName = "kinesis-bridge"
)

func sleepJitter(d time.Duration, factor float64) {
	time.Sleep(d + time.Duration(rand.Float64()*float64(d)*factor))
}

type subjectTemplateData struct {
	StreamName     string
	ShardID        string
	PartitionKey   string
	SequenceNumber string
	Data           any
}

type StreamNATSConfig struct {
	// NATS subject to send messages to.
	Subject string `yaml:"subject"`

	// Subject to send messages that could not parse properly.
	// This should be a subject bound to the same stream.
	DLQ string `yaml:"dlq"`

	subjectT *template.Template
	dlqT     *template.Template
}

type StreamConfig struct {
	// This can be "json" or "" (empty string) indicating it is opaque.
	Encoding string `yaml:"encoding"`

	// Optional pre-defined list of shards on the stream. If not
	// provided, the stream will be queried for the list of shards
	// and all shards will be fanned-in to the NATS subject.
	Shards []int `yaml:"shards"`

	// Optional starting position for shards. Options are "new" to start *after*
	// last message in the shard or "oldest" to start with the earliest message
	// in each shard.
	StartPosition string `yaml:"start_position"`

	NATS *StreamNATSConfig `yaml:"nats"`
}

type NATSConfig struct {
	Context string `yaml:"context"`

	Bucket string `yaml:"bucket"`
}

type Config struct {
	// Map of streams by name
	Kinesis map[string]*StreamConfig `yaml:"kinesis"`

	// NATS config for the entire process.
	NATS *NATSConfig `yaml:"nats"`

	// BatchSize should be set relative to the expected number of shards.
	BatchSize int `yaml:"batch_size"`
}

func (c *Config) Validate() error {
	for _, k := range c.Kinesis {
		switch k.Encoding {
		case "json":
		case "", "bytes":
			k.Encoding = "bytes"
		default:
			return fmt.Errorf(`invalid encoding: %q. must be "bytes" (default) or "json"`, k.Encoding)
		}

		// Default to new records.
		if k.StartPosition == "" {
			k.StartPosition = "new"
		} else if k.StartPosition != "new" && k.StartPosition != "oldest" {
			return fmt.Errorf(`invalid start position: %q. must be "new" (default) or "oldest"`, k.StartPosition)
		}

		if k.NATS == nil {
			k.NATS = &StreamNATSConfig{}
		}

		if k.NATS.Subject == "" {
			k.NATS.Subject = "{{.StreamName}}.{{.ShardID}}.{{.PartitionKey}}"
			k.NATS.DLQ = "{{.StreamName}}.{{.ShardID}}.dlq"
		}

		nt, err := template.New("subject").Parse(k.NATS.Subject)
		if err != nil {
			return fmt.Errorf("nats subject template: %w", err)
		}
		k.NATS.subjectT = nt

		nt, err = template.New("subject").Parse(k.NATS.DLQ)
		if err != nil {
			return fmt.Errorf("nats DLQ template: %w", err)
		}
		k.NATS.dlqT = nt
	}

	if c.NATS == nil {
		c.NATS = &NATSConfig{}
	}

	if c.NATS.Bucket == "" {
		c.NATS.Bucket = defauttBucketName
	}

	if c.BatchSize == 0 {
		c.BatchSize = defaultPublishBatchSize
	}

	return nil
}

func ParseConfig(b []byte) (*Config, error) {
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("config unmarshal: %w", err)
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return &c, nil
}

func ReadConfig(path string) (*Config, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config read: %w", err)
	}

	return ParseConfig(b)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		noProgress  bool
		batchSize   int
		enablePprof bool
	)

	flag.BoolVar(&noProgress, "no-progress", false, "Disable progress rendering.")
	flag.IntVar(&batchSize, "batch-size", 0, "Number of records to fetch and publish per shard.")
	flag.BoolVar(&enablePprof, "pprof", false, "Enable pprof.")

	flag.Parse()

	configPath := flag.Arg(0)
	if configPath == "" {
		return errors.New("config path required")
	}

	c, err := ReadConfig(configPath)
	if err != nil {
		return err
	}

	// Override config batch size if provided.
	if batchSize > 0 {
		c.BatchSize = batchSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	kc := kinesis.NewFromConfig(cfg)

	nc, err := natscontext.Connect(c.NATS.Context)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("nats jetstream: %w", err)
	}

	// Get the KV bucket used to stored the shard offsets.
	kv, err := js.KeyValue(c.NATS.Bucket)
	if err != nil {
		return fmt.Errorf("nats kv: %w", err)
	}

	pw := progress.NewWriter()
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.SetUpdateFrequency(250 * time.Millisecond)
	pw.Style().Visibility.Speed = true
	pw.Style().Visibility.SpeedOverall = true

	topicShards := make(map[string][]types.Shard)
	var numShards int

	// Fetch a snapshot of all the shards across all topics.
	// TODO: rebalancing of shards across workers is not yet implemented.
	for sn := range c.Kinesis {
		shards, err := getShards(ctx, kc, sn)
		if err != nil {
			return fmt.Errorf("%s: get shards: %w", sn, err)
		}
		topicShards[sn] = shards
		numShards += len(shards)
	}

	wg := &sync.WaitGroup{}
	errch := make(chan error, numShards)

	pw.SetNumTrackersExpected(numShards)
	if !noProgress {
		go pw.Render()
		defer pw.Stop()
	}

	if enablePprof {
		go http.ListenAndServe("localhost:6060", nil)
	}

	for sn, sc := range c.Kinesis {
		shards := topicShards[sn]

		for _, sh := range shards {
			sid := aws.ToString(sh.ShardId)

			// Separate JS context to manage async pending state for
			// each shard.
			js, _ := nc.JetStream()

			s := &ShardReader{
				startPosition: sc.StartPosition,
				batchSize:     c.BatchSize,
				streamName:    sn,
				kv:            kv,
				pw:            pw,
				kc:            kc,
				nc:            nc,
				js:            js,
				nst:           sc.NATS.subjectT,
				dlq:           sc.NATS.dlqT,
				shardId:       sid,
				encoding:      sc.Encoding,
			}

			go func(s *ShardReader) {
				defer wg.Done()
				wg.Add(1)
				err := s.Run(ctx)
				if err != nil {
					errch <- fmt.Errorf("%s: %s: %w", s.streamName, s.shardId, err)
				}
			}(s)
		}
	}

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)

	select {
	case err = <-errch:
	case <-sigch:
		log.Println("interrupt received")
	}
	cancel()
	wg.Wait()

	return err
}

// getShards returns all shards for a given stream. This will be used
func getShards(ctx context.Context, kc KinesisClient, streamName string) ([]types.Shard, error) {
	var shards []types.Shard

	in := &kinesis.ListShardsInput{
		StreamName: &streamName,
	}

	for {
		out, err := kc.ListShards(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("list shards: %w", err)
		}

		shards = append(shards, out.Shards...)
		if out.NextToken == nil {
			break
		}

		// Token without the stream name must be used for subsequent
		// iterations.
		in = &kinesis.ListShardsInput{
			NextToken: out.NextToken,
		}
	}

	return shards, nil
}

type ShardReader struct {
	startPosition string
	batchSize     int
	kc            KinesisClient
	nc            *nats.Conn
	js            nats.JetStreamContext
	kv            nats.KeyValue
	pw            progress.Writer
	nst           *template.Template
	dlq           *template.Template
	encoding      string
	streamName    string
	shardId       string
	errch         chan error
	kvSeq         uint64
	msgs          []*nats.Msg
	pfs           []nats.PubAckFuture
	h             hash.Hash
	td            *subjectTemplateData
	subBuf        *bytes.Buffer
	lastSeq       string

	maxCount int
	count    int
}

// TODO: apply locking by shard to prevent multiple handlers.
func (s *ShardReader) getSequence() (string, error) {
	key := fmt.Sprintf("%s.%s", s.streamName, s.shardId)
	e, err := s.kv.Get(key)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	s.kvSeq = e.Revision()
	return string(e.Value()), nil
}

// setSequence updates the last published shard sequence.
func (s *ShardReader) setSequence(seq string) error {
	key := fmt.Sprintf("%s.%s", s.streamName, s.shardId)

	rev, err := s.kv.Update(key, []byte(seq), s.kvSeq)
	if err != nil {
		return fmt.Errorf("kv-set: %w", err)
	}
	s.kvSeq = rev

	return nil
}

// getShardIterator returns a shard iterator for a given shard. This will be used
// to bootstrap reading records from a shard.
func (s *ShardReader) getShardIterator(ctx context.Context) (string, error) {
	seq, err := s.getSequence()
	if err != nil {
		return "", err
	}

	in := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(s.shardId),
		StreamName: aws.String(s.streamName),
	}

	// No existing sequence, start with the configured start position.
	if seq == "" {
		if s.startPosition == "earliest" {
			in.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
		} else {
			in.ShardIteratorType = types.ShardIteratorTypeLatest
		}
	} else {
		in.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		in.StartingSequenceNumber = aws.String(seq)
	}

	out, err := s.kc.GetShardIterator(ctx, in)
	if err != nil {
		return "", fmt.Errorf("get shard iterator: %w", err)
	}

	return *out.ShardIterator, nil
}

func (s *ShardReader) publishMsgs() error {
	s.pfs = s.pfs[:0]

	for _, msg := range s.msgs {
		pf, err := s.js.PublishMsgAsync(msg)
		if err != nil {
			return fmt.Errorf("%s: %s: nats publish message: %w", s.streamName, s.shardId, err)
		}
		s.pfs = append(s.pfs, pf)
	}
	return nil
}

func (s *ShardReader) checkAcks() error {
	for _, pf := range s.pfs {
		select {
		case <-pf.Ok():
		case err := <-pf.Err():
			return fmt.Errorf("%s: %s: nats publish ack: %w", s.streamName, s.shardId, err)
		}
	}
	return nil
}

func (s *ShardReader) prepareMsgs(records []types.Record) {
	s.msgs = s.msgs[:0]

	for _, r := range records {
		msg := s.prepareMsg(&r)
		s.msgs = append(s.msgs, msg)
	}
}

func (s *ShardReader) prepareMsg(r *types.Record) *nats.Msg {
	s.td.PartitionKey = aws.ToString(r.PartitionKey)
	s.td.SequenceNumber = aws.ToString(r.SequenceNumber)
	// Reset the data.
	s.td.Data = map[string]any{}

	// Use the standard subject template.
	st := s.nst

	s.subBuf.Reset()

	if s.encoding == "json" {
		if err := json.Unmarshal(r.Data, &s.td.Data); err != nil {
			// Failed to parse the data for template, so fallback to
			// the DLQ template.
			log.Printf("%s: %s: parse record data: %s", s.streamName, s.shardId, err)
			st = s.dlq
		}
	}

	if err := st.Execute(s.subBuf, &s.td); err != nil {
		// Nothing else to fallback to.
		if st == s.dlq {
			panic(fmt.Sprintf("%s: %s: execute DLQ subject template: %s", s.streamName, s.shardId, err))
		}

		log.Printf("%s: %s: execute subject template: %s", s.streamName, s.shardId, err)

		s.subBuf.Reset()
		if err := s.dlq.Execute(s.subBuf, &s.td); err != nil {
			panic(fmt.Sprintf("%s: %s: execute DLQ subject template: %s", s.streamName, s.shardId, err))
		}
	}

	subject := s.subBuf.String()

	s.h.Reset()
	s.h.Write([]byte(fmt.Sprintf("%s|%s|%s|%s", s.streamName, s.shardId, aws.ToString(r.PartitionKey), aws.ToString(r.SequenceNumber))))

	msg := nats.NewMsg(subject)
	msg.Header.Set(nats.MsgIdHdr, hex.EncodeToString(s.h.Sum(nil)))
	msg.Header.Set(HdrKinesisStreamName, s.streamName)
	msg.Header.Set(HdrKinesisShardId, s.shardId)
	msg.Header.Set(HdrKinesisPartitionKey, aws.ToString(r.PartitionKey))
	msg.Header.Set(HdrKinesisSequenceNumber, aws.ToString(r.SequenceNumber))
	msg.Header.Set(HdrKinesisArrivalTimestamp, r.ApproximateArrivalTimestamp.String())
	msg.Data = r.Data

	return msg
}

func (s *ShardReader) publishAndAckMsgs(records []types.Record) error {
	n := len(records)
	s.count += n

	for n > 0 {
		// Grab a slice of the records to publish.
		i := s.batchSize
		if len(records) < i {
			i = len(records)
		}

		s.prepareMsgs(records[:i])

		// TODO: handle retryable errors...
		err := s.publishMsgs()
		if err != nil {
			return err
		}

		// Wait for all messages to be acked.
		<-s.js.PublishAsyncComplete()

		// Loop until all messages are acked. Since we are using deduplication,
		// we can safely retry publishing messages.
		err = s.checkAcks()
		if err != nil {
			return err
		}

		lastSeq := aws.ToString(records[i-1].SequenceNumber)
		s.setSequence(lastSeq)

		n -= i
		records = records[i:]
	}

	return nil
}

func (s *ShardReader) Run(ctx context.Context) error {
	// Initialize local shared variables.
	s.msgs = make([]*nats.Msg, 0, s.batchSize)
	s.pfs = make([]nats.PubAckFuture, 0, s.batchSize)

	// Buffer for the subject template.
	s.subBuf = bytes.NewBuffer(nil)

	s.td = &subjectTemplateData{
		StreamName: s.streamName,
		ShardID:    s.shardId,
		Data:       map[string]any{},
	}
	s.h = sha256.New()

	t := &progress.Tracker{
		Message: fmt.Sprintf("%s-%s", s.streamName, s.shardId),
		Units:   progress.UnitsDefault,
	}

	s.pw.AppendTracker(t)

	si, err := s.getShardIterator(ctx)
	if err != nil {
		return fmt.Errorf("%s: %s: %w", s.streamName, s.shardId, err)
	}

	in := &kinesis.GetRecordsInput{
		Limit:         aws.Int32(int32(defaultShardBatchSize)),
		ShardIterator: aws.String(si),
	}

	for {
		if s.maxCount > 0 && s.count >= s.maxCount {
			break
		}

		out, err := s.kc.GetRecords(ctx, in)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		if err != nil {
			log.Printf("kinesis get records: %s", err)
			sleepJitter(50*time.Millisecond, 2)
			continue
		}

		err = s.publishAndAckMsgs(out.Records)
		if err != nil {
			return fmt.Errorf("%s: %s: %w", s.streamName, s.shardId, err)
		}

		t.Increment(int64(len(out.Records)))

		if out.NextShardIterator == nil {
			break
		}

		in = &kinesis.GetRecordsInput{
			ShardIterator: out.NextShardIterator,
		}
	}

	return nil
}

type KinesisClient interface {
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
}
