package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
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
)

const (
	HdrKinesisPartitionKey     = "Kinesis-Partition-Key"
	HdrKinesisArrivalTimestamp = "Kinesis-Arrival-Timestamp"

	defaultBatchSize = 1000
)

type subjectTemplateData struct {
	StreamName     string
	ShardID        string
	PartitionKey   string
	SequenceNumber string
	Data           any
}

type StreamNATSConfig struct {
	// NATS subject to send messages to.
	Subject string

	subjectT *template.Template
}

type StreamConfig struct {
	// This can be "json" or "" (empty string) indicating it is opaque.
	Encoding string

	// Optional pre-defined list of shards on the stream. If not
	// provided, the stream will be queried for the list of shards
	// and all shards will be fanned-in to the NATS subject.
	Shards []int

	NATS *StreamNATSConfig
}

type NATSConfig struct {
	Context string
}

type Config struct {
	// Map of streams by name
	Kinesis map[string]*StreamConfig

	NATS *NATSConfig
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

		if k.NATS == nil {
			k.NATS = &StreamNATSConfig{}
		}

		if k.NATS.Subject == "" {
			k.NATS.Subject = "{{.StreamName}}"
		}

		nt, err := template.New("subject").Parse(k.NATS.Subject)
		if err != nil {
			return fmt.Errorf("nats subject template: %w", err)
		}
		k.NATS.subjectT = nt
	}

	if c.NATS == nil {
		c.NATS = &NATSConfig{}
	}

	return nil
}

func ReadConfig(path string) (*Config, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config read: %w", err)
	}

	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("config unmarshal: %w", err)
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return &c, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	configPath := flag.Arg(0)
	if configPath == "" {
		return errors.New("config path required")
	}

	c, err := ReadConfig(configPath)
	if err != nil {
		return err
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

	// Create a bucket to store the shard offsets.
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "kinesis",
	})
	if err != nil {
		return fmt.Errorf("nats kv: %w", err)
	}

	pw := progress.NewWriter()
	pw.SetNumTrackersExpected(10) // TODO calculate
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.SetUpdateFrequency(250 * time.Millisecond)
	pw.Style().Visibility.Speed = true
	pw.Style().Visibility.SpeedOverall = true

	go pw.Render()

	wg := &sync.WaitGroup{}
	errch := make(chan error, 100)

	for sn, sc := range c.Kinesis {
		shards, err := getShards(ctx, kc, sn)
		if err != nil {
			return fmt.Errorf("%s: get shards: %w", sn, err)
		}

		for _, sh := range shards {
			sid := aws.ToString(sh.ShardId)

			// Separate JS context to manage async pending state for
			// each shard.
			js, _ := nc.JetStream()

			s := &ShardReader{
				batchSize:  defaultBatchSize,
				streamName: sn,
				kv:         kv,
				pw:         pw,
				kc:         kc,
				nc:         nc,
				js:         js,
				nst:        sc.NATS.subjectT,
				shardId:    sid,
				encoding:   sc.Encoding,
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
	}
	cancel()
	wg.Wait()
	pw.Stop()

	return err
}

// getShards returns all shards for a given stream. This will be used
func getShards(ctx context.Context, kc *kinesis.Client, streamName string) ([]types.Shard, error) {
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
	batchSize  int
	kc         *kinesis.Client
	nc         *nats.Conn
	js         nats.JetStreamContext
	kv         nats.KeyValue
	pw         progress.Writer
	nst        *template.Template
	encoding   string
	streamName string
	shardId    string
	errch      chan error
	kvSeq      uint64
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
		return err
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

	if seq == "" {
		in.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
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

func (s *ShardReader) publishMsgs(msgs []*nats.Msg, pfs []nats.PubAckFuture) ([]nats.PubAckFuture, error) {
	for _, msg := range msgs {
		pf, err := s.js.PublishMsgAsync(msg)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: nats publish message: %w", s.streamName, s.shardId, err)
		}
		pfs = append(pfs, pf)
	}
	return pfs, nil
}

func (s *ShardReader) checkAcks(pfs []nats.PubAckFuture) error {
	for _, pf := range pfs {
		select {
		case <-pf.Ok():
		case err := <-pf.Err():
			return fmt.Errorf("%s: %s: nats publish ack: %w", s.streamName, s.shardId, err)
		}
	}
	return nil
}

func (s *ShardReader) Run(ctx context.Context) error {
	shardIterator, err := s.getShardIterator(ctx)
	if err != nil {
		return fmt.Errorf("%s: %s: %w", s.streamName, s.shardId, err)
	}

	in := &kinesis.GetRecordsInput{
		Limit:         aws.Int32(int32(s.batchSize)),
		ShardIterator: aws.String(shardIterator),
	}

	msgs := make([]*nats.Msg, 0, s.batchSize)
	pfs := make([]nats.PubAckFuture, 0, s.batchSize)

	// Buffer for the subject template.
	subBuf := bytes.NewBuffer(nil)

	td := subjectTemplateData{
		StreamName: s.streamName,
		ShardID:    s.shardId,
		Data:       map[string]any{},
	}

	t := &progress.Tracker{
		Message: fmt.Sprintf("%s-%s", s.streamName, s.shardId),
		Units:   progress.UnitsDefault,
	}

	s.pw.AppendTracker(t)

	var lastSeq string

	for {
		msgs := msgs[:0]

		out, err := s.kc.GetRecords(ctx, in)
		if err == context.Canceled {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %s: kinesis get records: %w", s.streamName, s.shardId, err)
		}

		if len(out.Records) > 0 {
			for _, r := range out.Records {
				td.PartitionKey = aws.ToString(r.PartitionKey)
				td.SequenceNumber = aws.ToString(r.SequenceNumber)

				if s.encoding == "json" {
					if err := json.Unmarshal(r.Data, &td.Data); err != nil {
						return fmt.Errorf("%s: %s: parse record data: %w", s.streamName, s.shardId, err)
					}
				} else {
					td.Data = map[string]any{}
				}

				subBuf.Reset()

				if err := s.nst.Execute(subBuf, &td); err != nil {
					return fmt.Errorf("%s: %s: execute subject template: %w", s.streamName, s.shardId, err)
				}

				subject := subBuf.String()

				msg := nats.NewMsg(subject)
				msg.Header.Set(nats.MsgIdHdr, fmt.Sprintf("%s|%s|%s", s.streamName, s.shardId, aws.ToString(r.SequenceNumber)))
				msg.Header.Set(HdrKinesisPartitionKey, aws.ToString(r.PartitionKey))
				msg.Header.Set(HdrKinesisArrivalTimestamp, r.ApproximateArrivalTimestamp.String())
				msg.Data = r.Data

				msgs = append(msgs, msg)
				lastSeq = aws.ToString(r.SequenceNumber)
			}

			for {
				pfs = pfs[:0]

				pfs, err = s.publishMsgs(msgs, pfs)
				if err != nil {
					// TODO: check if this is a recoverable error before shutting down.
					return err
				}

				// Wait for all messages to be acked.
				<-s.js.PublishAsyncComplete()

				// Loop until all messages are acked. Since we are using deduplication,
				// we can safely retry publishing messages.
				err = s.checkAcks(pfs)
				if err != nil {
					return err
				}

				// All good.
				break
			}

			for {
				err := s.setSequence(lastSeq)
				if err == nil {
					break
				}
				log.Printf("%s: %s: %s", s.streamName, s.shardId, err)
			}

			t.Increment(int64(len(pfs)))
		}

		if out.NextShardIterator == nil {
			break
		}

		in = &kinesis.GetRecordsInput{
			ShardIterator: out.NextShardIterator,
		}
	}

	return nil
}
