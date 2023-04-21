package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"
)

const (
	payload = `{"facility_code": "AL207", "pointid": "1301115", "tagname": "NAH-ELEC-E300-72882_M_CONV_B_SCALP_DISC_SCRN_FEED.Test Trip", "col_timestamp": "02/21/2023 19:29:35.000", "numericvalue": "0", "stringvalue": "", "batch_id": "NAH-03S-20230221132923"}`

	biConst = "49639849650791367338098013431253362638742069108073825778"
)

func newMockKinesisClient() *mockKinesisClient {
	records := make([]types.Record, 10_000, 10_000)
	arrivalTime := time.Now().UTC()

	rr := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Start with different random sequence.
	br := big.NewInt(0)
	br.SetString(biConst, 10)

	bi := big.NewInt(0).Rand(rr, br)

	for i := 0; i < 10_000; i++ {
		seq := bi.Add(bi, big.NewInt(1)).Text(10)
		records[i] = types.Record{
			Data:                        []byte(payload),
			PartitionKey:                aws.String("100"),
			SequenceNumber:              aws.String(seq),
			ApproximateArrivalTimestamp: aws.Time(arrivalTime),
		}
	}

	return &mockKinesisClient{
		records: records,
		bi:      bi,
	}
}

type mockKinesisClient struct {
	records []types.Record
	bi      *big.Int
}

func (c *mockKinesisClient) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	// Update the sequence number on subsequent calls.
	for i := 0; i < 10_000; i++ {
		seq := c.bi.Add(c.bi, big.NewInt(1)).Text(10)
		c.records[i].SequenceNumber = aws.String(seq)
	}

	return &kinesis.GetRecordsOutput{
		Records:           c.records,
		NextShardIterator: aws.String("A"),
	}, nil
}

func (c *mockKinesisClient) GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("A"),
	}, nil
}

func (c *mockKinesisClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	return nil, nil
}

const (
	testConfig = `---
kinesis:
  test-stream:
    encoding: json
    nats:
      subject: "sensor-data.{{.Data.facility_code}}.{{.Data.pointid}}"
      dlq: "sensor-data.dlq"
`
)

func setupServer(tb testing.TB) *server.Server {
	ns, err := server.NewServer(&server.Options{
		JetStream: true,
		Port:      -1,
	})
	go ns.Start()

	ns.ReadyForConnections(5 * time.Second)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		tb.Errorf("nats connect: %s", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		tb.Errorf("nats jetstream: %s", err)
	}

	js.DeleteStream("test-stream")
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "test-stream",
		Subjects: []string{"sensor-data.>"},
	})
	if err != nil {
		tb.Errorf("nats stream: %s", err)
	}

	// Create a bucket to store the shard offsets.
	js.DeleteStream("KV_test")
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "test",
	})
	if err != nil {
		tb.Errorf("nats kv: %s", err)
	}

	return ns
}

func cleanupAssets(nc *nats.Conn) {
	js, _ := nc.JetStream()
	js.DeleteStream("test-stream")
	js.DeleteStream("KV_test")
}

func setupShardReader(tb testing.TB, nc *nats.Conn, kc KinesisClient, shardId string) *ShardReader {
	c, _ := ParseConfig([]byte(testConfig))

	pw := progress.NewWriter()

	sc := c.Kinesis["test-stream"]

	js, _ := nc.JetStream()
	kv, _ := js.KeyValue("test")

	s := ShardReader{
		batchSize:  c.BatchSize,
		streamName: "test-stream",
		kv:         kv,
		pw:         pw,
		kc:         kc,
		nc:         nc,
		js:         js,
		nst:        sc.NATS.subjectT,
		dlq:        sc.NATS.dlqT,
		shardId:    shardId,
		encoding:   sc.Encoding,
		msgs:       make([]*nats.Msg, 0, c.BatchSize),
		pfs:        make([]nats.PubAckFuture, 0, c.BatchSize),
		subBuf:     bytes.NewBuffer(nil),
		td: &subjectTemplateData{
			StreamName: "test-stream",
			ShardID:    shardId,
			Data:       map[string]any{},
		},
		h: sha256.New(),
	}

	return &s
}

func BenchmarkShardReader_prepareMsg(b *testing.B) {
	kc := newMockKinesisClient()

	ns := setupServer(b)
	nc, _ := nats.Connect(ns.ClientURL())

	nc.Close()
	ns.Shutdown()

	s := setupShardReader(b, nc, kc, "shardId-000000000000")

	b.ResetTimer()

	r := kc.records[0]
	for i := 0; i < b.N; i++ {
		s.prepareMsg(&r)
	}
}

func natsMsgSize(m *nats.Msg) int {
	var size int
	size += len(m.Data)
	for k, v := range m.Header {
		size += len(k)
		size += len(v[0])
	}
	return size
}

func TestThroughputWithMock(t *testing.T) {
	ns := setupServer(t)
	defer ns.Shutdown()

	pubs := 120
	bm := bench.NewBenchmark("Kinesis", 0, pubs)

	// Calculated with headers included...
	msgSize := 549

	ctx := context.Background()

	wg := sync.WaitGroup{}

	for i := 0; i < pubs; i++ {
		wg.Add(1)
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			t.Errorf("nats connect: %s", err)
		}
		defer nc.Close()

		kc := newMockKinesisClient()

		shardId := fmt.Sprintf("shardId-%012d", i)
		s := setupShardReader(t, nc, kc, shardId)
		s.maxCount = 20_000

		go func(s *ShardReader) {
			defer wg.Done()
			start := time.Now()
			if err := s.Run(ctx); err != nil {
				t.Errorf("run: %s", err)
			}
			s.nc.Flush()
			bm.AddPubSample(bench.NewSample(s.maxCount, msgSize, start, time.Now(), s.nc))
		}(s)
	}

	wg.Wait()
	bm.Close()

	fmt.Println(bm.Report())

	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	defer cleanupAssets(nc)

	js, _ := nc.JetStream()
	si, _ := js.StreamInfo("test-stream")
	fmt.Printf("Stream msgs: %d, bytes: %d\n", si.State.Msgs, si.State.Bytes)
}

func TestThroughput(t *testing.T) {
	ns := setupServer(t)
	defer ns.Shutdown()

	pubs := 120
	bm := bench.NewBenchmark("Kinesis", 0, pubs)

	// Calculated with headers included...
	msgSize := 549

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Errorf("load config: %s", err)
	}

	kc := kinesis.NewFromConfig(cfg)
	shards, err := getShards(ctx, kc, "test-stream")
	if err != nil {
		t.Errorf("get shards: %s", err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < pubs; i++ {
		wg.Add(1)
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			t.Errorf("nats connect: %s", err)
		}
		defer nc.Close()

		s := setupShardReader(t, nc, kc, *shards[i].ShardId)
		s.maxCount = 30_000

		go func(s *ShardReader) {
			defer wg.Done()
			start := time.Now()
			if err := s.Run(ctx); err != nil {
				t.Errorf("run: %s", err)
			}
			s.nc.Flush()
			if s.count > 0 {
				bm.AddPubSample(bench.NewSample(s.count, msgSize, start, time.Now(), s.nc))
			}
		}(s)
	}

	wg.Wait()
	bm.Close()

	fmt.Println(bm.Report())

	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	defer cleanupAssets(nc)

	js, _ := nc.JetStream()
	si, _ := js.StreamInfo("test-stream")
	fmt.Printf("Stream msgs: %d, bytes: %d\n", si.State.Msgs, si.State.Bytes)
}
