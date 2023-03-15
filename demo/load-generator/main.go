package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/jedib0t/go-pretty/v6/progress"
)

const (
	payload = `{"facility_code": "AL207", "pointid": "1301115", "tagname": "NAH-ELEC-E300-72882_M_CONV_B_SCALP_DISC_SCRN_FEED.Test Trip", "col_timestamp": "02/21/2023 19:29:35.000", "numericvalue": "0", "stringvalue": "", "batch_id": "NAH-03S-20230221132923"}`
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		streamName string
		putSleep   time.Duration
	)

	flag.StringVar(&streamName, "stream.name", "test-stream", "stream name")
	flag.DurationVar(&putSleep, "put.sleep", time.Second, "sleep between puts")

	flag.Parse()

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	kc := kinesis.NewFromConfig(cfg)

	out, err := kc.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)

	pw := progress.NewWriter()
	pw.SetNumTrackersExpected(len(out.StreamDescription.Shards))
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.SetUpdateFrequency(250 * time.Millisecond)
	pw.Style().Visibility.Speed = true
	pw.Style().Visibility.SpeedOverall = true

	go pw.Render()

	shardTrackers := make(map[string]*progress.Tracker)

	for _, s := range out.StreamDescription.Shards {
		id := aws.ToString(s.ShardId)
		t := progress.Tracker{
			Message: id,
		}
		pw.AppendTracker(&t)
		shardTrackers[id] = &t
	}

	var count int
	ticker := time.NewTicker(putSleep)

done:
	for {
		select {
		case <-sigch:
			break done

		case <-ticker.C:
			inrecs := make([]types.PutRecordsRequestEntry, 0, 500)

			for i := 0; i < 500; i++ {
				inrecs = append(inrecs, types.PutRecordsRequestEntry{
					Data:         []byte(payload),
					PartitionKey: aws.String(fmt.Sprintf("%d", count)),
				})
				count++
			}

			outrecs, err := kc.PutRecords(ctx, &kinesis.PutRecordsInput{
				Records:    inrecs,
				StreamName: aws.String(streamName),
			})
			if err != nil {
				return fmt.Errorf("put records: %w", err)
			}

			for _, rec := range outrecs.Records {
				shardTrackers[aws.ToString(rec.ShardId)].Increment(1)
			}
		}
	}

	return nil
}
