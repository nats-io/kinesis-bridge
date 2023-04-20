package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/jedib0t/go-pretty/v6/progress"
)

const (
	payload = `{"facility_code": "AL207", "pointid": "1301115", "tagname": "NAH-ELEC-E300-72882_M_CONV_B_SCALP_DISC_SCRN_FEED.Test Trip", "col_timestamp": "02/21/2023 19:29:35.000", "numericvalue": "0", "stringvalue": "", "batch_id": "NAH-03S-20230221132923"}`

	// Max batch size for PutRecords.
	defaultBatchSize = 500
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
		noProgress bool
		batchSize  int
		publishers int
	)

	flag.StringVar(&streamName, "stream.name", "test-stream", "Stream name")
	flag.DurationVar(&putSleep, "put.sleep", time.Second, "Sleep between puts")
	flag.BoolVar(&noProgress, "no-progress", false, "Disable progress bars")
	flag.IntVar(&batchSize, "batch-size", defaultBatchSize, "Batch size")
	flag.IntVar(&publishers, "publishers", 1, "Number of publishers")

	flag.Parse()

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	kc := kinesis.NewFromConfig(cfg)

	pw := progress.NewWriter()
	pw.SetNumTrackersExpected(publishers)
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.SetUpdateFrequency(250 * time.Millisecond)
	pw.Style().Visibility.Speed = true
	pw.Style().Visibility.SpeedOverall = true
	pw.Style().Visibility.SpeedOverall = true

	if !noProgress {
		go pw.Render()
		defer pw.Stop()
	}

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	for i := 0; i < publishers; i++ {
		wg.Add(1)

		t := progress.Tracker{
			Message: fmt.Sprintf("Publisher %d", i),
		}
		pw.AppendTracker(&t)

		go func(t *progress.Tracker) {
			defer wg.Done()
			if err := runPublisher(ctx, putSleep, kc, streamName, batchSize, t); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}(&t)
	}

	<-sigch

	return nil
}

func runPublisher(ctx context.Context, putSleep time.Duration, kc *kinesis.Client, streamName string, batchSize int, tracker *progress.Tracker) error {
	var count int
	ticker := time.NewTicker(putSleep)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			inrecs := make([]types.PutRecordsRequestEntry, 0, batchSize)

			for i := 0; i < batchSize; i++ {
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

			tracker.Increment(int64(len(outrecs.Records)))
		}
	}
}
