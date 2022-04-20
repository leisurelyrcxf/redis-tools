package main

import (
    "flag"
    "fmt"
    "io"
    "net"
    "os"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"

    "github.com/leisurelyrcxf/redis-tools/cmd"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
)

const (
    maxRetry      = 10
    retryInterval = time.Second*5
)

func main()  {
    maxSlotNum := flag.Int("max-slot-num", 0, "max slot number")
    slotsDesc := flag.String("slots",  "-1,-2", "slots")
    sourceAddr := flag.String("source-addr", "", "source addr")
    targetAddr := flag.String("target-addr", "", "target addr")
    expire := flag.Duration("expire", 0, "expire time")
    batchSize := flag.Int("batch-size", 100, "batch size")
    readerCount := flag.Int("reader", 4, "reader count")
    writerCount := flag.Int("writer", 4, "writer count")
    maxBuffered := flag.Int("max-buffered", 1024, "max buffered batch size")

    flag.Parse()
    if *expire > 0 {
        cmd.DefaultExpire = *expire
    }
    flag.Parse()
    var slots = utils.ParseSlots(*maxSlotNum, *slotsDesc)
    if *sourceAddr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*sourceAddr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    if *targetAddr == "" {
        log.Fatalf("target addr not provided")
    }
    if _, _, err := net.SplitHostPort(*targetAddr); err != nil {
        log.Fatalf("target addr not valid: %v", err)
    }

    var (
        srcClient = redis.NewClient(&redis.Options{
            Network:            "tcp",
            Addr:               *sourceAddr,
            DialTimeout:        240*time.Second,
            ReadTimeout:        240*time.Second,
            WriteTimeout:       240*time.Second,
            PoolSize:50,
            IdleCheckFrequency: time.Second*10,
        })

        targetClient = redis.NewClient(&redis.Options{
            Network:            "tcp",
            Addr:               *targetAddr,
            DialTimeout:        240*time.Second,
            ReadTimeout:        240*time.Second,
            WriteTimeout:       240*time.Second,
            PoolSize: 50,
            IdleCheckFrequency: time.Second*10,
        })

        isRetryableErr = func(err error) bool {
            return strings.Contains(err.Error(), "broken pipe") || err == io.EOF
        }
    )


    var (
        readerWg sync.WaitGroup

        rawRowsCh       = make(chan cmd.Rows, *maxBuffered)
        rowsWithValueCh = make(chan cmd.Rows, *maxBuffered)

        successfulReadBatches,  failedReadBatches int64
    )

    for i := 0; i < *readerCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range rawRowsCh {
                if err := utils.ExecWithRetry(func() error {
                    return rows.MCard(srcClient)
                }, maxRetry, retryInterval, isRetryableErr); err != nil {
                    atomic.AddInt64(&failedReadBatches, 1)
                    log.Fatalf("[Manual] Read failed: %v, keys: %v", err, rows.Keys())
                } else {
                    rowsWithValueCh <- rows
                    log.Infof("Read %d batches successfully", atomic.AddInt64(&successfulReadBatches, 1))
                }
            }
        }()
    }

    go func() {
        readerWg.Wait()
        log.Infof("all readers finished, close rowsWithValueCh")
        close(rowsWithValueCh)
    }()

    var (
        writerWg               sync.WaitGroup
        successfulWriteBatches, failedWriteBatches int64
    )
    for i := 0; i < *writerCount; i++ {
        writerWg.Add(1)

        go func(i int) {
            defer writerWg.Done()

            w, err := os.Create(fmt.Sprintf("comparator-%d", i))
            if err != nil {
                panic(err)
            }
            for rows := range rowsWithValueCh {
                if err := utils.ExecWithRetry(func() error {
                    return rows.MDiff(targetClient)
                }, maxRetry, retryInterval, isRetryableErr); err != nil {
                    atomic.AddInt64(&failedWriteBatches, 1)
                    log.Fatalf("[Manual] Write failed: '%v' keys: %v", err, rows.Keys())
                } else {
                    log.Infof("Compare %d batches successfully %d ", atomic.AddInt64(&successfulWriteBatches, 1))
                    for _, row := range rows {
                        if row.D != nil {
                            if err := row.D.Output(w); err != nil {
                                log.Fatalf("Output error: %v", err)
                            }
                            if _, err = w.Write([]byte{'\n'}); err != nil {
                                log.Fatalf("Write error: %v", err)
                            }
                        }
                    }
                }
            }
            _ = w.Close()
        }(i)
    }

    scannedBatches := cmd.ScanSlots(srcClient, slots, *batchSize, maxRetry, retryInterval, rawRowsCh)

    readerWg.Wait()
    writerWg.Wait()
    if failedReadBatches == 0 && failedWriteBatches == 0 {
        log.Infof("migration succeeded")
    } else {
        if failedReadBatches > 0 {
            log.Errorf("%d read batches failed", failedReadBatches)
        }
        if failedWriteBatches > 0 {
            log.Errorf("%d write batches failed", failedWriteBatches)
        }
    }
    if failedReadBatches + successfulReadBatches != scannedBatches && failedWriteBatches + successfulWriteBatches != successfulReadBatches{
        log.Fatalf("failedReadBatches(%d) + successfulReadBatches(%d) != scannedBatches(%d) && failedWriteBatches(%d) + successfulWriteBatches(%d) != successfulReadBatches(%d)",
            failedReadBatches, successfulReadBatches, scannedBatches, failedWriteBatches, successfulWriteBatches, successfulReadBatches)
    }
    if failedReadBatches + successfulReadBatches != scannedBatches {
        log.Fatalf("failedReadBatches(%d) + successfulReadBatches(%d) != scannedBatches(%d)",
            failedReadBatches, successfulReadBatches, scannedBatches)
    }
    if failedWriteBatches + successfulWriteBatches != successfulReadBatches{
        log.Fatalf("failedWriteBatches(%d) + successfulWriteBatches(%d) != successfulReadBatches(%d)",
            failedWriteBatches, successfulWriteBatches, successfulReadBatches)
    }
}