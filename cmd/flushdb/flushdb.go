package main

import (
    "flag"
    "net"
    "sync"
    "sync/atomic"
    "time"

    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"

    "github.com/leisurelyrcxf/redis-tools/cmd"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
)

func main()  {
    maxSlotNum := flag.Int("max-slot-num", 0, "max slot number")
    slotsDesc := flag.String("slots",  "-1,-2", "slots")
    sourceAddr := flag.String("addr", "", "source addr")
    batchSize := flag.Int("batch-size", 100, "batch size")
    writerCount := flag.Int("writer", 4, "reader count")
    maxBuffered := flag.Int("max-buffered", 1024, "max buffered batch size")

    flag.Parse()
    var slots = utils.ParseSlots(*maxSlotNum, *slotsDesc)
    if *sourceAddr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*sourceAddr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    log.Infof("flushdb of %s", *sourceAddr)

    var (
        srcClient = redis.NewClient(&redis.Options{
            Network:            "tcp",
            Addr:               *sourceAddr,
            DialTimeout:        120*time.Second,
            ReadTimeout:        120*time.Second,
            WriteTimeout:       120*time.Second,
            IdleCheckFrequency: time.Second*10,
        })

        writerWg sync.WaitGroup

        rawRowsCh                                  = make(chan cmd.Rows, *maxBuffered)
        successfulWriteBatches, failedWriteBatches int64
    )

    for i := 0; i < *writerCount; i++ {
        writerWg.Add(1)

        go func() {
            defer writerWg.Done()

            for rows := range rawRowsCh {
                if err := utils.ExecWithRetryRedis(func() error {
                    return rows.MDel(srcClient)
                }, 10, time.Second); err != nil {
                    atomic.AddInt64(&failedWriteBatches, 1)
                    log.Fatalf("[Manual] Read failed: %v @round %d, keys: %v", err, i, rows.Keys())
                } else {
                    tmp := atomic.AddInt64(&successfulWriteBatches, 1)
                    log.Infof("Read %d batches successfully @round %d", tmp, i)
                }
            }
        }()
    }

    var scannedBatches int64
    cmd.ScanSlotsAsync(srcClient, slots, *batchSize, 10, time.Second, &scannedBatches, rawRowsCh)
    writerWg.Wait()

    if failedWriteBatches == 0 {
        panic("")
    }
    utils.Assert(failedWriteBatches == 0 && successfulWriteBatches == scannedBatches)
    log.Infof("del keys succeeded")
}