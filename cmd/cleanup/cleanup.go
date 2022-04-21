package main

import (
    "flag"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
    "io"
    "net"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/go-redis/redis"
    "github.com/leisurelyrcxf/redis-tools/cmd"
    log "github.com/sirupsen/logrus"
)

func main()  {
    maxSlotNum := flag.Int("max-slot-num", 0, "max slot number")
    slotsDesc := flag.String("slots",  "-1,-2", "slots")
    addr := flag.String("addr", "", "addr")

    flag.Parse()
    var slots = utils.ParseSlots(*maxSlotNum, *slotsDesc)
    if *addr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*addr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }

    var (
        cli = redis.NewClient(&redis.Options{
            Network:            "tcp",
            Addr:               *addr,
            DialTimeout:        240*time.Second,
            ReadTimeout:        240*time.Second,
            WriteTimeout:       240*time.Second,
            PoolSize:50,
            IdleCheckFrequency: time.Second*10,
        })
    )

    const batchSize = 50

    var (
        readerWg sync.WaitGroup
        maxBuffered = 10000
        rawRowsCh       = make(chan cmd.Rows, maxBuffered)

        successfulReadBatches,  failedReadBatches int64
        maxRetry = 10

        isRetryableErr = func(err error) bool {
            return strings.Contains(err.Error(), "broken pipe") || err == io.EOF
        }
        retryInterval = time.Second * 10
        readerCount = 30
    )

    for i := 0; i < readerCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range rawRowsCh {
                for i := 0; ; i++ {
                    if err := rows.MGet(cli); err != nil {
                        if i >= maxRetry- 1 || !isRetryableErr(err) {
                            atomic.AddInt64(&failedReadBatches, 1)
                            log.Errorf("[Manual] Read failed: %v @round %d, keys: %v", err, i, rows.Keys())
                            break
                        }
                        log.Warnf("Read failed: '%v' @round %d, retrying in %s...", err, i, retryInterval)
                        time.Sleep(retryInterval)
                        continue
                    }
                    tmp := atomic.AddInt64(&successfulReadBatches, 1)
                    log.Infof("Read %d batches successfully @round %d", tmp, i)

                    for _, row := range rows {
                        if row.IsValueEmpty() {
                            log.Infof("%s key '%s' is empty", row.T, row.K)
                        }
                    }
                    break
                }
            }
        }()
    }

    var scannedBatches int64
    cmd.ScanSlotsAsync(cli, slots, batchSize, maxRetry, retryInterval, &scannedBatches, rawRowsCh)
    readerWg.Wait()

    utils.Assert(scannedBatches == failedReadBatches + successfulReadBatches)
    log.Infof("all readers finished")
}
