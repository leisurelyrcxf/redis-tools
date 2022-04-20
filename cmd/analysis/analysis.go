package main

import (
    "flag"
    "fmt"
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
    logLevel := flag.String("log-level", "error", "log level, can be 'panic', 'error', 'fatal', 'warn', 'info'")
    batchSize := flag.Int("batch-size", 256, "batch size")

    flag.Parse()
    var slots = utils.ParseSlots(*maxSlotNum, *slotsDesc)
    if *addr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*addr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    lvl, err := log.ParseLevel(*logLevel)
    if err != nil {
        log.Fatalf("invalid log level: '%v'", err)
    }
    log.SetLevel(lvl)

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
        stats = cmd.NewStats()
    )

    for i := 0; i < readerCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range rawRowsCh {
                for i := 0; ; i++ {
                    if err := rows.MCard(cli); err != nil {
                        if i >= maxRetry- 1 || !isRetryableErr(err) {
                            atomic.AddInt64(&failedReadBatches, 1)
                            log.Errorf("[Manual] Read failed: %v @round %d, keys: %v", err, i, rows.Keys())
                            break
                        }
                        log.Warnf("Card failed: '%v' @round %d, retrying in %s...", err, i, retryInterval)
                        time.Sleep(retryInterval)
                        continue
                    }
                    successfulReadBatches := atomic.AddInt64(&successfulReadBatches, 1)
                    if successfulReadBatches%1000 == 1 {
                        log.Warningf("Card %d batches successfully @round %d", successfulReadBatches, i)
                        fmt.Print(stats.String())
                    } else {
                        log.Infof("Card %d batches successfully @round %d", successfulReadBatches, i)
                    }
                    stats.Collect(rows)
                    break
                }
            }
        }()
    }


    cmd.ScanSlots(cli, slots, *batchSize, maxRetry, retryInterval, rawRowsCh)


    readerWg.Wait()

    log.Warningf("----------------------------------------------------------------------------------")
    log.Warningf("all readers finished")
    fmt.Print(stats.String())
    log.Warningf("----------------------------------------------------------------------------------")
}
