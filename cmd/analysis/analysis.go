package main

import (
    "ads-recovery/cmd"
    "flag"
    "fmt"
    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"
    "io"
    "net"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"
)

func main()  {
    addr := flag.String("addr", "", "addr")
    maxSlotNum := flag.Int("max-slot-num", 0, "max slot number")
    slotsDesc := flag.String("slots",  "-1,-2", "slots")
    logLevel := flag.String("log-level", "error", "log level, can be 'panic', 'error', 'fatal', 'warn', 'info'")
    batchSize := flag.Int("batch-size", 256, "batch size")

    flag.Parse()
    var slots []int
    if *addr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*addr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    if *maxSlotNum == 0 && *slotsDesc == "" {
        log.Fatalf("max slot number is 0 && slot == 0")
    }
    if *maxSlotNum != 0 {
        for slot := 0; slot < *maxSlotNum; slot++ {
            slots = append(slots, slot)
        }
    } else {
        if strings.Contains(*slotsDesc, "-") {
            parts := strings.Split(*slotsDesc, "-")
            if len(parts) != 2 {
                log.Fatalf("len(parts) != 2")
            }
            first, err := strconv.Atoi(parts[0])
            if err != nil {
                log.Fatalf("first invalid: %v", err)
            }
            second, err := strconv.Atoi(parts[1])
            if err != nil {
                log.Fatalf("second invalid: %v", err)
            }
            for i := first; i <= second; i++ {
                slots = append(slots, i)
            }
            if len(slots) == 0 {
                log.Fatalf("len(slots) == 0")
            }
        } else {
            parts := strings.Split(*slotsDesc, ",")
            slots = make([]int, len(parts))
            for idx, part := range parts {
                slot, err := strconv.Atoi(part)
                if err != nil {
                    log.Fatalf("invalid slots: %v", err)
                }
                slots[idx] = slot
            }
        }
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
                    if err := rows.Card(cli); err != nil {
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

    var (
        scannedBatches int
    )
    log.Infof("Analyze key of slots %v of %s", slots, *addr)
    for _, slot := range slots {
        var cursorID = 0
        for round := 1; ; round++ {
            var (
                rawRows cmd.Rows
                err  error
            )

            for i :=0; ; i++ {
                var newCursorID int
                if rawRows, newCursorID, err = cmd.Scan(cli, slot, cursorID, *batchSize); err != nil {
                    if i >= maxRetry- 1 {
                        log.Fatalf("scan cursor %d failed: '%v' @round %d", cursorID, err, i)
                        return
                    }
                    log.Errorf("scan cursor %d failed: '%v' @round %d, retrying in %s...", cursorID, err, i, retryInterval)
                    time.Sleep(retryInterval)
                    continue
                }
                cursorID = newCursorID
                scannedBatches++
                break
            }

            rawRowsCh <- rawRows
            if cursorID == 0 {
                log.Warningf("scanned all keys of slot %d", slot)
                break
            }

            if round%1000 == 0 {
                log.Warningf("scanned %d keys for slot %d", *batchSize*round, slot)
            }
        }
    }
    close(rawRowsCh)


    readerWg.Wait()

    log.Warningf("----------------------------------------------------------------------------------")
    log.Warningf("all readers finished")
    fmt.Print(stats.String())
    log.Warningf("----------------------------------------------------------------------------------")
}
