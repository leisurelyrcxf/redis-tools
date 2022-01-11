package main

import (
    "flag"
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
    addr := flag.String("addr", "", "addr")
    maxSlotNum := flag.Int("max-slot-num", 0, "max slot number")
    slot := flag.Int("slot", -1, "slot")

    flag.Parse()
    if *addr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*addr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    if *maxSlotNum == 0 && *slot == -1 {
        log.Fatalf("max slot number is 0 && slot == 0")
    }
    if *maxSlotNum != 0 {
        log.Infof("cleanup all slots[0, %d) of %s", *maxSlotNum, *addr)
    } else {
        log.Infof("cleanup slot %d of %s", *slot, *addr)
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

    var slots []int
    if *maxSlotNum != 0 {
        for slot := 0; slot < *maxSlotNum; slot++ {
            slots = append(slots, slot)
        }
    } else {
        slots = append(slots, *slot)
    }

    var scannedBatches int
    for _, slot := range slots {
        var cursorID = 0
        for round := 1; ; round++ {
            var (
                rawRows cmd.Rows
                err  error
            )

            for i :=0; ; i++ {
                var newCursorID int
                if rawRows, newCursorID, err = cmd.Scan(cli, slot, cursorID, batchSize); err != nil {
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
                log.Infof("scanned all keys of slot %d", slot)
                break
            }

            if round%100 == 0 {
                log.Infof("scanned %d keys for slot %d", round*batchSize, slot)
            }
        }
    }
    close(rawRowsCh)


    readerWg.Wait()
    log.Infof("all readers finished")
}
