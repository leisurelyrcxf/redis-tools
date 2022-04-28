package main

import (
    "fmt"
    "io"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    log "github.com/sirupsen/logrus"

    "github.com/leisurelyrcxf/redis-tools/cmd"
    "github.com/leisurelyrcxf/redis-tools/cmd/common"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
)

func main()  {
    c := common.Flags("analyze key size", false)
    c.Parse()

    var (
        cli = c.SrcClient
        readerWg sync.WaitGroup
        maxBuffered = 10000
        rawRowsCh       = make(chan cmd.Rows, maxBuffered)

        successfulReadBatches,  failedReadBatches int64

        isRetryableErr = func(err error) bool {
            return strings.Contains(err.Error(), "broken pipe") || err == io.EOF
        }
        readerCount = 30
        stats = cmd.NewStats()
    )

    var scannedBatches int64
    c.ScanSlotsAsync(&scannedBatches, rawRowsCh)

    for i := 0; i < readerCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range rawRowsCh {
                for i := 0; ; i++ {
                    if err := rows.MCard(cli); err != nil {
                        if i >= common.DefaultMaxRetry - 1 || !isRetryableErr(err) {
                            atomic.AddInt64(&failedReadBatches, 1)
                            log.Errorf("[Manual] Read failed: %v @round %d, keys: %v", err, i, rows.Keys())
                            break
                        }
                        log.Warnf("Card failed: '%v' @round %d, retrying in %s...", err, i, common.DefaultRetryInterval)
                        time.Sleep(common.DefaultRetryInterval)
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

    readerWg.Wait()
    utils.Assert(scannedBatches == failedReadBatches + successfulReadBatches)
    log.Warningf("----------------------------------------------------------------------------------")
    log.Warningf("all readers finished")
    fmt.Print(stats.String())
    log.Warningf("----------------------------------------------------------------------------------")
}
