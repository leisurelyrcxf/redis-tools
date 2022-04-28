package main

import (
    "sync"
    "sync/atomic"

    log "github.com/sirupsen/logrus"

    "github.com/leisurelyrcxf/redis-tools/cmd"
    "github.com/leisurelyrcxf/redis-tools/cmd/common"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
)

func main()  {
    c := common.Flags("find empty keys (card == 0)", false)
    c.Parse()

    var (
        cli = c.SrcClient
        readerWg sync.WaitGroup
        maxBuffered = 10000
        rawRowsCh       = make(chan cmd.Rows, maxBuffered)

        successfulReadBatches,  failedReadBatches int64
        readerCount = 30
    )

    var scannedBatches int64
    c.ScanSlotsAsync(&scannedBatches, rawRowsCh)

    for i := 0; i < readerCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range rawRowsCh {
                if err := utils.ExecWithRetryRedis(func() error {
                    if err := rows.MCard(cli); err != nil {
                        return err
                    }

                    for _, row := range rows {
                        if row.Cardinality == 0 {
                            log.Infof("%s key '%s' is empty", row.T, row.K)
                        }
                    }
                    return nil
                }, common.DefaultMaxRetry, common.DefaultRetryInterval); err != nil {
                    atomic.AddInt64(&failedReadBatches, 1)
                    log.Errorf("[Manual] MCard failed: %v, keys: %v", err, rows.Keys())
                } else {
                    tmp := atomic.AddInt64(&successfulReadBatches, 1)
                    log.Infof("Read %d batches successfully @round %d", tmp, i)
                }
            }
        }()
    }

    readerWg.Wait()
    utils.Assert(scannedBatches == failedReadBatches + successfulReadBatches)
    log.Infof("all readers finished")
}
