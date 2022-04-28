package main

import (
    "github.com/leisurelyrcxf/redis-tools/cmd/common"
    "sync"
    "sync/atomic"
    "time"

    log "github.com/sirupsen/logrus"

    "github.com/leisurelyrcxf/redis-tools/cmd"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
)

func main()  {
    c := common.Flags("flushdb", false)
    c.Parse()

    var (
        writerWg sync.WaitGroup
        rawRowsCh                                  = make(chan cmd.Rows, c.MaxBuffered)
        successfulWriteBatches, failedWriteBatches int64
    )

    var scannedBatches int64
    c.ScanSlotsAsync(&scannedBatches, rawRowsCh)

    for i := 0; i < c.WriterCount; i++ {
        writerWg.Add(1)

        go func() {
            defer writerWg.Done()

            for rows := range rawRowsCh {
                if err := utils.ExecWithRetryRedis(func() error {
                    return rows.MDel(c.SrcClient)
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

    writerWg.Wait()

    if failedWriteBatches == 0 {
        panic("")
    }
    utils.Assert(failedWriteBatches == 0 && successfulWriteBatches == scannedBatches)
    log.Infof("del keys succeeded")
}