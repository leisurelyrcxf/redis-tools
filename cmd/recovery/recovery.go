package main

import (
    "flag"
    "github.com/leisurelyrcxf/redis-tools/cmd/common"
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
    c := common.Flags()
    notLogExistedKeys := flag.Bool("not-log-existed-keys", false, "not log existed keys")
    largeObjCard := flag.Int64("large-obj-card", 6000000, "large obj card")
    overwriteExistedKeys := flag.Bool("overwrite", false, "overwrite existed keys")
    delTargetKeyBeforeOverwrite := flag.Bool("del-target-key-before-overwrite", false, "delete target key first when overwrite")

    c.Parse()
    var (
        input  = make(chan cmd.Rows, c.MaxBuffered)
    )
    var scannedBatches, diffOKReadBatches, diffFailedReadBatches, diffOKWriteBatches, diffFailedWriteBatches, diffBatches int64
    cmd.ScanSlotsRawAsync(c.SrcClient, c.Slots, c.RedisType,
        *overwriteExistedKeys, *delTargetKeyBeforeOverwrite,
        c.BatchSize, maxRetry, retryInterval, &scannedBatches, input)
    diffCh := cmd.DiffAsync(input, c.ReaderCount, c.WriterCount, c.SrcClient, c.TargetClient, c.MaxBuffered, maxRetry, retryInterval,
        &diffOKReadBatches, &diffFailedReadBatches, &diffOKWriteBatches, &diffFailedWriteBatches, &diffBatches)

    var migrateOKReadBatches, migrateFailedReadBatches, migrateOKWriteBatches, migrateFailedWriteBatches int64
    migrateDone := migrateAsync(diffCh, c.ReaderCount, c.WriterCount, c.WriterCount,
        c.SrcClient, c.TargetClient, c.MaxBuffered, c.BatchSize, !*notLogExistedKeys, *largeObjCard,
        &migrateOKReadBatches, &migrateFailedReadBatches, &migrateOKWriteBatches, &migrateFailedWriteBatches)
    <- migrateDone

    utils.Assert(scannedBatches == diffOKReadBatches+diffFailedReadBatches)
    utils.Assert(diffOKReadBatches == diffOKWriteBatches+diffFailedWriteBatches)
    utils.Assert(diffBatches == migrateOKReadBatches + migrateFailedReadBatches)
    utils.Assert(migrateOKReadBatches == migrateOKWriteBatches + migrateFailedWriteBatches)
    if diffFailedReadBatches == 0 && diffFailedWriteBatches == 0 && migrateFailedReadBatches == 0 && migrateFailedWriteBatches == 0 {
        if diffBatches == 0 {
            log.Warningf("source and dest are the same")
        } else {
            log.Warningf("migration succeeded, migrated %d batches", diffBatches)
        }
    } else {
        if diffFailedReadBatches > 0 {
            log.Errorf("%d diff read batches failed", diffFailedReadBatches)
        }
        if diffFailedWriteBatches > 0 {
            log.Errorf("%d diff write batches failed", diffFailedWriteBatches)
        }
        if migrateFailedReadBatches > 0 {
            log.Errorf("%d migrate read batches failed", diffFailedReadBatches)
        }
        if migrateFailedWriteBatches > 0 {
            log.Errorf("%d migrate write batches failed", diffFailedWriteBatches)
        }
    }
}

func migrateAsync(input <-chan cmd.Rows, readerCount, smallKeyWriterCount, largeKeyWriterCount int,
    srcClient *redis.Client, targetClient *redis.Client,
    maxBuffered int, batchSize int, logExistedKeys bool, largeObjCard int64,
    successfulReadBatches, failedReadBatches, successfulWriteBatches, failedWriteBatches *int64) (done chan struct{}) {
    done = make(chan struct{})
    var (
        readerWg     sync.WaitGroup
        smallKeyRows = make(chan cmd.Rows, maxBuffered)
        largeKeyRows = make(chan cmd.Rows, 1024*1024) // Since large key rows don't hold any data, thus the channel buffer can be much bigger.
    )

    for i := 0; i < readerCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range input {
                large, small := rows.Filter(func(row *cmd.Row) bool {
                    return row.Cardinality >= largeObjCard
                })

                largeKeyRows <- large

                if err := utils.ExecWithRetryRedis(func() error {
                    return small.MGet(srcClient, true, largeObjCard)
                }, maxRetry, retryInterval); err != nil {
                    atomic.AddInt64(failedReadBatches, 1)
                    log.Errorf("[migrateAsync][Manual] Read failed: %v, keys: %v", err, small.Keys())
                } else {
                    smallKeyRows <- small
                    tmp := atomic.AddInt64(successfulReadBatches, 1)
                    log.Infof("[migrateAsync] Read %d batches successfully", tmp)
                }
            }
        }()
    }

    go func() {
        readerWg.Wait()
        log.Infof("all readers finished, close smallKeyRows")
        close(smallKeyRows)
        close(largeKeyRows)
    }()

    var (
        smallKeyWriterWg sync.WaitGroup
        largeKeyWriterWg sync.WaitGroup
    )
    for i := 0; i < smallKeyWriterCount; i++ {
        smallKeyWriterWg.Add(1)

        go func() {
            defer smallKeyWriterWg.Done()

            for rows := range smallKeyRows {
                if err := utils.ExecWithRetryRedis(func() error {
                    return rows.MSet(targetClient, batchSize, logExistedKeys)
                }, maxRetry, retryInterval); err != nil {
                    atomic.AddInt64(failedWriteBatches, 1)
                    log.Errorf("[migrateAsync][Manual] Write failed: '%v', keys: %v", err, rows.Keys())
                } else {
                    tmp := atomic.AddInt64(successfulWriteBatches, 1)
                    log.Infof("[migrateAsync] Written %d batches successfully", tmp)
                }
            }
        }()
    }

    for i := 0; i < largeKeyWriterCount; i++ {
        largeKeyWriterWg.Add(1)

        go func() {
            defer largeKeyWriterWg.Done()

            for rows := range largeKeyRows {
                for _, row := range rows {
                    if err := row.MigrateLargeKey(srcClient, targetClient, batchSize, logExistedKeys, maxRetry, retryInterval); err != nil {
                        log.Errorf("migrate large key '%s' failed: '%v'", row.K, err)
                    }
                }
            }
        }()
    }


    go func() {
        smallKeyWriterWg.Wait()
        largeKeyWriterWg.Wait()
        close(done)
    }()

    return done
}