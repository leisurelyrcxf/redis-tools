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
    notLogExistedKeys := flag.Bool("not-log-existed-keys", false, "not log existed keys")
    dataType := flag.String("data-type", "", "data types could be 'string', 'hash', 'zset'")
    overwriteExistedKeys := flag.Bool("overwrite", false, "overwrite existed keys")
    expire := flag.Duration("expire", 0, "expire time")
    batchSize := flag.Int("batch-size", 100, "batch size")
    readerCount := flag.Int("reader", 4, "reader count")
    writerCount := flag.Int("writer", 4, "writer count")
    maxBuffered := flag.Int("max-buffered", 1024, "max buffered batch size")

    flag.Parse()
    var slots = utils.ParseSlots(*maxSlotNum, *slotsDesc)
    if *expire > 0 {
        cmd.DefaultExpire = *expire
    }
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
    var redisType = cmd.RedisTypeUnknown
    if *dataType != "" {
        var err error
        if redisType, err = cmd.ParseRedisType(*dataType); err != nil {
            log.Fatalf("unknown data type: %v", *dataType)
        }
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
    )


    var (
        input  = make(chan cmd.Rows, *maxBuffered)
    )
    cmd.ScanSlotsRaw(srcClient, slots, redisType, *overwriteExistedKeys, *batchSize, maxRetry, retryInterval, input)
    var successfulReadBatches, failedReadBatches, successfulWriteBatches, failedWriteBatches int64
    diffCh := cmd.RedisDiff(input, *readerCount, *writerCount, srcClient, targetClient, *maxBuffered, maxRetry, retryInterval,
        &successfulReadBatches, &failedReadBatches, &successfulWriteBatches, &failedWriteBatches)
    _, failedReadBatches, _, failedWriteBatches =
         migrate(diffCh, *readerCount, *writerCount, srcClient, targetClient, *maxBuffered, *batchSize, !*notLogExistedKeys)
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
}

func migrate(input <-chan cmd.Rows, readerCount, writerCount int,
    srcClient *redis.Client, targetClient *redis.Client,
    maxBuffered int, pipeCap int, logExistedKeys bool) (successfulReadBatches, failedReadBatches, successfulWriteBatches, failedWriteBatches int64) {
    var (
        readerWg sync.WaitGroup
        rowsRead = make(chan cmd.Rows, maxBuffered)
        isRetryableErr = func(err error) bool {
            return strings.Contains(err.Error(), "broken pipe") || err == io.EOF
        }
    )

    for i := 0; i < readerCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range input {
                if err := utils.ExecWithRetry(func() error {
                    return rows.MGet(srcClient)
                }, maxRetry, retryInterval, isRetryableErr); err != nil {
                    atomic.AddInt64(&failedReadBatches, 1)
                    log.Errorf("[Manual] Read failed: %v, keys: %v", err, rows.Keys())
                } else {
                    rowsRead <- rows
                    tmp := atomic.AddInt64(&successfulReadBatches, 1)
                    log.Infof("Read %d batches successfully", tmp)
                }
            }
        }()
    }

    go func() {
        readerWg.Wait()
        log.Infof("all readers finished, close rowsRead")
        close(rowsRead)
    }()

    var (
        writerWg               sync.WaitGroup
    )
    for i := 0; i < writerCount; i++ {
        writerWg.Add(1)

        go func() {
            defer writerWg.Done()

            for rows := range rowsRead {
                if err := utils.ExecWithRetry(func() error {
                    return rows.MSet(targetClient, pipeCap, logExistedKeys)
                }, maxRetry, retryInterval, isRetryableErr); err != nil {
                    atomic.AddInt64(&failedWriteBatches, 1)
                    log.Errorf("[Manual] Write failed: '%v', keys: %v", err, rows.Keys())
                } else {
                    tmp := atomic.AddInt64(&successfulWriteBatches, 1)
                    log.Infof("Written %d batches successfully", tmp)
                }
            }
        }()
    }
    writerWg.Wait()
    return successfulReadBatches, failedReadBatches, successfulWriteBatches, failedWriteBatches
}