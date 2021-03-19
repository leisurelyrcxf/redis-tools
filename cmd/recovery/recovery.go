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

const (
    maxRetry      = 10
    retryInterval = time.Second*5
)

func main()  {
    pSlot := flag.Int("slot", -1, "slot, may be 3, 19, 31, 35, 38, 44, 51, 54, 57, 63")
    sourceAddr := flag.String("source-addr", "", "source addr")
    targetAddr := flag.String("target-addr", "", "target addr")
    notLogExistedKeys := flag.Bool("not-log-existed-keys", false, "not log existed keys")
    dataType := flag.String("data-type", "", "data types could be 'string', 'hash', 'zset'")
    overwriteExistedKeys := flag.Bool("overwrite", false, "overwrite existed keys")
    expire := flag.Duration("expire", 0, "expire time")
    batchSize := flag.Int("batch-size", 100, "batch size")
    readerCount := flag.Int("reader", 15, "reader count")
    writerCount := flag.Int("writer", 15, "writer count")
    maxBuffered := flag.Int("max-buffered", 1024, "max buffered batch size")

    flag.Parse()
    if *expire > 0 {
        cmd.DefaultExpire = *expire
    }
    if *pSlot == -1 {
        log.Fatalf("slot not provided")
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
    log.Infof("migrating slot %d, data type: '%s', overwrite existed keys: %v", *pSlot, redisType, *overwriteExistedKeys)

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

        scan = func(cid int) (rows cmd.Rows, newCid int, err error) {
            result, err := srcClient.Do([]interface{}{"SLOTSSCAN", *pSlot, cid, "count", *batchSize}...).Result()
            if err != nil {
                log.Errorf("slotsscan failed: '%v'", err)
                return nil, 0, err
            }
            resultArray, ok := result.([]interface{})
            if !ok {
                return nil, -1, fmt.Errorf("result type not []interface{}")
            }
            if len(resultArray) != 2 {
                return nil, -1, fmt.Errorf("result not 2 rows")
            }
            newCursorString := fmt.Sprintf("%v", resultArray[0])
            if newCid, err = strconv.Atoi(newCursorString); err != nil {
                return nil, -1, fmt.Errorf("result '%s' not int", newCursorString)
            }
            keysResult, ok := resultArray[1].([]interface{})
            if !ok {
                return nil, -1, fmt.Errorf("rows result type not []interface{}")
            }
            rows = make([]*cmd.Row, 0, len(keysResult))
            for _, keyResult := range keysResult {
                key, ok := keyResult.(string)
                if !ok {
                    return nil, -1, fmt.Errorf("key type not sring")
                }
                rows = append(rows, &cmd.Row{K: key})
            }

            if redisType != cmd.RedisTypeUnknown {
                for _, row := range rows {
                    row.T = redisType
                }
            }
            if *overwriteExistedKeys {
                for _, row := range rows {
                    row.OverwriteExistedKeys = true
                }
            }
            return rows, newCid, nil
        }

        isRetryableErr = func(err error) bool {
            return strings.Contains(err.Error(), "broken pipe") || err == io.EOF
        }
    )


    var (
        readerWg sync.WaitGroup

        rawRowsCh       = make(chan cmd.Rows, *maxBuffered)
        rowsWithValueCh = make(chan cmd.Rows, *maxBuffered)

        successfulReadBatches int64
    )

    for i := 0; i < *readerCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range rawRowsCh {
                for i := 0; ; i++ {
                    if err := rows.MGet(srcClient); err != nil {
                        if i >= maxRetry- 1 || !isRetryableErr(err) {
                            log.Errorf("[Manual] Read failed: %v @round %d, keys: %v", err, i, rows.Keys())
                            break
                        }
                        log.Errorf("Read failed: '%v' @round %d, retrying in %s...", err, i, retryInterval)
                        time.Sleep(retryInterval)
                        continue
                    }
                    rowsWithValueCh <- rows
                    tmp := atomic.AddInt64(&successfulReadBatches, 1)
                    log.Infof("Read %d batches successfully @round %d", tmp, i)
                    break
                }
            }
        }()
    }

    go func() {
        readerWg.Wait()
        log.Infof("all readers finished, close rowsWithValueCh")
        close(rowsWithValueCh)
    }()

    var (
        writerWg               sync.WaitGroup
        successfulWriteBatches int64
    )
    for i := 0; i < *writerCount; i++ {
        writerWg.Add(1)

        go func() {
            defer writerWg.Done()

            for rows := range rowsWithValueCh {
                for i := 0; ; i++ {
                    if err := rows.MSet(targetClient, *notLogExistedKeys); err != nil {
                        if i >= maxRetry- 1 || !isRetryableErr(err) {
                            log.Errorf("[Manual] Write failed: '%v' @round %d, keys: %v", err, i, rows.Keys())
                            break
                        }
                        log.Errorf("Write failed: '%v' @round %d, retrying in %s...", err, i, retryInterval)
                        time.Sleep(retryInterval)
                        continue
                    }
                    tmp := atomic.AddInt64(&successfulWriteBatches, 1)
                    log.Infof("Written %d batches successfully @round %d ", tmp, i)
                    break
                }
            }
        }()
    }


    var (
        cursorID = 0
        scannedBatches int64
        start = time.Now()
    )
    for rounds := 0; ; rounds++ {
        var (
            rawRows cmd.Rows
            err     error
        )

        for i :=0; ; i++ {
            var newCursorID int
            if rawRows, newCursorID, err = scan(cursorID); err != nil {
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
            close(rawRowsCh)
            break
        }

        if rounds%100 == 0 {
            log.Infof("scanned %d batches in %v successfully", scannedBatches, time.Since(start))
            start = time.Now()
        }
    }

    readerWg.Wait()
    writerWg.Wait()
    failedReadBatches, failedWriteBatches := scannedBatches-successfulReadBatches, successfulReadBatches-successfulWriteBatches
    if failedReadBatches == 0 && failedWriteBatches == 0 {
        log.Infof("migration succeeded")
    } else {
        if failedReadBatches > 0 {
            log.Errorf("%d read batches failed", failedReadBatches)
        }
        if failedWriteBatches > 0 {
            log.Errorf("%d write batches failed", failedWriteBatches)
        }
        if failedReadBatches < 0 && failedWriteBatches < 0 {
            log.Fatalf("successfulReadBatches > scannedBatches && successfulWriteBatches > successfulReadBatches")
        }
        if failedReadBatches < 0 {
            log.Fatalf("successfulReadBatches > scannedBatches")
        }
        if failedWriteBatches < 0 {
            log.Fatalf("successfulWriteBatches > successfulReadBatches")
        }
    }
}