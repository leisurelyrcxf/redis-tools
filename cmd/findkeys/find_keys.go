package main

import (
    "github.com/leisurelyrcxf/redis-tools/cmd/common"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
    "os"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/leisurelyrcxf/redis-tools/cmd"
    log "github.com/sirupsen/logrus"
)

func main()  {
    c := common.Flags("find key which satisfies certain critera", false)
    c.Parse()

    var (
        readerWg sync.WaitGroup
        rawRowsCh       = make(chan cmd.Rows, c.MaxBuffered)

        successfulReadBatches,  failedReadBatches int64

        keys = make([]string, 0, 100000)
        keyMutex sync.Mutex
        addKeys = func(ks []string) {
            keyMutex.Lock()
            keys = append(keys, ks...)
            if len(keys) > 10000000 {
                for _, key := range keys {
                    println(key)
                }
                keyMutex.Unlock()
                os.Exit(0)
            }
            keyMutex.Unlock()
        }
    )

    var scannedBatches int64
    c.ScanSlotsAsync(&scannedBatches, rawRowsCh)

    for i := 0; i < c.ReaderCount; i++ {
        readerWg.Add(1)

        go func() {
            defer readerWg.Done()

            for rows := range rawRowsCh {
                for i := 0; ; i++ {
                    if err := rows.Types(c.SrcClient); err != nil {
                        if i >= common.DefaultMaxRetry - 1 || !utils.IsRetryableRedisErr(err) {
                            atomic.AddInt64(&failedReadBatches, 1)
                            log.Errorf("[Manual] Read failed: %v @round %d, keys: %v", err, i, rows.Keys())
                            break
                        }
                        log.Warnf("Read failed: '%v' @round %d, retrying in %s...", err, i, common.DefaultRetryInterval)
                        time.Sleep(common.DefaultRetryInterval)
                        continue
                    }
                    tmp := atomic.AddInt64(&successfulReadBatches, 1)
                    log.Infof("Read %d batches successfully @round %d", tmp, i)

                    hashKeys := make([]string, 0, 10)
                    for _, row := range rows {
                        if row.T == cmd.RedisTypeHash && strings.HasPrefix(row.K, "query"){
                            hashKeys = append(hashKeys, row.K)
                        }
                    }
                    addKeys(hashKeys)
                    break
                }
            }
        }()
    }

    readerWg.Wait()
    for _, key := range keys {
        println(key)
    }
    log.Infof("all readers finished")
}
