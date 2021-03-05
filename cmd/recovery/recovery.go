package main

import (
    "ads-recovery/cmd"
    "flag"
    "fmt"
    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"
    "net"
    "strconv"
    "time"
)

func main()  {
    pSlot := flag.Int("slot", -1, "slot, may be 3, 19, 31, 35, 38, 44, 51, 54, 57, 63")
    sourceAddr := flag.String("source-addr", "", "source addr")
    targetAddr := flag.String("target-addr", "", "target addr")
    notLogExistedKeys := flag.Bool("not-log-existed-keys", false, "not log existed keys")
    dataType := flag.String("data-type", "", "data types could be 'string', 'hash', 'zset'")
    overwriteExistedKeys := flag.Bool("overwrite", false, "overwrite existed keys")
    expire := flag.Duration("expire", 0, "expire time")
    batchSize := flag.Int("batch-size", 10000, "batch size")

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
            DialTimeout:        120*time.Second,
            ReadTimeout:        120*time.Second,
            WriteTimeout:       120*time.Second,
        })

        targetClient = redis.NewClient(&redis.Options{
            Network:            "tcp",
            Addr:               *targetAddr,
            DialTimeout:        120*time.Second,
            ReadTimeout:        120*time.Second,
            WriteTimeout:       120*time.Second,
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
            if err := rows.MGet(srcClient); err != nil {
                return nil, -1, err
            }
            return rows, newCid, nil
        }

        cursorID = 0
    )

    start := time.Now()
    for round := 0;;round++{
        var (
            rows cmd.Rows
            err error
        )
        if rows, cursorID, err = scan(cursorID); err != nil {
            log.Fatalf("scan failed: '%v'", err)
        }

        if err = rows.MSet(targetClient, *notLogExistedKeys); err != nil {
            log.Fatalf("MSet failed: '%v'", err)
        }

        if cursorID == 0 {
            break
        }

        if round % 100 == 0 {
            log.Infof("round %d finished in %v", round, time.Since(start))
            start = time.Now()
        }
    }
    log.Infof("migration succeeded")
}