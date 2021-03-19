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
    sourceAddr := flag.String("addr", "", "source addr")

    flag.Parse()
    if *sourceAddr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*sourceAddr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    log.Infof("flushdb of %s", *sourceAddr)

    var (
        srcClient = redis.NewClient(&redis.Options{
            Network:            "tcp",
            Addr:               *sourceAddr,
            DialTimeout:        120*time.Second,
            ReadTimeout:        120*time.Second,
            WriteTimeout:       120*time.Second,
            IdleCheckFrequency: time.Second*10,
        })

        scan = func(cid int, slot int) (rows cmd.Rows, newCid int, err error) {
            result, err := srcClient.Do([]interface{}{"SLOTSSCAN", slot, cid, "count", 100000000}...).Result()
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
            return rows, newCid, nil
        }
    )

    for slot := 0; slot < 16384; slot++ {
        var cursorID = 0
        for round := 1; ; round++ {
            var (
                rows cmd.Rows
                err  error
            )
            if rows, cursorID, err = scan(cursorID, slot); err != nil {
                log.Fatalf("scan slot %d failed: '%v'", slot, err)
            }

            if err = rows.Delete(srcClient); err != nil {
                log.Fatalf("Del slot %d failed: '%v'", slot, err)
            }

            if cursorID == 0 {
                if (slot+1) % 256 == 0 {
                    log.Infof("del all keys of slot %d", slot)
                }
                break
            }

            if round%100 == 0 {
                log.Infof("round %d finished for slot %d", round, slot)
            }
        }
    }
    log.Infof("del keys succeeded")
}