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
    addr := flag.String("addr", "", "addr")
    maxSlotNum := flag.Int("max-slot-num", 0, "max slot number")
    slot := flag.Int("slot", -1, "slot")

    flag.Parse()
    if *addr == "" {
        log.Fatalf("source addr not provided")
    }
    if *maxSlotNum == 0 && *slot == 0 {
        log.Fatalf("max slot number is 0 && slot == 0")
    }
    if _, _, err := net.SplitHostPort(*addr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    log.Infof("cleanup %s", *addr)

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

        scan = func(cid int, slot int) (rows cmd.Rows, newCid int, err error) {
            result, err := cli.Do([]interface{}{"SLOTSSCAN", slot, cid, "count", 100000000}...).Result()
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

    var slots []int
    if *maxSlotNum != 0 {
        for slot := 0; slot < *maxSlotNum; slot++ {
            slots = append(slots, slot)
        }
    } else {
        slots = append(slots, *slot)
    }
    for _, slot := range slots {
        var cursorID = 0
        for round := 1; ; round++ {
            var (
                rows cmd.Rows
                err  error
            )
            if rows, cursorID, err = scan(cursorID, slot); err != nil {
                log.Fatalf("scan slot %d failed: '%v'", slot, err)
            }

            if err = rows.MGet(cli); err != nil {
                log.Fatalf("MGet rows of slot %d failed: '%v'", slot, err)
            }

            for _, row := range rows {
                if row.IsValueEmpty() {
                    log.Infof("key %s is empty", row.K)
                }
            }

            if cursorID == 0 {
                log.Infof("cleanup all keys of slot %d", slot)
                break
            }

            if round%100 == 0 {
                log.Infof("round %d finished for slot %d", round, slot)
            }
        }
    }
    log.Infof("del keys succeeded")
}
