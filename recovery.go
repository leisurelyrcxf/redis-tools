package main

import (
    "flag"
    "fmt"
    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"
    "net"
    "strconv"
    "time"
)

var defaultExpire time.Duration = 0

type RedisType string

const (
    RedisTypeUnknown RedisType = ""
    RedisTypeString  RedisType = "string"
    RedisTypeHash    RedisType = "hash"
    RedisTypeZset    RedisType= "zset"
)

var RedisTypes = []RedisType{
    RedisTypeString,
    RedisTypeHash,
    RedisTypeZset,
}

func parseRedisType(typeStr string) (RedisType, error) {
    for _, redisType := range RedisTypes {
        if string(redisType) == typeStr {
            return redisType, nil
        }
    }
    return RedisTypeUnknown, fmt.Errorf("unknown redis type '%s'", typeStr)
}

type Row struct {
    K string
    T RedisType
    V interface{}  // string, map[string]string, []redis.Z
    TargetNotExists bool
}

func (r *Row) Get(p redis.Pipeliner) {
    switch r.T {
    case RedisTypeString:
        p.Get(r.K)
    case RedisTypeHash:
        p.HGetAll(r.K)
    case RedisTypeZset:
        p.ZRangeWithScores(r.K, 0, -1)
    default:
        panic(fmt.Sprintf("unknown redis type %s", r.T))
    }
}

func (r *Row) ParseValue(cmder redis.Cmder) (interface{}, error) {
    switch cmd := cmder.(type) {
    case *redis.StringCmd:
        ret, err := cmd.Result()
        if err != nil {
            log.Errorf("cmd '%s' failed: %v", cmd.String(), err)
        }
        return ret, err
    case *redis.StringStringMapCmd:
        ret, err := cmd.Result()
        if err != nil {
            log.Errorf("cmd '%s' failed: %v", cmd.String(), err)
        }
        return ret, err
    case *redis.ZSliceCmd:
        ret, err := cmd.Result()
        if err != nil {
            log.Errorf("cmd '%s' failed: %v", cmd.String(), err)
        }
        return ret, err
    default:
        panic(fmt.Sprintf("unknown cmder type %T", cmder))
    }
}

func (r *Row) Set(p redis.Pipeliner) {
    switch r.T {
    case RedisTypeString:
        p.SetNX(r.K, r.V, defaultExpire)
    case RedisTypeHash:
        valueMap := make(map[string]interface{})
        for k ,v := range r.V.(map[string]string) {
            valueMap[k] = v
        }
        if len(valueMap) == 0 {
            log.Panicf("len(valueMap) == 0 for key %v(%v)", r.K, r.T)
        }
        p.HMSet(r.K, valueMap)
        if defaultExpire > 0 {
            p.Expire(r.K, defaultExpire)
        }
    case RedisTypeZset:
        if len(r.V.([]redis.Z)) == 0 {
            log.Panicf("len(r.V.([]redis.Z)) == 0 for key %v(%v)", r.K, r.T)
        }
        p.ZAddNX(r.K, r.V.([]redis.Z)...)
        if defaultExpire > 0 {
            p.Expire(r.K, defaultExpire)
        }
    default:
        panic(fmt.Sprintf("unknown redis type %s", r.T))
    }
}

func (r *Row) IsValueEmpty() bool {
    switch r.T {
    case RedisTypeString:
        return false
    case RedisTypeHash:
        return len(r.V.(map[string]string)) == 0
    case RedisTypeZset:
        return len(r.V.([]redis.Z)) == 0
    default:
        panic(fmt.Sprintf("unknown redis type %s", r.T))
    }
}

type Rows []*Row

func (rs Rows) types(client *redis.Client) error {
    p := client.Pipeline()
    for _, row := range rs {
        p.Type(row.K)
    }
    cmders, cmdErr := p.Exec()
    if cmdErr != nil {
        return cmdErr
    }
    for idx, cmder := range cmders {
        typeString, err := cmder.(*redis.StatusCmd).Result()
        if err != nil {
            log.Errorf("cmd '%s' failed: %v", cmder.(*redis.StatusCmd).String(), err)
            return err
        }
        if rs[idx].T, err = parseRedisType(typeString); err != nil {
            return err
        }
    }
    return nil
}

func (rs Rows) MGet(client *redis.Client) error {
    if err := rs.types(client); err != nil {
        log.Errorf("can't get types: '%v'", err)
        return err
    }
    p := client.Pipeline()
    for _, row := range rs {
        row.Get(p)
    }
    cmders, cmdErr := p.Exec()
    if cmdErr != nil {
        return cmdErr
    }
    for idx, cmder := range cmders {
        val, err := rs[idx].ParseValue(cmder)
        if err != nil {
            return err
        }
        rs[idx].V = val
    }
    return nil
}

func (rs Rows) MSet(target *redis.Client, notLogExistedKeys bool) error {
    if err := rs.exists(target); err != nil {
        log.Errorf("rs.exists failed: '%v'", err)
        return err
    }

    p := target.Pipeline()
    for _, row := range rs {
        if row.TargetNotExists {
            if row.IsValueEmpty() {
                log.Warnf("skip empty value of key %s, type: %v", row.K, row.T)
            } else {
                row.Set(p)
            }
        } else if !notLogExistedKeys {
            log.Warnf("skip existed key %s", row.K)
        }
    }
    cmders, cmdErr := p.Exec()
    return parseErr(cmders, cmdErr)
}

func (rs Rows) exists(target *redis.Client) error {
    p := target.Pipeline()
    for _, row := range rs {
        p.Exists(row.K)
    }
    cmders, cmdErr := p.Exec()
    if cmdErr != nil {
        return cmdErr
    }
    for idx, cmder := range cmders {
        existsVal, err := cmder.(*redis.IntCmd).Result()
        if err != nil {
            log.Errorf("cmd '%s' failed: %v", cmder.(*redis.IntCmd).String(), err)
            return err
        }
        rs[idx].TargetNotExists = existsVal == 0
    }
    return nil
}

func parseErr(cmders []redis.Cmder, err error) error {
    if err != nil {
        log.Errorf("pipeline failed: %v", err)
        return err
    }
    for _, cmder := range cmders {
        switch cmd := cmder.(type) {
        case *redis.StatusCmd:
            if _, err := cmd.Result(); err != nil {
                log.Errorf("cmd '%s' failed: %v", cmd.String(), err)
                return err
            }
        case *redis.BoolCmd:
            if _, err := cmd.Result(); err != nil {
                log.Errorf("cmd '%s' failed: %v", cmd.String(), err)
                return err
            }
        case *redis.IntCmd:
            if _, err := cmd.Result(); err != nil {
                log.Errorf("cmd '%s' failed: %v", cmd.String(), err)
                return err
            }
        default:
            panic(fmt.Sprintf("unknown cmder type %T", cmder))
        }
    }
    return nil
}

func main()  {
    pSlot := flag.Int("slot", -1, "slot, may be 3, 19, 31, 35, 38, 44, 51, 54, 57, 63")
    sourceAddr := flag.String("source-addr", "", "source addr")
    targetAddr := flag.String("target-addr", "", "target addr")
    notLogExistedKeys := flag.Bool("not-log-existed-keys", false, "not log existed keys")
    expire := flag.Duration("expire", 0, "expire time")

    flag.Parse()
    if *expire > 0 {
        defaultExpire = *expire
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
    log.Infof("migrating slot %d", *pSlot)

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

        scan = func(cid int) (rows Rows, newCid int, err error) {
            result, err := srcClient.Do([]interface{}{"SLOTSSCAN", *pSlot, cid}...).Result()
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
            rows = make([]*Row, 0, len(keysResult))
            for _, keyResult := range keysResult {
                key, ok := keyResult.(string)
                if !ok {
                    return nil, -1, fmt.Errorf("key type not sring")
                }
                rows = append(rows, &Row{K: key})
            }

            if err := rows.MGet(srcClient); err != nil {
                return nil, -1, err
            }
            return
        }

        cursorID = 0
    )

    for round := 0;;round++{
        var (
        	rows Rows
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
            log.Infof("round %d finished", round)
        }
    }
    log.Infof("migration succeeded")
}
