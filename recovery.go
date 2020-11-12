package main

import (
    "flag"
    "fmt"
    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"
    "strconv"
    "time"
)

const DefaultExpire = time.Hour * 24 * 30 * 7

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
}

func (r *Row) AccuGet(p redis.Pipeliner) {
    switch r.T {
    case RedisTypeString:
        p.Get(r.K)
    case RedisTypeHash:
        p.HGetAll(r.K)
    case RedisTypeZset:
        p.ZRangeWithScores(r.K, 0, -1)
    default:
        panic(fmt.Sprintf("unkown redis type %s", r.T))
    }
}

func (r *Row) ParseValue(cmder redis.Cmder) (interface{}, error) {
    switch cmd := cmder.(type) {
    case *redis.StringCmd:
        return cmd.Result()
    case *redis.StringStringMapCmd:
        return cmd.Result()
    case *redis.ZSliceCmd:
        return cmd.Result()
    default:
        panic(fmt.Sprintf("unknown cmder type %T", cmder))
    }
}

func (r *Row) AccuSet(p redis.Pipeliner) {
    switch r.T {
    case RedisTypeString:
        p.SetNX(r.K, r.V, DefaultExpire)
    case RedisTypeHash:
        // TODO May change to string, does type matter?
        for k, v := range r.V.(map[string]string) {
            p.HSetNX(r.K, k, v)
        }
        p.Expire(r.K, DefaultExpire)
    case RedisTypeZset:
        p.ZAddNX(r.K, r.V.([]redis.Z)...)
        p.Expire(r.K, DefaultExpire)
    default:
        panic(fmt.Sprintf("unkown redis type %s", r.T))
    }
}

type Rows []*Row

func (rs Rows) GetTypes(client *redis.Client) error {
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
    p := client.Pipeline()
    for _, row := range rs {
        row.AccuGet(p)
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

func (rs Rows) MSet(client *redis.Client) error {
    p := client.Pipeline()
    for _, row := range rs {
        row.AccuSet(p)
    }
    cmders, cmdErr := p.Exec()
    return parseErr(cmders, cmdErr)
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

    flag.Parse()
    if *pSlot == -1 {
        log.Errorf("slot not provided")
        return
    }
    log.Infof("migrating slot %d", *pSlot)

    var (
    	srcClient = redis.NewClient(&redis.Options{
            Network:            "tcp",
            Addr:               "10.129.100.195:11111",
            DialTimeout:        30*time.Second,
            ReadTimeout:        30*time.Second,
            WriteTimeout:       30*time.Second,
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

            if err := rows.GetTypes(srcClient); err != nil {
                return nil, -1, err
            }
            if err := rows.MGet(srcClient); err != nil {
                return nil, -1, err
            }
            return
        }

        targetClient = redis.NewClient(&redis.Options{
            Network:            "tcp",
            //Addr:               "localhost:6379",
            Addr:               "ads.proxy.copi.live.sg.cloud.shopee.io:10078",
            DialTimeout:        30*time.Second,
            ReadTimeout:        30*time.Second,
            WriteTimeout:       30*time.Second,
        })

        cursorID = 0
    )

    for round := 0;;round++{
        var (
        	rows Rows
        	err error
        )
        if rows, cursorID, err = scan(cursorID); err != nil {
            log.Errorf("migration failed: '%v'", err)
            return
        }

        if err = rows.MSet(targetClient); err != nil {
            log.Errorf("migration failed: '%v'", err)
            return
        }

        if cursorID == 0 {
            break
        }

        if round % 10 == 0 {
            fmt.Printf("round %d finished\n", round)
        }
    }
    log.Infof("migration succeeded")
}
