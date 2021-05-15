package cmd

import (
    "fmt"
    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"
    "time"
)

var DefaultExpire time.Duration = 0

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

func ParseRedisType(typeStr string) (RedisType, error) {
    for _, redisType := range RedisTypes {
        if string(redisType) == typeStr {
            return redisType, nil
        }
    }
    return RedisTypeUnknown, fmt.Errorf("unknown redis type '%s'", typeStr)
}

type Row struct {
    K string
    T                    RedisType
    OverwriteExistedKeys bool
    V                    interface{}  // string, map[string]string, []redis.Z
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
        p.SetNX(r.K, r.V, DefaultExpire)
    case RedisTypeHash:
        valueMap := make(map[string]interface{})
        for k ,v := range r.V.(map[string]string) {
            valueMap[k] = v
        }
        if len(valueMap) == 0 {
            log.Panicf("len(valueMap) == 0 for key %v(%v)", r.K, r.T)
        }
        p.HMSet(r.K, valueMap)
        if DefaultExpire > 0 {
            p.Expire(r.K, DefaultExpire)
        }
    case RedisTypeZset:
        if len(r.V.([]redis.Z)) == 0 {
            log.Panicf("len(r.V.([]redis.Z)) == 0 for key %v(%v)", r.K, r.T)
        }
        p.ZAddNX(r.K, r.V.([]redis.Z)...)
        if DefaultExpire > 0 {
            p.Expire(r.K, DefaultExpire)
        }
    default:
        panic(fmt.Sprintf("unknown redis type %s", r.T))
    }
}

func (r *Row) Delete(p redis.Pipeliner) {
    p.Del(r.K)
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

func (rs Rows) Keys() []string{
    keys := make([]string, len(rs))
    for i, r := range rs {
        keys[i] = r.K
    }
    return keys
}

func (rs Rows) Types(client *redis.Client) error {
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
        if rs[idx].T, err = ParseRedisType(typeString); err != nil {
            return err
        }
    }
    return nil
}

func (rs Rows) MGet(client *redis.Client) error {
    if len(rs) == 0 {
        return nil
    }
    if rs[0].T == RedisTypeUnknown {
        if err := rs.Types(client); err != nil {
            log.Errorf("can't get types: '%v'", err)
            return err
        }
    }
    // TODO ignore wrong types error
    p := client.Pipeline()
    for _, row := range rs {
        row.Get(p)
    }
    cmders, cmdErr := p.Exec()
    if cmdErr != nil {
        log.Errorf("Rows::MGet pipeline execution failed: %v", cmdErr)
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
    if len(rs) == 0 {
        return nil
    }
    if !rs[0].OverwriteExistedKeys {
        if err := rs.exists(target); err != nil {
            log.Errorf("rs.exists failed: '%v'", err)
            return err
        }
    }

    p := target.Pipeline()
    for _, row := range rs {
        if row.OverwriteExistedKeys || row.TargetNotExists {
            if row.V == nil {
                log.Warnf("skip nil value of key %s, type: %v", row.K, row.T)
            } else if row.IsValueEmpty() {
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

func (rs Rows) Delete(target *redis.Client) error {
    p := target.Pipeline()
    for _, row := range rs {
        row.Delete(p)
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

func (rs Rows) Stats() (successCount, errCount int64) {
    for _, row := range rs {
        if row.V == nil {
            errCount++
        } else {
            successCount++
        }
    }
    return successCount, errCount
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
