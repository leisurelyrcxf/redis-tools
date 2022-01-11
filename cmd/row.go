package cmd

import (
    "fmt"
    "math"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"
)

var DefaultExpire time.Duration = 0

type RedisType string

const (
    RedisTypeUnknown RedisType = ""
    RedisTypeString  RedisType = "string"
    RedisTypeList RedisType = "list"
    RedisTypeHash    RedisType = "hash"
    RedisTypeSet RedisType = "set"
    RedisTypeZset    RedisType= "zset"
)

var RedisTypes = []RedisType{
    RedisTypeString,
    RedisTypeList,
    RedisTypeHash,
    RedisTypeSet,
    RedisTypeZset,
}

func Scan(cli *redis.Client, slot int, cid int, batchSize int) (rows Rows, newCid int, err error) {
    result, err := cli.Do([]interface{}{"SLOTSSCAN", slot, cid, "count", batchSize}...).Result()
    if err != nil {
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

    return rows, newCid, nil
}

func ParseRedisType(typeStr string) (RedisType, error) {
    for _, redisType := range RedisTypes {
        if string(redisType) == typeStr {
            return redisType, nil
        }
    }
    return RedisTypeUnknown, fmt.Errorf("unknown redis type '%s'", typeStr)
}


// AtomicInt64 is a wrapper with a simpler interface around atomic.(Add|Store|Load|CompareAndSwap)Int64 functions.
type AtomicInt64 struct {
    int64
}

// NewAtomicInt64 initializes a new AtomicInt64 with a given value.
func NewAtomicInt64(n int64) AtomicInt64 {
    return AtomicInt64{n}
}

// Add atomically adds n to the value.
func (i *AtomicInt64) Add(n int64) int64 {
    return atomic.AddInt64(&i.int64, n)
}

// Set atomically sets n as new value.
func (i *AtomicInt64) Set(n int64) {
    atomic.StoreInt64(&i.int64, n)
}

// Get atomically returns the current value.
func (i *AtomicInt64) Get() int64 {
    return atomic.LoadInt64(&i.int64)
}

// CompareAndSwap automatically swaps the old with the new value.
func (i *AtomicInt64) CompareAndSwap(oldVal, newVal int64) (swapped bool) {
    return atomic.CompareAndSwapInt64(&i.int64, oldVal, newVal)
}

type Stat struct {
    SumCard  AtomicInt64
    KeyCount AtomicInt64

    sync.Mutex
    MaxCard  AtomicInt64
    MaxCardKey string

    // Output
    AvgCard float64
}

func (es *Stat) collect(row *Row) {
    es.SumCard.Add( int64(row.Cardinality))
    es.KeyCount.Add(1)
    if row.Cardinality > int(es.MaxCard.Get())  {
        es.Lock()
        if row.Cardinality > int(es.MaxCard.Get()) {
            es.MaxCard.Set(int64(row.Cardinality))
            es.MaxCardKey = row.K
        }
        es.Unlock()
    }
}

func (es *Stat) calc() {
    if es.KeyCount.Get() == 0 {
        es.AvgCard = math.NaN()
    }
    es.AvgCard = float64(es.SumCard.Get())/float64(es.KeyCount.Get())
}

type Stats struct {
    perRedisType map[RedisType]*Stat
}

func NewStats() *Stats {
    st := &Stats{perRedisType: make(map[RedisType]*Stat)}
    for _, typ := range RedisTypes {
        st.perRedisType[typ]= &Stat{}
    }
    return st
}

func (ess *Stats) Collect(rows Rows) {
    for _, row := range rows {
        ess.perRedisType[row.T].collect(row)
    }
}

func (ess *Stats) Calc() {
    for _, stats := range ess.perRedisType {
        stats.calc()
    }
}

func (ess *Stats) ForEachType(f func(RedisType, *Stat)) {
    for _, typ := range RedisTypes {
        f(typ, ess.perRedisType[typ])
    }
}

func (ess *Stats) String() string {
    ess.Calc()
    var sb strings.Builder
    sb.WriteString("-------------------------------------------------------------------\n")
    sb.WriteString("Stats:\n")
    ess.ForEachType(func(redisType RedisType, stat *Stat) {
        if redisType == RedisTypeString {
            return
        }
        if math.IsNaN(stat.AvgCard) {
            if stat.MaxCard.Get() != 0 {
                panic("stat.AvgCard.IsNaN() && stat.MaxCard != 0")
            }
            return
        }
        sb.WriteString(fmt.Sprintf("    %s: avg_card: %.1f, max_card: %d, max_card_key: %s\n", redisType, stat.AvgCard, stat.MaxCard.Get(), stat.MaxCardKey))
    })
    sb.WriteString("-------------------------------------------------------------------\n")
    return sb.String()
}

type Row struct {
    K                    string
    T                    RedisType
    OverwriteExistedKeys bool
    V                    interface{} // string, map[string]string, []redis.Z
    TargetNotExists      bool
    Cardinality          int
}

func (r *Row) Get(p redis.Pipeliner) {
    switch r.T {
    case RedisTypeString:
        p.Get(r.K)
    case RedisTypeList:
        p.LRange(r.K, 0, -1)
    case RedisTypeHash:
        p.HGetAll(r.K)
    case RedisTypeSet:
        p.SMembers(r.K)
    case RedisTypeZset:
        p.ZRangeWithScores(r.K, 0, -1)
    default:
        panic(fmt.Sprintf("unknown redis type %s", r.T))
    }
}

func (r *Row) ParseValue(cmder redis.Cmder) (obj interface{}, err error) {
    commandString := ""
    defer func() {
        if err != nil {
            log.Errorf("cmd '%s' failed: %v",commandString, err)
        }
    }()
    switch command := cmder.(type) {
    case *redis.StringCmd:
        commandString = command.String()
        return command.Result()
    case *redis.StringSliceCmd:
        // set or list
        commandString = command.String()
        return command.Result()
    case *redis.StringStringMapCmd:
        commandString = command.String()
        return command.Result()
    case *redis.ZSliceCmd:
        commandString = command.String()
        return command.Result()
    default:
        panic(fmt.Sprintf("unknown command type %T", cmder))
    }
}

func (r *Row) Set(p redis.Pipeliner) {
    switch r.T {
    case RedisTypeString:
        p.SetNX(r.K, r.V, DefaultExpire) // TODO check this.
    case RedisTypeList:
        for _ ,v := range r.V.([]string) {
            p.RPush(r.K, v)
        }
        if DefaultExpire > 0 {
            p.Expire(r.K, DefaultExpire)
        }
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
    case RedisTypeSet:
        values := make([]interface{}, len(r.V.([]string)))
        for idx, v := range r.V.([]string) {
            values[idx] = v
        }
        p.SAdd(r.K, values...)
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

func (r *Row) XCard(p redis.Pipeliner) {
    switch r.T {
    case RedisTypeString:
        p.Exists(r.K) // If you skip this, then needs special handling in Rows::Card
    case RedisTypeList:
        p.LLen(r.K)
    case RedisTypeHash:
        p.HLen(r.K)
    case RedisTypeSet:
        p.SCard(r.K)
    case RedisTypeZset:
        p.ZCard(r.K)
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
    case RedisTypeList, RedisTypeSet:
        return len(r.V.([]string)) == 0
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

func (rs Rows) Types(client *redis.Client) (err error) {
    if len(rs) == 0 {
        return nil
    }

    defer func() {
        if err != nil {
            rs[0].T = RedisTypeUnknown
        }
    }()
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
            log.Errorf("type unknown: %v", err)
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
        if err := rs.Exists(target); err != nil {
            log.Errorf("rs.Exists failed: '%v'", err)
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

func (rs Rows) Exists(target *redis.Client) error {
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

func (rs Rows) Card(target *redis.Client) error {
    if len(rs) == 0 {
        return nil
    }

    if rs[0].T == RedisTypeUnknown {
        if err := rs.Types(target); err != nil {
            log.Errorf("can't get types: '%v'", err)
            return err
        }
    }

    p := target.Pipeline()
    for _, row := range rs {
        row.XCard(p)
    }
    cmders, cmdErr := p.Exec()
    if cmdErr != nil {
        return cmdErr
    }
    for idx, cmder := range cmders {
        card, err := cmder.(*redis.IntCmd).Result()
        if err != nil {
            log.Errorf("cmd '%s' failed: %v", cmder.(*redis.IntCmd).String(), err)
            return err
        }
        if card > math.MaxInt32 {
            panic(fmt.Sprintf("card(%d) > math.MaxInt32", card))
        }
        rs[idx].Cardinality = int(card)
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

func (rs Rows) Filter(filter func(*Row)bool) Rows {
    ret := make(Rows, 0, len(rs))
    for _, r := range rs {
        if filter(r) {
            ret = append(ret, r)
        }
    }
    return ret
}

func parseErr(cmders []redis.Cmder, err error) (resultErr error) {
    if err != nil {
        log.Errorf("pipeline failed: %v", err)
        return err
    }
    var cmdString string
    defer func() {
        if resultErr != nil {
            log.Errorf("cmd '%s' failed: %v", cmdString, err)
        }
    }()
    for _, cmder := range cmders {
        switch cmd := cmder.(type) {
        case *redis.StatusCmd:
            cmdString = cmd.String()
            _, err := cmd.Result()
            return err
        case *redis.BoolCmd:
            cmdString = cmd.String()
            _, err := cmd.Result()
            return err
        case *redis.IntCmd:
            cmdString = cmd.String()
            if _, err := cmd.Result(); err != nil {
                return err
            }
        default:
            panic(fmt.Sprintf("unknown cmder type %T", cmder))
        }
    }
    return nil
}
