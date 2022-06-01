package cmd

import (
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/leisurelyrcxf/redis-tools/cmd/utils"
	"io"
	"math"
	"reflect"
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
	RedisTypeList    RedisType = "list"
	RedisTypeHash    RedisType = "hash"
	RedisTypeSet     RedisType = "set"
	RedisTypeZset    RedisType = "zset"
)

var RedisTypes = []RedisType{
	RedisTypeString,
	RedisTypeList,
	RedisTypeHash,
	RedisTypeSet,
	RedisTypeZset,
}

type Pipeline struct {
	*redis.Pipeline

	useTxPipeline bool
	cap           int
	cli           *redis.Client
}

func NewPipeline(cli *redis.Client, cap int, useTxPipeline bool) *Pipeline {
	p := &Pipeline{
		useTxPipeline: useTxPipeline,
		cap:           cap,
		cli:           cli,
	}
	p.Pipeline = p.GetPipeline()
	return p
}

func (p *Pipeline) Size() int {
	if p.Pipeline == nil {
		return 0
	}
	v := reflect.ValueOf(p.Pipeline)
	y := v.Elem().FieldByName("cmds")
	l := y.Len()
	return l
}

func (p *Pipeline) TryExec() error {
	if p.Size() < p.cap {
		return nil
	}

	err := p.Exec()
	p.Pipeline = p.GetPipeline()
	return err
}

func (p *Pipeline) GetPipeline() *redis.Pipeline {
	if !p.useTxPipeline {
		return p.cli.Pipeline().(*redis.Pipeline)
	}
	return p.cli.TxPipeline().(*redis.Pipeline)
}

func (p *Pipeline) Exec() error {
	err := parseErr(p.Pipeline.Exec())
	p.Pipeline = nil
	return err
}

func Scan(cli *redis.Client, slot int, cursorId int, batchSize int, typ RedisType, overwriteExistedKeys, deleteTarget bool) (rows Rows, newCid int, err error) {
	result, err := cli.Do([]interface{}{"SLOTSSCAN", slot, cursorId, "count", batchSize}...).Result()
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
		rows = append(rows, &Row{K: key, T: typ,
			OverwriteExistedKeys: overwriteExistedKeys, DeleteTargetBeforeOverwrite: deleteTarget,
			Cardinality: -1})
	}

	return rows, newCid, nil
}

func ScanSlotsAsync(cli *redis.Client, slots []int,
	batchSize, maxRetry int, retryInterval time.Duration, scannedBatches *int64, rawRowsCh chan<- Rows) {
	ScanSlotsRawAsync(cli, slots, RedisTypeUnknown, false, false, batchSize, maxRetry, retryInterval, scannedBatches, rawRowsCh)
}

func ScanSlotsRawAsync(cli *redis.Client, slots []int, typ RedisType, overwriteExistedKeys bool, delTargetKeyBeforeOverwrite bool,
	batchSize, maxRetry int, retryInterval time.Duration, scannedBatches *int64, scannedRows chan<- Rows) {
	log.Infof("Scan slots: %v\n"+
		"Data type: '%s'\n"+
		"Overwrite existed keys: %v", slots, typ, overwriteExistedKeys)
	go func() {
		defer close(scannedRows)

		for _, slot := range slots {
			var cursorID = 0
			for round := 1; ; round++ {
				var (
					rawRows Rows
					err     error
				)

				for i := 0; ; i++ {
					var newCursorID int
					if rawRows, newCursorID, err = Scan(cli, slot, cursorID, batchSize, typ, overwriteExistedKeys, delTargetKeyBeforeOverwrite); err != nil {
						if i >= maxRetry-1 {
							log.Errorf("scan cursor %d failed: '%v' @round %d", cursorID, err, i)
							return
						}
						log.Warningf("scan cursor %d failed: '%v' @round %d, retrying in %s...", cursorID, err, i, retryInterval)
						time.Sleep(retryInterval)
						continue
					}
					cursorID = newCursorID
					break
				}

				if len(rawRows) > 0 {
					scannedRows <- rawRows
					atomic.AddInt64(scannedBatches, 1)
				}

				if cursorID == 0 {
					log.Warningf("Scanned all keys for slot %d", slot)
					break
				}

				if round%1000 == 0 {
					log.Warningf("scanned %d keys for slot %d", batchSize*round, slot)
				}
			}
		}
	}()
}

func ParseRedisType(typeStr string) (RedisType, error) {
	for _, redisType := range RedisTypes {
		if string(redisType) == typeStr {
			return redisType, nil
		}
	}
	return RedisTypeUnknown, fmt.Errorf("unknown redis type '%s'", typeStr)
}

func MustParseRedisType(typeStr string) RedisType {
	typ, err := ParseRedisType(typeStr)
	if err != nil {
		log.Fatalf("parse redis type failed: '%s'", typeStr)
		return RedisTypeUnknown
	}
	return typ
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
	MaxCard    AtomicInt64
	MaxCardKey string

	// Output
	AvgCard float64
}

func (es *Stat) collect(row *Row) {
	es.SumCard.Add(int64(row.Cardinality))
	es.KeyCount.Add(1)
	if row.Cardinality > es.MaxCard.Get() {
		es.Lock()
		if row.Cardinality > es.MaxCard.Get() {
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
	es.AvgCard = float64(es.SumCard.Get()) / float64(es.KeyCount.Get())
}

type Stats struct {
	perRedisType map[RedisType]*Stat
}

func NewStats() *Stats {
	st := &Stats{perRedisType: make(map[RedisType]*Stat)}
	for _, typ := range RedisTypes {
		st.perRedisType[typ] = &Stat{}
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
		if math.IsNaN(stat.AvgCard) {
			if stat.MaxCard.Get() != 0 {
				panic("stat.AvgCard.IsNaN() && stat.MaxCard != 0")
			}
			return
		}
		sb.WriteString(fmt.Sprintf("    %s: avg_card: %.1f, max_card: %d, max_card_key: %s, key_count: %d\n", redisType, stat.AvgCard, stat.MaxCard.Get(), stat.MaxCardKey, stat.KeyCount.Get()))
	})
	sb.WriteString("-------------------------------------------------------------------\n")
	return sb.String()
}

type Diff interface {
	Output(w io.StringWriter) error
}

type Row struct {
	K                           string
	T                           RedisType
	OverwriteExistedKeys        bool
	DeleteTargetBeforeOverwrite bool
	V                           interface{} // string, map[string]string, []redis.Z
	D                           Diff
	TargetNotExists             bool
	Cardinality                 int64
}

func (r *Row) IsBigKey(largeObjCard int64) bool {
	return r.T != RedisTypeString && r.Cardinality >= largeObjCard
}

func (r *Row) Get(p redis.Pipeliner, largeObjCard int64) {
	if r.IsBigKey(largeObjCard) {
		p.Ping()
		return
	}

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

func ParseValue(cmder redis.Cmder) (obj interface{}, err error) {
	commandString := ""
	defer func() {
		if err != nil {
			log.Errorf("cmd '%s' failed: %v", commandString, err)
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

type StringDiff struct {
	K           string
	Mine, Other string
}

func (s *StringDiff) Output(w io.StringWriter) error {
	_, err := w.WriteString(fmt.Sprintf("Key: %s, Mine: %s, Other: %s", s.K, s.Mine, s.Other))
	return err
}

type CardDiff struct {
	K           string
	Mine, Other int64
}

func (s *CardDiff) Output(w io.StringWriter) error {
	_, err := w.WriteString(fmt.Sprintf("Key: %s, Mine_card: %d, Other_card: %d", s.K, s.Mine, s.Other))
	return err
}

func (r *Row) CalcDiff(val interface{}) Diff {
	switch r.T {
	case RedisTypeString:
		if r.V.(string) != val.(string) {
			return &StringDiff{
				K:     r.K,
				Mine:  r.V.(string),
				Other: val.(string),
			}
		}
	case RedisTypeList, RedisTypeSet:
		if card := int64(len(val.([]string))); r.Cardinality != card {
			return &CardDiff{
				K:     r.K,
				Mine:  r.Cardinality,
				Other: card,
			}
		}
	case RedisTypeHash:
		if card := int64(len(val.(map[string]string))); r.Cardinality != card {
			return &CardDiff{
				K:     r.K,
				Mine:  r.Cardinality,
				Other: card,
			}
		}
	case RedisTypeZset:
		if card := int64(len(val.([]redis.Z))); r.Cardinality != card {
			return &CardDiff{
				K:     r.K,
				Mine:  r.Cardinality,
				Other: card,
			}
		}
	default:
		panic(fmt.Sprintf("unknown redis type %s", r.T))
	}
	return nil
}

func (r *Row) Set(p *Pipeline) error {
	switch r.T {
	case RedisTypeString:
		p.SetNX(r.K, r.V, DefaultExpire)
		return nil
	case RedisTypeList:
		p.Del(r.K)
		for _, v := range r.V.([]string) {
			p.RPush(r.K, v)
			if err := p.TryExec(); err != nil {
				return err
			}
		}
		if DefaultExpire > 0 {
			p.Expire(r.K, DefaultExpire)
		}
		return nil
	case RedisTypeHash:
		for k, v := range r.V.(map[string]string) {
			p.HSet(r.K, k, v)
			if err := p.TryExec(); err != nil {
				return err
			}
		}
		if DefaultExpire > 0 {
			p.Expire(r.K, DefaultExpire)
		}
		return nil
	case RedisTypeSet:
		for _, v := range r.V.([]string) {
			p.SAdd(r.K, v)
			if err := p.TryExec(); err != nil {
				return err
			}
		}
		if DefaultExpire > 0 {
			utils.Assert(false) // TODO temporarily forbidden
			p.Expire(r.K, DefaultExpire)
		}
		return nil
	case RedisTypeZset:
		if len(r.V.([]redis.Z)) == 0 {
			log.Panicf("len(r.V.([]redis.Z)) == 0 for key %v(%v)", r.K, r.T)
		}
		for _, z := range r.V.([]redis.Z) {
			p.ZAddNX(r.K, z)
			if err := p.TryExec(); err != nil {
				return err
			}
		}
		if DefaultExpire > 0 {
			p.Expire(r.K, DefaultExpire)
		}
		return nil
	default:
		panic(fmt.Sprintf("unknown redis type %s", r.T))
	}
}

func (r *Row) XCard(p redis.Pipeliner) {
	switch r.T {
	case RedisTypeString:
		p.StrLen(r.K) // If you skip this, then needs special handling in Rows::Card
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

func (r *Row) Del(p redis.Pipeliner) {
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

func (r *Row) MigrateLargeKey(src *redis.Client, target *redis.Client, batchSize int, logExistedKey bool,
	maxRetry int, retryInterval time.Duration) error {
	if !r.OverwriteExistedKeys {
		cmd := target.Exists(r.K)
		existsVal, err := cmd.Result()
		if err != nil {
			return errors.Errorf("cmd '%s' failed: %v", cmd.String(), err)
		}
		if r.TargetNotExists = existsVal == 0; !r.TargetNotExists {
			if logExistedKey {
				log.Warnf("skip existed key %s", r.K)
			}
			return nil
		}
	} else if r.DeleteTargetBeforeOverwrite {
		if err := utils.ExecWithRetryRedis(func() error {
			return target.Del(r.K).Err()
		}, maxRetry, retryInterval); err != nil {
			log.Errorf("delete key '%s' failed: %v", r.K, err)
			return err
		}
	}

	switch r.T {
	case RedisTypeSet:
		var cursorID = uint64(0)
		for round := 0; ; round++ {
			var (
				fields      []string
				newCursorID uint64
			)
			if err := utils.ExecWithRetryRedis(func() (err error) {
				fields, newCursorID, err = src.SScan(r.K, cursorID, "", int64(batchSize)).Result()
				return err
			}, maxRetry, retryInterval); err != nil {
				log.Errorf("SScan key '%s' cursor: %d failed: '%v'", r.K, cursorID, err)
				return err
			}

			if len(fields) > 0 {
				if err := utils.ExecWithRetryRedis(func() error {
					num, err := target.SAdd(r.K, utils.StringArray2ObjectArray(fields)...).Result()
					utils.Assert(err != nil || num <= int64(len(fields)))
					return err
				}, maxRetry, retryInterval); err != nil {
					log.Errorf("SAdd for big key '%s' failed: %v", r.K, err)
					return err
				}
			}

			if newCursorID == 0 {
				log.Warningf("SScan iterated all fields for big key '%s'", r.K)
				break
			}

			if round%1000 == 0 {
				log.Warningf("SScan iterated roughly %d fields for big key '%s'", round*batchSize, r.K)
			}

			cursorID = newCursorID
		}
		return nil
	case RedisTypeHash:
		var cursorID = uint64(0)
		for round := 0; ; round++ {
			var (
				fields      []string
				newCursorID uint64
			)
			if err := utils.ExecWithRetryRedis(func() (err error) {
				fields, newCursorID, err = src.HScan(r.K, cursorID, "", int64(batchSize)).Result()
				return err
			}, maxRetry, retryInterval); err != nil {
				log.Errorf("HScan key '%s' cursor: %d failed: '%v'", r.K, cursorID, err)
				return err
			}

			if len(fields) > 0 {
				ret, err := utils.StringArray2HashMap(fields)
				if err != nil {
					log.Errorf("HScan key '%s' return invalid scan results: %v", r.K, fields)
					return err
				}
				if err := utils.ExecWithRetryRedis(func() error {
					return target.HMSet(r.K, ret).Err()
				}, maxRetry, retryInterval); err != nil {
					log.Errorf("HMSet for big key '%s' failed: %v", r.K, err)
					return err
				}
			}

			if newCursorID == 0 {
				log.Warningf("HScan migrated all fields for big key '%s'", r.K)
				break
			}

			if round%1000 == 0 {
				log.Warningf("HScan iterated roughly %d fields for big key '%s'", round*batchSize, r.K)
			}

			cursorID = newCursorID
		}
		return nil
	case RedisTypeZset:
		var cursorID = uint64(0)
		for round := 0; ; round++ {
			var (
				fields      []string
				newCursorID uint64
			)
			if err := utils.ExecWithRetryRedis(func() (err error) {
				fields, newCursorID, err = src.ZScan(r.K, cursorID, "", int64(batchSize)).Result()
				return err
			}, maxRetry, retryInterval); err != nil {
				log.Errorf("ZScan key '%s' cursor: %d failed: '%v'", r.K, cursorID, err)
				return err
			}

			if len(fields) > 0 {
				zs, err := utils.StringArray2ZArray(fields)
				if err != nil {
					log.Errorf("ZScan key '%s' return invalid scan results: err: '%v', results: %v", r.K, err, fields)
					return err
				}
				if err := utils.ExecWithRetryRedis(func() error {
					return target.ZAdd(r.K, zs...).Err()
				}, maxRetry, retryInterval); err != nil {
					log.Errorf("ZAdd for big key '%s' failed: %v", r.K, err)
					return err
				}
			}

			if newCursorID == 0 {
				log.Warningf("ZScan migrated all fields for big key '%s'", r.K)
				break
			}

			if round%1000 == 0 {
				log.Warningf("ZScan iterated roughly %d fields for big key '%s'", round*batchSize, r.K)
			}

			cursorID = newCursorID
		}
		return nil
	default:
		return fmt.Errorf("migrate key '%s' of type '%s' not supported", r.K, r.T)
	}
}

type Rows []*Row

func (rs Rows) Keys() []string {
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
			return errors.Errorf("cmd '%s' failed: %v", cmder.(*redis.StatusCmd).String(), err)
		}
		if rs[idx].T, err = ParseRedisType(typeString); err != nil {
			log.Errorf("type unknown: %v", err)
			return err
		}
	}
	return nil
}

func (rs Rows) MGet(client *redis.Client, checkLargeObj bool, largeObjCard int64) error {
	if len(rs) == 0 {
		return nil
	}

	if rs[0].T == RedisTypeUnknown {
		if err := rs.Types(client); err != nil {
			return errors.Errorf("MGet::Types failed: %v", err)
		}
	}

	if checkLargeObj && rs[0].Cardinality < 0 {
		if err := rs.MCard(client); err != nil {
			return errors.Errorf("MGet::MCard failed: %v", err)
		}
	}

	p := client.Pipeline()
	for _, row := range rs {
		row.Get(p, largeObjCard)
	}
	cmders, cmdErr := p.Exec()
	if cmdErr != nil {
		return errors.Errorf("Rows::MGet pipeline execution failed: %v", cmdErr)
	}
	for idx, cmder := range cmders {
		if _, ok := cmder.(*redis.StatusCmd); ok {
			log.Warnf("Skipped large key: %s(Card: %d, Type: %s)", rs[idx].K, rs[idx].Cardinality, rs[idx].T)
			rs[idx].V = nil
			continue
		}
		val, err := ParseValue(cmder)
		if err != nil {
			return err
		}
		rs[idx].V = val
	}
	return nil
}

func (rs Rows) MDiff(client *redis.Client, replaceCard bool) error {
	if len(rs) == 0 {
		return nil
	}
	if rs[0].T == RedisTypeUnknown {
		panic("MDiff: unknwon type")
	}
	p := client.Pipeline()
	for _, row := range rs {
		row.XCard(p)
	}
	cmders, cmdErr := p.Exec()
	if cmdErr != nil {
		return errors.Errorf("Rows::MGet pipeline execution failed: %v", cmdErr)
	}
	for idx, cmder := range cmders {
		card, err := cmder.(*redis.IntCmd).Result()
		if err != nil {
			return errors.Errorf("cmd '%s' failed: %v", cmder.(*redis.IntCmd).String(), err)
		}
		if card > math.MaxInt32 {
			panic(fmt.Sprintf("card(%d) > math.MaxInt32", card))
		}
		if card != rs[idx].Cardinality {
			rs[idx].D = &CardDiff{
				K:     rs[idx].K,
				Mine:  rs[idx].Cardinality,
				Other: card,
			}
			if replaceCard {
				rs[idx].Cardinality = card
			}
		}
	}
	return nil
}

func (rs Rows) MSet(target *redis.Client, pipelineCap int, logExistedKey, useTxPipeline bool) error {
	if len(rs) == 0 {
		return nil
	}
	if !rs[0].OverwriteExistedKeys {
		if err := rs.MExists(target); err != nil {
			return errors.Errorf("rs.Exists failed: '%v'", err)
		}
	}

	p := NewPipeline(target, pipelineCap, useTxPipeline)
	for _, row := range rs {
		if row.OverwriteExistedKeys || row.TargetNotExists {
			if row.OverwriteExistedKeys && row.DeleteTargetBeforeOverwrite {
				p.Del(row.K)
			}

			if row.V == nil {
				log.Warnf("skip nil value of key %s, type: %v", row.K, row.T)
			} else if row.IsValueEmpty() {
				log.Warnf("skip empty value of key %s, type: %v", row.K, row.T)
			} else {
				if err := row.Set(p); err != nil {
					return err
				}
			}
		} else if logExistedKey {
			log.Warnf("skip existed key %s", row.K)
		}
	}
	return p.Exec()
}

func (rs Rows) MDel(target *redis.Client) error {
	p := target.Pipeline()
	for _, row := range rs {
		row.Del(p)
	}
	cmders, cmdErr := p.Exec()
	return parseErr(cmders, cmdErr)
}

func (rs Rows) MExists(target *redis.Client) error {
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
			return errors.Errorf("cmd '%s' failed: %v", cmder.(*redis.IntCmd).String(), err)
		}
		rs[idx].TargetNotExists = existsVal == 0
	}
	return nil
}

func (rs Rows) MCard(target *redis.Client) error {
	if len(rs) == 0 {
		return nil
	}

	if rs[0].T == RedisTypeUnknown {
		if err := rs.Types(target); err != nil {
			return errors.Errorf("Rows::MCard: can't get types: '%v'", err)
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
			return errors.Errorf("cmd '%s' failed: %v", cmder.(*redis.IntCmd).String(), err)
		}
		if card > math.MaxInt32 {
			return errors.Errorf("card(%d) > math.MaxInt32", card)
		}
		rs[idx].Cardinality = card
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

func (rs Rows) Filter(filter func(*Row) bool) (filtered, nonFiltered Rows) {
	for _, r := range rs {
		if filter(r) {
			filtered = append(filtered, r)
		} else {
			nonFiltered = append(nonFiltered, r)
		}
	}
	return
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

func DiffAsync(input <-chan Rows, readerCount, writerCount int,
	srcClient *redis.Client, targetClient *redis.Client,
	maxBuffered int, maxRetry int, retryInterval time.Duration,
	successfulReadBatches, failedReadBatches, successfulWriteBatches, failedWriteBatches, diffBatches *int64) (output chan Rows) {
	output = make(chan Rows)
	var (
		readerWg sync.WaitGroup
		rowsRead = make(chan Rows, maxBuffered)
	)

	for i := 0; i < readerCount; i++ {
		readerWg.Add(1)

		go func() {
			defer readerWg.Done()

			for rows := range input {
				if err := utils.ExecWithRetryRedis(func() error {
					if err := rows.Types(srcClient); err != nil {
						return errors.Errorf("Rows::MCard: can't get types: '%v'", err)
					}
					return rows.MCard(targetClient)
				}, maxRetry, retryInterval); err != nil {
					atomic.AddInt64(failedReadBatches, 1)
					log.Errorf("[DiffAsync][Manual] MCard failed: %v, keys: %v", err, rows.Keys())
				} else {
					rowsRead <- rows
					tmp := atomic.AddInt64(successfulReadBatches, 1)
					log.Infof("[DiffAsync] MCard %d batches successfully", tmp)
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
		writerWg sync.WaitGroup
	)
	for i := 0; i < writerCount; i++ {
		writerWg.Add(1)

		go func(i int) {
			defer writerWg.Done()

			for rows := range rowsRead {
				if err := utils.ExecWithRetryRedis(func() error {
					return rows.MDiff(srcClient, true)
				}, maxRetry, retryInterval); err != nil {
					atomic.AddInt64(failedWriteBatches, 1)
					log.Errorf("[DiffAsync][Manual] MDiff failed: '%v' keys: %v", err, rows.Keys())
				} else {
					tmp := atomic.AddInt64(successfulWriteBatches, 1)
					log.Infof("[DiffAsync] MDiff %d batches successfully", tmp)
					if rows, _ = rows.Filter(func(row *Row) bool {
						return row.D != nil
					}); len(rows) > 0 {
						atomic.AddInt64(diffBatches, 1)
						output <- rows
					}
				}
			}
		}(i)
	}

	go func() {
		writerWg.Wait()
		log.Infof("all comparator finished, close rowsRead")
		close(output)
	}()
	return output
}
