package main

import (
	"container/list"
	"context"
	"flag"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	codis "github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/go-redis/redis"
	"github.com/golang/glog"
	"github.com/leisurelyrcxf/redis-tools/cmd"
	"github.com/leisurelyrcxf/redis-tools/cmd/common"
	"github.com/leisurelyrcxf/redis-tools/cmd/utils"
	"github.com/leisurelyrcxf/redis-tools/versioninfo"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	defaultValueSize         = 2
	defaultOverheadSize      = 20
	defaultPipelineSize      = 100
)

type Task struct {
	cmd.Rows

	cmd.RedisType
	lowKey, highKey int64
	valueSize       int
	value           string
	memtier         bool

	maxSlotNum int
	slots      []int

	errCount atomic.Int64
}

func (t *Task) String() string {
	return fmt.Sprintf("Task{%d-%d, value-size:%d, memtier:%v", t.lowKey, t.highKey, t.valueSize, t.memtier)
}

func NewTask(redisType cmd.RedisType, lowKey, highKey int64, valueSize int, memtier bool) *Task {
	task := (&Task{
		RedisType: redisType,
		lowKey:    lowKey,
		highKey:   highKey,
		valueSize: valueSize,
		memtier:   memtier,
		errCount:  *atomic.NewInt64(-1),
	}).GenValue()
	for key := task.lowKey; key < task.highKey; key++ {
		strKey := genKey(key, task.memtier)
		task.Rows = append(task.Rows, &cmd.Row{
			K:                    strKey,
			T:                    task.RedisType,
			OverwriteExistedKeys: true,
			V:                    task.getValue(strKey),
			TargetNotExists:      true,
		})
	}
	return task
}

func NewVerifyTask(redisType cmd.RedisType, lowKey, highKey int64, valueSize int, memtier bool, maxSlotNum int, slots []int) *Task {
	task := (&Task{
		RedisType:  redisType,
		lowKey:     lowKey,
		highKey:    highKey,
		valueSize:  valueSize,
		memtier:    memtier,
		maxSlotNum: maxSlotNum,
		slots:      slots,
		errCount:   *atomic.NewInt64(-1),
	}).GenValue()
	for key := task.lowKey; key < task.highKey; key++ {
		row := &cmd.Row{
			K:                    genKey(key, task.memtier),
			T:                    task.RedisType,
			OverwriteExistedKeys: true,
			V:                    task.value,
			TargetNotExists:      true,
		}
		if ContainsKey(slots, row.K, maxSlotNum) {
			task.Rows = append(task.Rows, row)
		}
	}
	return task
}

func (t *Task) GenValue() *Task {
	t.value = generateRandomString(t.valueSize)
	return t
}

func (t *Task) VerifyValue(key string, val interface{}) error {
	if val == nil {
		return fmt.Errorf("nil value")
	}
	switch t.RedisType {
	case cmd.RedisTypeString:
		if len(val.(string)) != t.valueSize {
			return fmt.Errorf("value len not match, expect %d, but got '%s'", t.valueSize, val)
		}
		glog.V(101).Infof("string key '%s' ok", key)
		return nil
	case cmd.RedisTypeHash:
		value, ok := val.(map[string]string)
		if !ok {
			return fmt.Errorf("expect type map[string]string, got %T", t.value)
		}
		if len(value) != 4 {
			return fmt.Errorf("value len not match expect 4 fields, but got '%s'", value)
		}
		if value["k1"] != "v1" {
			return fmt.Errorf("value not match for field 'k1', expect '%s', but got '%s'", "v1", value["k1"])
		}
		if value["k2"] != "v2" {
			return fmt.Errorf("value not match for field 'k2', expect '%s', but got '%s'", "v2", value["k2"])
		}
		if value["k3"] != "v3" {
			return fmt.Errorf("value not match for field 'k3', expect '%s', but got '%s'", "v3", value["k3"])
		}
		if len(value[key]) != t.valueSize {
			return fmt.Errorf("value len not match for filed 'k4', expect %d, but got '%s'", t.valueSize, value[key])
		}
		glog.V(101).Infof("hash key '%s' ok", key)
		return nil
	default:
		panic(fmt.Sprintf("unsupported type '%s'", t.RedisType))
	}
}

func (t *Task) Finish(errCount int) {
	if errCount < 0 {
		panic("[Finish] errCount < 0")
	}
	t.errCount.Store(int64(errCount))
}

func (t *Task) IsFinished() bool {
	return t.GetErrCount() >= 0
}

func (t *Task) GetErrCount() int64 {
	return t.errCount.Load()
}

func (t *Task) Verify(ctx context.Context, cli *redis.Client) {
	if err := utils.ExecWithRetryRedis(func() error {
		return t.Rows.MGet(cli, false, math.MaxInt64)
	}, common.DefaultMaxRetry, common.DefaultRetryInterval); err != nil {
		log.Errorf("MGet failed: '%v'", err)
		t.Finish(len(t.Rows))
		return
	}
	var errCount int
	for _, row := range t.Rows {
		if err := t.VerifyValue(row.K, row.V); err != nil {
			glog.Warningf("verify value failed for key '%s': '%v'", row.K, err)
			errCount += 1
		}
	}
	t.Finish(errCount)
}

func (t *Task) Write(ctx context.Context, cli *redis.Client) {
	if err := utils.ExecWithRetryRedis(func() error {
		return t.Rows.MSet(cli, len(t.Rows), true)
	}, common.DefaultMaxRetry, common.DefaultRetryInterval); err != nil {
		log.Errorf("MSet failed: '%v'", err)
		t.Finish(len(t.Rows))
		return
	}
	t.Finish(0)
}

func (t *Task) getValue(key string) interface{} {
	switch t.RedisType {
	case cmd.RedisTypeString:
		return generateRandomString(t.valueSize)
	case cmd.RedisTypeHash:
		return map[string]string{"k1": "v1", "k2": "v2", "k3": "v3", key: t.value}
	default:
		panic(fmt.Sprintf("unsupported type '%s'", t.RedisType))
	}
}

func minInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func genKey(i int64, memtier bool) string {
	if memtier {
		return fmt.Sprintf("memtier-%d", i)
	}
	return fmt.Sprintf("key:%012d", i)
}

func getDBSizeAttr(isPika bool, isRedisKV bool) string {
	if isPika {
		return "db_size"
	}
	if isRedisKV {
		return "total_db_size"
	}
	return "used_memory"
}

func getCurrentDBSize(serverAddr string) (int64, error) {
	c, err := codis.NewClientNoAuth(serverAddr, time.Minute)
	if err != nil {
		return -1, err
	}
	m, err := c.Info()
	if err != nil {
		return -1, err
	}
	var isPika = false
	if _, ok := m["db_size"]; ok {
		isPika = true
	}
	var isRedisKV = false
	if _, ok := m["total_db_size"]; ok {
		isRedisKV = true
	}
	usedMemoryString, ok := m[getDBSizeAttr(isPika, isRedisKV)]
	if !ok {
		return -1, fmt.Errorf("can't get '%s'", getDBSizeAttr(isPika, isRedisKV))
	}
	return strconv.ParseInt(usedMemoryString, 10, 64)
}

func fillRedis(
	ctx context.Context,
	dbId int,
	serverAddr string, password string,
	threadNum int, redisType cmd.RedisType,
	lowKey, highKey int64,
	taskSize int,
	valueSize int, maxDBSize int64, memtier bool,
	stopWhenFailed bool) int {
	var (
		taskCh       = make(chan *Task, 64)
		taskFinishCh = make(chan *Task, 64)

		writtenUntil  int64 = 0
		totalErrCount int64 = 0

		wg sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var (
			flyingTasks   = list.New()
			currentDBSize int64
		)

		updateFlyingTasks := func(finishedTask *Task) {
			for e := flyingTasks.Front(); e != nil; {
				t := e.Value.(*Task)
				if !t.IsFinished() {
					break
				}

				next := e.Next()
				flyingTasks.Remove(e)
				e = next

				totalErrCount += t.GetErrCount()
				writtenUntil = t.highKey
			}

			// update current db size
			currentDBSize = 0
			if totalErrCount > 0 && stopWhenFailed {
				currentDBSize = math.MaxInt64
			}
			if finishedTask.GetErrCount() > 0 && stopWhenFailed {
				currentDBSize = math.MaxInt64
			}
			if dbSize, err := getCurrentDBSize(serverAddr); err != nil {
				currentDBSize = math.MaxInt64
			} else {
				glog.Infof("%s written in total until %d, total error count: %d\n",
					bytesize.Int64(dbSize).HumanString(), writtenUntil-1, totalErrCount)
				if dbSize > currentDBSize {
					currentDBSize = dbSize
				}
			}
		}

	LoopTasks:
		for curKey := lowKey; curKey <= highKey && currentDBSize < maxDBSize; curKey += int64(taskSize) {
			task := NewTask(redisType, curKey, minInt64(curKey+int64(taskSize), highKey), valueSize, memtier)
			for {
				select {
				case taskCh <- task:
					flyingTasks.PushBack(task)
					continue LoopTasks
				case finishedTask := <-taskFinishCh:
					updateFlyingTasks(finishedTask)
				}
			}
		}
		glog.Infof("finished scheduled")
		close(taskCh)

		for flyingTasks.Len() > 0 {
			select {
			case finishedTask := <-taskFinishCh:
				updateFlyingTasks(finishedTask)
			}
		}
		glog.Infof("flying tasks cleared")

		if currentDBSize < maxDBSize {
			if writtenUntil != highKey {
				panic("currentDBSize < maxDBSize && writtenUntil != highKey")
			}
		} else {
			println(fmt.Sprintf("write until %d", writtenUntil))
		}
	}()

	var rdb = redis.NewClient(&redis.Options{
		Addr:         serverAddr,
		Password:     password, // no password set
		DB:           dbId,     // use default DB
		DialTimeout:  60 * time.Second,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
		PoolSize:     int(float64(threadNum) * 1.2),
		MinIdleConns: threadNum,
	})

	for i := 0; i < threadNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for task := range taskCh {
				task.Write(ctx, rdb)
				taskFinishCh <- task
			}
		}()
	}

	wg.Wait()
	return int(totalErrCount)
}

var hexDigits = []byte{
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
}

func generateRandomString(length int) string {
	//return strings.Repeat("A", length)
	rand.Seed(time.Now().UTC().UnixNano())
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = hexDigits[rand.Int()%len(hexDigits)] // nolint:gosec
	}
	return string(bytes)
}

func VersionString(ver, rev, buildAt string) string {
	version := ""
	version += fmt.Sprintf("Version:        %s\n", ver)
	version += fmt.Sprintf("Git hash:       %s\n", rev)
	version += fmt.Sprintf("Built:          %s\n", buildAt)
	version += fmt.Sprintf("Golang version: %s\n", runtime.Version())
	version += fmt.Sprintf("OS/Arch:        %s/%s\n", runtime.GOOS, runtime.GOARCH)
	return version
}

func Version() string {
	return VersionString(
		versioninfo.VERSION,
		versioninfo.REVISION,
		versioninfo.BUILTAT,
	)
}

func main() {
	dbId := flag.Int("db", 0, "db id")
	dbSizeStr := flag.String("db-size", "128mb", "db size")
	valueSize := flag.Int("value-size", defaultValueSize, "value size")
	overHeadSize := flag.Int("overhead-size", defaultOverheadSize, "overhead size ")
	maxKeyCoef := flag.Int("max-key-coef", 1, "max key coefficient")
	pipelineSize := flag.Int("pipeline", defaultPipelineSize, "pipeline size")

	host := flag.String("host", "localhost", "redis host")
	port := flag.Int("port", 6379, "redis port")
	password := flag.String("password", "", "redis password")
	threadNum := flag.Int("thread-num", 50, "thread num")
	timeout := flag.Duration("timeout", 7*24*time.Hour, "timeout")
	startKey := flag.Int64("start-key", 0, "start key")
	maxKey := flag.Int64("max-key", math.MaxInt64, "max key")
	dontUseMemtier := flag.Bool("dont-use-memtier", false, "dont use memtier")
	version := flag.Bool("version", false, "show version info")
	dontStopWhenFailed := flag.Bool("dont-stop-when-failed", false, "dont stop when failed")

	dataType := flag.String("data-type", "", "data type, can be 'string' or 'hash")

	doVerify := flag.Bool("verify", false, "verify data")
	maxSlotNum := flag.Int("max-slot-num", 0, "max slot num")
	var myFlags IntsFlag
	flag.Var(&myFlags, "slots", "slots for this server, can be set multiple times")

	flag.Parse()
	if *version {
		fmt.Print(Version())
		return
	}

	dbSize, err := bytesize.Parse(*dbSizeStr)
	if err != nil {
		glog.Errorf("invalid db size: %s", *dbSizeStr)
		panic(err)
	}
	var (
		memtier        = !*dontUseMemtier
		defaultKeySize = len(genKey(100000000, memtier))
	)
	if *maxKey == 0 {
		if *doVerify {
			println("must provide high key in verify mode")
			os.Exit(1)
		}
		*maxKey = dbSize / int64(defaultKeySize+*valueSize+*overHeadSize) * int64(*maxKeyCoef)
	}
	if *doVerify {
		fmt.Printf("verify until (estimated) max key: %d, start key: %d, use memtier: %v\n\n", *maxKey, *startKey, memtier)
	} else {
		fmt.Printf("write until (estimated) max key: %d, start key: %d, use memtier: %v\n\n", *maxKey, *startKey, memtier)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if *doVerify {
		errCount := verify(ctx, fmt.Sprintf("%s:%d", *host, *port), *password, *threadNum, cmd.MustParseRedisType(*dataType), *startKey, *maxKey,
			*pipelineSize, *valueSize, memtier, *maxSlotNum, myFlags, !*dontStopWhenFailed)
		if errCount == 0 {
			fmt.Printf("\nVerification succeeded\n")
			return
		}
		fmt.Printf("\nError Count in total: %d\n", errCount)
		os.Exit(1)
		return
	}
	errCount := fillRedis(ctx, *dbId, fmt.Sprintf("%s:%d", *host, *port), *password, *threadNum, cmd.MustParseRedisType(*dataType),
		*startKey, *maxKey, *pipelineSize, *valueSize, dbSize, memtier, !*dontStopWhenFailed)
	glog.Flush()
	fmt.Printf("\nError Count in total: %d\n", errCount)
}

func scan(ctx context.Context, cli *redis.Client, maxSlotNum, slot int, batchSize int) (err error) {
	var (
		cursorID = 0
	)
	for rounds := 0; ; rounds++ {
		var newCursorID int
		if newCursorID, err = scanOnce(ctx, cli, maxSlotNum, slot, cursorID, batchSize); err != nil {
			return err
		}
		if cursorID = newCursorID; cursorID == 0 {
			break
		}
	}
	return nil
}

func scanOnce(ctx context.Context, cli *redis.Client, maxSlotNum, slot int, cid int, batchSize int) (newCid int, err error) {
	result, err := cli.Do([]interface{}{"SLOTSSCAN", slot, cid, "count", batchSize}...).Result()
	if err != nil {
		return 0, err
	}
	resultArray, ok := result.([]interface{})
	if !ok {
		return -1, fmt.Errorf("result type not []interface{}")
	}
	if len(resultArray) != 2 {
		return -1, fmt.Errorf("result not 2 rows")
	}
	newCursorString := fmt.Sprintf("%v", resultArray[0])
	if newCid, err = strconv.Atoi(newCursorString); err != nil {
		return -1, fmt.Errorf("result '%s' not int", newCursorString)
	}
	keysResult, ok := resultArray[1].([]interface{})
	if !ok {
		return -1, fmt.Errorf("rows result type not []interface{}")
	}
	for _, keyResult := range keysResult {
		key, ok := keyResult.(string)
		if !ok {
			return -1, fmt.Errorf("key type not sring")
		}
		if Dispatch(key, maxSlotNum) != slot {
			return -1, fmt.Errorf("[scanOnce] slot of key '%s' not match, expect %d, got %d, probably meta not cleared", key, slot, Dispatch(key, maxSlotNum))
		}
	}
	return newCid, nil
}

func verify(
	ctx context.Context,
	serverAddr string, password string,
	threadNum int, redisType cmd.RedisType,
	lowKey, highKey int64,
	taskSize int,
	valueSize int,
	memtier bool,
	maxSlotNum int,
	slots []int,
	stopWhenFailed bool) int {
	if maxSlotNum != 0 {
		println(fmt.Sprintf("max_slot_num: %d, slots: %v", maxSlotNum, slots))
	}

	if maxSlotNum != 0 {
		var (
			rdb = redis.NewClient(&redis.Options{
				Addr:         serverAddr,
				Password:     password, // no password set
				DB:           0,        // use default DB
				DialTimeout:  60 * time.Second,
				ReadTimeout:  60 * time.Second,
				WriteTimeout: 60 * time.Second,
			})
		)
		for _, slot := range slots {
			if err := scan(ctx, rdb, maxSlotNum, slot, 1000); err != nil {
				glog.Errorf("[verify] scan failed: '%v'", err)
				return 1111
			}
			glog.Infof("[verify] slot %d meta cleared", slot)
		}
	}

	var (
		taskCh       = make(chan *Task, 64)
		taskFinishCh = make(chan struct{}, 64)

		readUntil     int64 = 0
		totalErrCount int64 = 0

		wg sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var (
			flyingTasks = list.New()
			stop        bool
		)

		updateFlyingTasks := func() {
			for e := flyingTasks.Front(); e != nil; {
				t := e.Value.(*Task)
				if !t.IsFinished() {
					break
				}

				next := e.Next()
				flyingTasks.Remove(e)
				e = next

				if totalErrCount += t.GetErrCount(); totalErrCount > 0 && stopWhenFailed {
					stop = true
				}
				readUntil = t.highKey
				glog.Infof("read until %d, total error count: %d\n", readUntil-1, totalErrCount)
			}
		}

	LoopTasks:
		for curKey := lowKey; curKey < highKey && !stop; curKey += int64(taskSize) {
			task := NewVerifyTask(redisType, curKey, minInt64(curKey+int64(taskSize), highKey), valueSize, memtier, maxSlotNum, slots)
			for {
				select {
				case taskCh <- task:
					flyingTasks.PushBack(task)
					continue LoopTasks
				case <-taskFinishCh:
					updateFlyingTasks()
				}
			}
		}
		close(taskCh)

		for flyingTasks.Len() > 0 {
			select {
			case <-taskFinishCh:
				updateFlyingTasks()
			}
		}
	}()

	for i := 0; i < threadNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			reader(ctx, serverAddr, password, taskCh, taskFinishCh)
		}()
	}

	wg.Wait()
	return int(totalErrCount)
}

func reader(
	ctx context.Context,
	serverAddr string, password string,
	taskCh <-chan *Task, taskFinishCh chan<- struct{}) {
	var (
		rdb = redis.NewClient(&redis.Options{
			Addr:         serverAddr,
			Password:     password, // no password set
			DB:           0,        // use default DB
			DialTimeout:  60 * time.Second,
			ReadTimeout:  60 * time.Second,
			WriteTimeout: 60 * time.Second,
		})
	)

	for task := range taskCh {
		task.Verify(ctx, rdb)
		taskFinishCh <- struct{}{}
	}
	return
}
