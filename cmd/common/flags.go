package common

import "C"
import (
    "flag"
    "github.com/go-redis/redis"
    "github.com/leisurelyrcxf/redis-tools/cmd"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
    log "github.com/sirupsen/logrus"
    "net"
    "time"
)

type Common struct {
    Slots      []int
    SrcClient, TargetClient *redis.Client
    LogLevel     log.Level
    RedisType cmd.RedisType
    Expire   time.Duration
    BatchSize int
    ReaderCount int
    WriterCount int
    MaxBuffered int
    
    flagMaxSlotNum *int
    flagSlotsDesc  *string
    flagSourceAddr *string
    flagTargetAddr *string
    flagLogLevel *string
    flagDataType  *string
    flagExpire      *time.Duration
    flagBatchSize   *int
    flagReaderCount *int
    flagWriterCount *int
    flagMaxBuffered *int
}

func (c *Common) Parse()  {
    flag.Parse()

    c.parseSlots()
    c.parseAddrs()

    var err error
    c.LogLevel, err = log.ParseLevel(*c.flagLogLevel)
    if err != nil {
        log.Fatalf("invalid log level: '%v'", err)
    }
    log.SetLevel(c.LogLevel)

    c.RedisType = cmd.RedisTypeUnknown
    if *c.flagDataType != "" {
        var err error
        if c.RedisType, err = cmd.ParseRedisType(*c.flagDataType); err != nil {
            log.Fatalf("unknown data type: %v", *c.flagDataType)
        }
    }

    if c.Expire = *c.flagExpire; c.Expire > 0 {
        // TODO inherit old expire
        cmd.DefaultExpire = c.Expire
    }

    c.BatchSize = *c.flagBatchSize
    c.ReaderCount = *c.flagReaderCount
    c.WriterCount = *c.flagWriterCount
    c.MaxBuffered = *c.flagMaxBuffered
    return
}

func (c *Common) parseSlots() {
    c.Slots = utils.ParseSlots(*c.flagMaxSlotNum, *c.flagSlotsDesc)
}

func (c *Common) parseAddrs() {
    if *c.flagSourceAddr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*c.flagSourceAddr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    if *c.flagTargetAddr == "" {
        log.Fatalf("target addr not provided")
    }
    if _, _, err := net.SplitHostPort(*c.flagTargetAddr); err != nil {
        log.Fatalf("target addr not valid: %v", err)
    }

    c.SrcClient = redis.NewClient(&redis.Options{
        Network:            "tcp",
        Addr:               *c.flagSourceAddr,
        DialTimeout:        240*time.Second,
        ReadTimeout:        240*time.Second,
        WriteTimeout:       240*time.Second,
        PoolSize:50,
        IdleCheckFrequency: time.Second*10,
    })
    c.TargetClient = redis.NewClient(&redis.Options{
        Network:            "tcp",
        Addr:               *c.flagTargetAddr,
        DialTimeout:        240*time.Second,
        ReadTimeout:        240*time.Second,
        WriteTimeout:       240*time.Second,
        PoolSize: 50,
        IdleCheckFrequency: time.Second*10,
    })

    if err := utils.CheckRedisConn(c.SrcClient); err != nil {
        log.Fatalf("can't connect to src '%s'", err)
    }

    if err := utils.CheckRedisConn(c.TargetClient); err != nil {
        log.Fatalf("can't connect to src '%s'", err)
    }
}

func Flags() Common {
    return Common{
        flagMaxSlotNum:  flag.Int("max-slot-num", 0, "max slot number"),
        flagSlotsDesc:   flag.String("slots",  "-1,-2", "slots"),
        flagSourceAddr:  flag.String("source-addr", "", "source addr"),
        flagTargetAddr:  flag.String("target-addr", "", "target addr"),
        flagLogLevel:    flag.String("log-level", "error", "log level, can be 'panic', 'error', 'fatal', 'warn', 'info'"),
        flagDataType:    flag.String("data-type", "", "data types could be 'string', 'hash', 'zset'"),
        flagExpire:      flag.Duration("expire", 0, "expire time"),
        flagBatchSize:   flag.Int("batch-size", 256, "batch size"),
        flagReaderCount: flag.Int("reader", 4, "reader count"),
        flagWriterCount: flag.Int("writer", 4, "writer count"),
        flagMaxBuffered: flag.Int("max-buffered", 256, "max buffered batch size"),
    }
}
