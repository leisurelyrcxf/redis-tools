package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/golang/glog"
	"time"
)

const DefaultMaxRetry = 10

type CommandType string

var CommandTypeGet CommandType = "get"
var CommandTypeSet CommandType = "set"
var CommandTypeHMSet CommandType = "hmset"

type CommandInfo struct {
	CommandType CommandType
	Key         string
	Value       interface{}
	Expiration  time.Duration
}

type Pipeliner struct {
	Delegate redis.Pipeliner
	commands []CommandInfo

	rdb      *redis.Client
	maxRetry int
}

func NewPipeliner(rdb *redis.Client) *Pipeliner {
	return &Pipeliner{
		Delegate: rdb.Pipeline(),
		rdb:      rdb,
		maxRetry: DefaultMaxRetry,
	}
}

func (p *Pipeliner) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	cmd := p.Delegate.Set(ctx, key, value, expiration)
	p.commands = append(p.commands, CommandInfo{
		CommandType: CommandTypeSet,
		Key:         key,
		Value:       value,
		Expiration:  expiration,
	})
	return cmd
}

func (p *Pipeliner) HMSet(ctx context.Context, key string, value map[string]interface{}) *redis.BoolCmd {
	cmd := p.Delegate.HMSet(ctx, key, value)
	p.commands = append(p.commands, CommandInfo{
		CommandType: CommandTypeHMSet,
		Key:         key,
		Value:       value,
	})
	return cmd
}

func (p *Pipeliner) Exec(ctx context.Context) int {
	for i := 0; ; i++ {
		cmds, err := p.Delegate.Exec(ctx)
		errCount := getWriteErrCount(cmds, err)
		if errCount <= 0 || i >= p.maxRetry {
			return errCount
		}
		glog.Infof("retrying pipeline...")
		p.recover(ctx)
		time.Sleep(time.Second)
	}
}

func (p *Pipeliner) recover(ctx context.Context) {
	p.Delegate = p.rdb.Pipeline()
	for _, cmd := range p.commands {
		switch cmd.CommandType {
		case CommandTypeSet:
			p.Delegate.Set(ctx, cmd.Key, cmd.Value, cmd.Expiration)
		case CommandTypeHMSet:
			p.Delegate.HMSet(ctx, cmd.Key, cmd.Value)
		default:
			panic(fmt.Sprintf("unsupported command type: %s", cmd.CommandType))
		}
	}
}

func getWriteErrCount(cmders []redis.Cmder, err error) int {
	if err != nil {
		glog.Errorf("write pipeline failed: %v", err)
		return len(cmders)
	}
	var errCount int
	for _, cmder := range cmders {
		switch v := cmder.(type) {
		case *redis.StatusCmd:
			if _, err := v.Result(); err != nil {
				glog.Errorf("cmd '%s' failed: %v", v.String(), err)
				errCount++
			}
		case *redis.BoolCmd:
			if _, err := v.Result(); err != nil {
				glog.Errorf("cmd '%s' failed: %v", v.String(), err)
				errCount++
			}
		default:
			panic(fmt.Sprintf("unknown cmd type '%T'", v))
		}
	}
	return errCount
}
