package main

import (
    "ads-recovery/cmd"
    "bufio"
    "context"
    "flag"
    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"
    "io"
    "math/rand"
    "net"
    "os"
    "strings"
    "sync"
    "sync/atomic"
    "time"
)

func main() {
    addr := flag.String("addr", "", "addr")
    threadNum := flag.Int("thread-num", 10, "thread number")
    logLevel := flag.String("log-level", "info", "log level, can be 'panic', 'error', 'fatal', 'warn', 'info'")
    keyFileName := flag.String("key-file", "/tmp/hash.keys", "key file path")
    keyCount := flag.Int("key-count", 1000000, "key count")
    duration := flag.Duration("duration", time.Second*15, "test duration")

    flag.Parse()
    if *addr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*addr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    lvl, err := log.ParseLevel(*logLevel)
    if err != nil {
        log.Fatalf("invalid log level: '%v'", err)
    }
    log.SetLevel(lvl)
    if *threadNum <= 0 {
        log.Fatalf("invalid thread num %d", *threadNum)
    }

    keys := loadKeys(*keyFileName)
    if len(keys) != *keyCount {
        log.Fatalf("keys number(%d) != expKeyCount(%d)", len(keys), *keyCount)
    }

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
        totalUserSeconds, totalSucessCount, totalErrCount int64
    )
    ctx, cancel := context.WithTimeout(context.Background(), *duration)
    defer cancel()

    var wg sync.WaitGroup
    for i := 0; i <= *threadNum; i++ {
        wg.Add(1)

        go func() {
            defer wg.Done()

            for ctx.Err() == nil {
                rows := make(cmd.Rows, 500)

                rand.Seed(time.Now().UnixNano())
                for i := 0; i < len(rows); i++ {
                    rows[i] = &cmd.Row{
                        K: keys[rand.Intn(len(keys))],
                        T: cmd.RedisTypeHash,
                        V: nil,
                    }
                }

                start := time.Now()
                _ = rows.MGet(cli)
                successCount, errCount := rows.Stats()
                atomic.AddInt64(&totalSucessCount, successCount)
                atomic.AddInt64(&totalUserSeconds, int64(time.Since(start)/time.Microsecond)*successCount)
                atomic.AddInt64(&totalErrCount, errCount)
            }
        }()
    }

    wg.Wait()
    log.Infof("failed commands: %d", totalErrCount)
    log.Infof("qps: %.2f command/s", float64(totalSucessCount)/float64(*duration/time.Second))
    log.Infof("avg latency: %.2f ms", float64(totalUserSeconds)/float64(totalSucessCount)/1000)
}

func loadKeys(keyFileName string) []string {
    fr, err := os.Open(keyFileName)
    if err != nil {
        log.Fatalf("can't open key file '%s': '%v'", keyFileName, err)
        return nil
    }
    defer fr.Close()

    var keys []string
    br := bufio.NewReader(fr)
    for {
        line, _, err := br.ReadLine()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Fatalf("failed to readline: '%v'", err)
            return nil
        }
        keys = append(keys, strings.TrimSpace(string(line)))
    }
    return keys
}