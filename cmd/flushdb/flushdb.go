package main

import (
    "ads-recovery/cmd"
    "flag"
    "github.com/go-redis/redis"
    log "github.com/sirupsen/logrus"
    "net"
    "time"
)

func main()  {
    sourceAddr := flag.String("addr", "", "source addr")

    flag.Parse()
    if *sourceAddr == "" {
        log.Fatalf("source addr not provided")
    }
    if _, _, err := net.SplitHostPort(*sourceAddr); err != nil {
        log.Fatalf("source addr not valid: %v", err)
    }
    log.Infof("flushdb of %s", *sourceAddr)

    var (
        srcClient = redis.NewClient(&redis.Options{
            Network:            "tcp",
            Addr:               *sourceAddr,
            DialTimeout:        120*time.Second,
            ReadTimeout:        120*time.Second,
            WriteTimeout:       120*time.Second,
            IdleCheckFrequency: time.Second*10,
        })
    )

    const batchSize = 1000

    for slot := 0; slot < 16384; slot++ {
        var cursorID = 0
        for round := 1; ; round++ {
            var (
                rows cmd.Rows
                err  error
            )
            if rows, cursorID, err = cmd.Scan(srcClient, cursorID, slot, batchSize); err != nil {
                log.Fatalf("scan slot %d failed: '%v'", slot, err)
            }

            if err = rows.Delete(srcClient); err != nil {
                log.Fatalf("Del slot %d failed: '%v'", slot, err)
            }

            if cursorID == 0 {
                if (slot+1) % 256 == 0 {
                    log.Infof("del all keys of slot %d", slot)
                }
                break
            }

            if round%100 == 0 {
                log.Infof("round %d finished for slot %d", round, slot)
            }
        }
    }
    log.Infof("del keys succeeded")
}