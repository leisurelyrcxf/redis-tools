package utils

import (
    "fmt"
    "github.com/go-redis/redis"
    "io"
    "strconv"
    "strings"
    "time"

    log "github.com/sirupsen/logrus"
)

type alsoOutputStd struct {
    w io.StringWriter
}

func (a alsoOutputStd) WriteString(str string) (n int, err error) {
    n, err = a.w.WriteString(str)
    fmt.Print(str)
    return n, err
}

func AlsoOutputStd(w io.StringWriter) io.StringWriter {
    return alsoOutputStd{w:w}
}

func Assert(b bool) {
    if !b {
        panic("assert failed")
    }
}

func ParseSlots(maxSlotNum int, slotsDesc string) []int {
    var slots []int
    if maxSlotNum == 0 && slotsDesc == "" {
        log.Fatalf("max slot number is 0 && slot == 0")
    }
    if maxSlotNum != 0 {
        for slot := 0; slot < maxSlotNum; slot++ {
            slots = append(slots, slot)
        }
    } else {
        if strings.Contains(slotsDesc, "-") {
            parts := strings.Split(slotsDesc, "-")
            if len(parts) != 2 {
                log.Fatalf("len(parts) != 2")
            }
            first, err := strconv.Atoi(parts[0])
            if err != nil {
                log.Fatalf("first invalid: %v", err)
            }
            second, err := strconv.Atoi(parts[1])
            if err != nil {
                log.Fatalf("second invalid: %v", err)
            }
            for i := first; i <= second; i++ {
                slots = append(slots, i)
            }
            if len(slots) == 0 {
                log.Fatalf("len(slots) == 0")
            }
        } else {
            parts := strings.Split(slotsDesc, ",")
            slots = make([]int, len(parts))
            for idx, part := range parts {
                slot, err := strconv.Atoi(part)
                if err != nil {
                    log.Fatalf("invalid slots: %v", err)
                }
                slots[idx] = slot
            }
        }
    }
    return slots
}

func CheckRedisConn(cli *redis.Client) error {
    ret, err := cli.Ping().Result()
    if err != nil {
        return err
    }
    if ret != "PONG" {
        return fmt.Errorf("not PONG")
    }
    return nil
}

func StringArray2ObjectArray(strings []string) []interface{} {
    objects := make([]interface{}, len(strings))
    for idx, fd := range strings {
        objects[idx] = fd
    }
    return objects
}

func StringArray2HashMap(strings []string) (map[string]interface{}, error) {
    if len(strings) & 1 == 1 {
        return nil, fmt.Errorf("wrong result of hscan, odd string list")
    }
    m := make(map[string]interface{})
    var key string
    for _, fd := range strings {
        if key != "" {
            m[key] = fd
            key = ""
        } else {
            key = fd
        }
    }
    return m, nil
}

func StringArray2ZArray(strings []string) (result []redis.Z, err error) {
    if len(strings) & 1 == 1 {
        return nil, fmt.Errorf("wrong result of zscan, odd string list")
    }
    var member string
    for _, fd := range strings {
        if member != "" {
            f, err := strconv.ParseFloat(fd, 64)
            if err != nil {
                return nil, err
            }
            result = append(result, redis.Z{
                Score:  f,
                Member: member,
            })
            member = ""
        } else {
            member = fd
        }
    }
    return result, nil
}

func ExecWithRetryRedis(exec func()error, maxRetry int, retryInterval time.Duration) error {
    return ExecWithRetry(exec, maxRetry, retryInterval, IsRetryableRedisErr)
}

func ExecWithRetry(exec func() error, maxRetry int, retryInterval time.Duration, isRetryableErr func(err error) bool) error {
    for i := 0; ; i++ {
        if err := exec(); err != nil {
            if i >= maxRetry- 1 || !isRetryableErr(err) {
                return err
            }
            log.Warnf("Read failed: '%v' @round %d, retrying in %s...", err, i, retryInterval)
            time.Sleep(retryInterval)
            continue
        }
        break
    }
    return nil
}

func IsRetryableRedisErr(err error) bool {
    return strings.Contains(err.Error(), "broken pipe") || err == io.EOF
}