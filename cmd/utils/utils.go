package utils

import (
    "strconv"
    "strings"
    "time"

    log "github.com/sirupsen/logrus"
)

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