package main

import (
    "flag"
    "github.com/leisurelyrcxf/redis-tools/cmd/common"
    "io"
    "os"
    "time"

    log "github.com/sirupsen/logrus"

    "github.com/leisurelyrcxf/redis-tools/cmd"
    "github.com/leisurelyrcxf/redis-tools/cmd/utils"
)

const (
    maxRetry      = 10
    retryInterval = time.Second*5
)

func main()  {
    c := common.Flags("Diff all keys by cardinality", true)
    outputFileName := flag.String("output", "compare-result", "output file name")
    outputToStd := flag.Bool("output-to-std", false, "output to std")

    c.Parse()
    if *outputFileName == "" {
        log.Fatalf("output file name empty")
    }


    file, err := os.Create(*outputFileName)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    var w io.StringWriter = file
    if *outputToStd {
        w = utils.AlsoOutputStd(w)
    }

    var (
        input  = make(chan cmd.Rows, c.MaxBuffered)
        scannedBatches, successfulReadBatches, failedReadBatches, successfulWriteBatches, failedWriteBatches, diffBatches int64
    )
    c.ScanSlotsAsync(&scannedBatches, input)
    output := cmd.DiffAsync(input, c.ReaderCount, c.WriterCount, c.SrcClient, c.TargetClient, c.MaxBuffered, maxRetry, retryInterval,
        &successfulReadBatches, &failedReadBatches, &successfulWriteBatches, &failedWriteBatches, &diffBatches)

    for rows := range output {
        for _, row := range rows {
            if err := row.D.Output(w); err != nil {
                log.Fatalf("Output error: %v", err)
            }
            if _, err = w.WriteString("\n"); err != nil {
                log.Fatalf("Write error: %v", err)
            }
        }
    }

    if diffBatches == 0 {
        _, _ = w.WriteString("source and target are the same")
    }

    if failedReadBatches == 0 && failedWriteBatches == 0 {
        log.Infof("migration succeeded")
    } else {
        if failedReadBatches > 0 {
            log.Errorf("%d read batches failed", failedReadBatches)
        }
        if failedWriteBatches > 0 {
            log.Errorf("%d write batches failed", failedWriteBatches)
        }
    }
    if failedReadBatches + successfulReadBatches != scannedBatches && failedWriteBatches + successfulWriteBatches != successfulReadBatches{
        log.Fatalf("failedReadBatches(%d) + successfulReadBatches(%d) != scannedBatches(%d) && failedWriteBatches(%d) + successfulWriteBatches(%d) != successfulReadBatches(%d)",
            failedReadBatches, successfulReadBatches, scannedBatches, failedWriteBatches, successfulWriteBatches, successfulReadBatches)
    }
    if failedReadBatches + successfulReadBatches != scannedBatches {
        log.Fatalf("failedReadBatches(%d) + successfulReadBatches(%d) != scannedBatches(%d)",
            failedReadBatches, successfulReadBatches, scannedBatches)
    }
    if failedWriteBatches + successfulWriteBatches != successfulReadBatches{
        log.Fatalf("failedWriteBatches(%d) + successfulWriteBatches(%d) != successfulReadBatches(%d)",
            failedWriteBatches, successfulWriteBatches, successfulReadBatches)
    }
}