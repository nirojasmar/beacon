package obs

import (
	"log"
	"os"
	"time"

	"beacon/internal/config"
)

func StartFaults(cfg *config.Config) {
	if cfg.FaultCrashAfterSeconds > 0 {
		go func() {
			time.Sleep(time.Duration(cfg.FaultCrashAfterSeconds) * time.Second)
			log.Printf("fault injection: exiting after configured delay")
			os.Exit(1)
		}()
	}
	if cfg.FaultLeakMBPerMinute > 0 {
		go func() {
			retained := make([][]byte, 0)
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				size := int(cfg.FaultLeakMBPerMinute * 1024 * 1024)
				if size > 0 {
					retained = append(retained, make([]byte, size))
					log.Printf("fault injection: retained %d MiB in %d allocations", size/(1024*1024), len(retained))
				}
			}
		}()
	}
}
