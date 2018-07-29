package main

import (
	"fmt"
	"time"

	breaker "github.com/oif/circuit-breaker"
)

var bk *breaker.CircuitBreaker

func init() {
	opt := breaker.Option{
		GenerationInterval:   2 * time.Second,
		OpenStateExpiry:      2 * time.Second,
		FailureThreshold:     3,
		SuccessThreshold:     5,
		HalfOpenRequestLimit: 10,
		OnStateChange:        onChange,
	}
	bk = breaker.New(opt)
}

func onChange(ev breaker.Event) {
	fmt.Printf("[%s] State change from %v to %v due to '%s'\n",
		ev.When, ev.From, ev.To, ev.Reason)
}

func do(success bool, times int) {
	for i := 1; i <= times; i++ {
		_, err := bk.Do(func() (interface{}, error) {
			if success {
				return nil, nil
			} else {
				err := fmt.Errorf("manual error times: %d", i)
				return nil, err
			}
		})
		fmt.Printf("[%d] %v, %v", i, bk.Counts(), bk.State())
		if err != nil {
			fmt.Printf(", error: %s", err)
		}
		fmt.Println()
	}
}

func main() {
	do(false, 10)
	fmt.Println("Sleep 2s")
	time.Sleep(2 * time.Second)
	do(true, 2)
	do(false, 1)
	do(true, 2)
	fmt.Println("Sleep 2s")
	time.Sleep(2 * time.Second)
	do(true, 6)
}
