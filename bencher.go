package sql

import (
	"fmt"
	"reflect"
	"runtime"
	"time"
)

func simplebanch(N int, testingFunction func()) {
	var sum int64
	var channels = make([]chan int64, 0, N)
	for i := 0; i < N; i++ {
		channels = append(channels, make(chan int64))
	}
	for i := 0; i < N; i++ {
		go func(i int) {
			var startPoint = time.Now()
			defer func() {
				channels[i] <- time.Since(startPoint).Nanoseconds()
			}()
			testingFunction()
		}(i)
	}
	for _, ch := range channels {
		sum += <-ch
	}
	fmt.Printf("BENCHMARK %s ON %d ATTEMPTS.\n RESULT: %d NANOSECONDS", GetFunctionName(testingFunction), N, sum/int64(N))
}

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
