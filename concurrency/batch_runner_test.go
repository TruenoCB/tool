package concurrency

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBatchRunner(t *testing.T) {
	testFunc := func(batchNumber, expect int) {
		runner := NewBatchRunner()
		for i := 0; i < batchNumber; i++ {
			i := i
			runner.Submit(func() error {
				time.Sleep(100 * time.Millisecond)
				if i%7 == 0 {
					panic("panic")
				}
				if i%10 == 0 {
					return errors.New("err occur")
				}
				return nil
			})
		}
		runner.Run(10)
		fmt.Println(runner.ErrorMap)
		assert.Equal(t, expect, len(runner.ErrorMap))
	}
	wg := sync.WaitGroup{}
	wg.Add(300)
	for test := 0; test < 100; test++ {
		go func() {
			defer wg.Done()
			testFunc(50, 12)
		}()
	}
	for test := 0; test < 100; test++ {
		go func() {
			defer wg.Done()
			testFunc(7, 1)
		}()
	}
	for test := 0; test < 50; test++ {
		go func() {
			defer wg.Done()
			testFunc(52, 13)
		}()
	}
	for test := 0; test < 50; test++ {
		go func() {
			defer wg.Done()
			testFunc(8, 2)
		}()
	}
	wg.Wait()
}
