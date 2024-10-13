package concurrency

import (
	"fmt"
	"sync"
)

// BatchRunner 并发批量发送http请求或执行其它任务，并记录err和panic
type BatchRunner struct {
	in []func() error
	wg *sync.WaitGroup

	errorMapLock sync.Mutex
	ErrorMap     map[int]error
}

func NewBatchRunner() *BatchRunner {
	return &BatchRunner{
		in:       []func() error{},
		wg:       &sync.WaitGroup{},
		ErrorMap: make(map[int]error),
	}
}

// Submit 提交批量执行任务
func (r *BatchRunner) Submit(handel func() error) {
	r.in = append(r.in, handel)
}

// Run 以batchNumber为单位,分批并发执行函数
func (r *BatchRunner) Run(batchNumber int) {
	for i, f := range r.in {
		i := i
		// 如果是最后一个函数在执行队列的最后一个，在wg.wait之前执行，避免死锁
		isLast := i == len(r.in)-1

		// 执行函数，会对err和panic进行记录，并且recover，防止进程崩溃
		runFunc := func(fn func() error) {
			defer func() {
				// 处理panic并记录
				if err := recover(); err != nil {
					r.errorMapLock.Lock()
					r.ErrorMap[i] = fmt.Errorf("panic occur:%v", err)
					r.errorMapLock.Unlock()
				}
				r.wg.Done()
			}()
			err := fn()
			if err != nil {
				r.errorMapLock.Lock()
				r.ErrorMap[i] = err
				r.errorMapLock.Unlock()
			}
		}

		if isLast {
			go runFunc(f)
		}
		// 等待并发任务执行结束，并申请新的WaitGroup队列
		if i%batchNumber == 0 || isLast {
			r.wg.Wait()
			if len(r.in)-i < batchNumber {
				r.wg.Add(len(r.in) - i)
			} else {
				r.wg.Add(batchNumber)
			}
		}
		if !isLast {
			go runFunc(f)
		}
	}
}
