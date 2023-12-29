package worker

import (
	pond "github.com/alitto/pond"
	"log"
	"time"
)

type Pond struct {
	workerPool *pond.WorkerPool
}

func NewPond(max int, poolSize int, workers ...pond.Option) *Pond {
	worker := pond.New(max, poolSize, workers...)
	return &Pond{
		workerPool: worker,
	}
}

func (p *Pond) Submit(task func()) {
	log.Println("Submit task to worker from pond")
	p.workerPool.Submit(task)
}

func (p *Pond) SubmitAndWait(task func()) {
	log.Println("SubmitAndWait task to worker from pond")
	p.workerPool.SubmitAndWait(task)
}

func (p *Pond) SubmitBefore(task func(), dur time.Duration) {
	log.Println("SubmitBefore task to worker from pond")
	p.workerPool.SubmitBefore(task, dur)
}
