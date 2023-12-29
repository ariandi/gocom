package worker

import (
	"log"
	"time"
)

type DirectWorker struct{}

func (DirectWorker) Submit(task func()) {
	log.Println("Submit task to worker from direct worker")
	task()
}

func (DirectWorker) SubmitAndWait(task func()) {
	log.Println("SubmitAndWait task to worker from direct worker")
	task()
}

func (DirectWorker) SubmitBefore(task func(), dur time.Duration) {
	log.Println("SubmitBefore task to worker from direct worker")
	task()
}
