package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"gitlab.axiatadigitallabs.com/adlindo/gocom"
)

type QueueCtrl struct {
}

func (o *QueueCtrl) Init() {

	gocom.POST("/queue", o.postRequest)
}

func (o *QueueCtrl) postRequest(ctx gocom.Context) error {

	queue := gocom.Queue()
	data := &TestDTO{}
	ctx.Bind(&data)

	if queue != nil {

		err := queue.Publish("queue_test", data)

		if err != nil {
			return ctx.SendError(gocom.NewError(101, "Push error : "+err.Error()))
		}

		return ctx.SendResult("OK")
	} else {
		return ctx.SendError(gocom.NewError(101, "invalid queue conn"))
	}
}

var queueCtrl *QueueCtrl
var queueCtrlOnce sync.Once

func GetQueueCtrl() *QueueCtrl {

	queueCtrlOnce.Do(func() {

		queueCtrl = &QueueCtrl{}

		gocom.Queue().Consume("queue_test", func(name, msg string) {

			data := TestDTO{}
			json.Unmarshal([]byte(msg), &data)
			fmt.Println("Consume : ", data)
		})
	})

	return queueCtrl
}
