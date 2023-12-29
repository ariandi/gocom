package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ariandi/gocom"
)

type PubSubCtrl struct {
}

func (o *PubSubCtrl) Init() {

	gocom.POST("/pubsub/request", o.postRequest)
}

func (o *PubSubCtrl) postRequest(ctx gocom.Context) error {

	pubsub := gocom.PubSub()
	data := &TestDTO{}
	ctx.Bind(&data)

	if pubsub != nil {

		ret, err := pubsub.Request("func_helo", data)

		if err == nil {
			return ctx.SendResult(ret)
		}

		return ctx.SendError(gocom.NewError(101, "Add list error : "+err.Error()))
	} else {
		return ctx.SendError(gocom.NewError(101, "invalid KeyVal conn"))
	}
}

var pubSubCtrl *PubSubCtrl
var pubSubCtrlOnce sync.Once

func GetPubSubCtrl() *PubSubCtrl {

	pubSubCtrlOnce.Do(func() {

		pubSubCtrl = &PubSubCtrl{}

		gocom.PubSub().RequestSubscribe("func_helo", func(name, msg string) string {

			data := TestDTO{}
			json.Unmarshal([]byte(msg), &data)
			fmt.Println("Request : ", data)
			return "Hello " + data.DataString
		})
	})

	return pubSubCtrl
}
