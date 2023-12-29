package main

import (
	"fmt"

	"gitlab.axiatadigitallabs.com/adlindo/gocom"
	"gitlab.axiatadigitallabs.com/adlindo/gocom/pubsub"
)

func main() {

	fmt.Println("====>> ADL Common Lib Test <<====")

	// gocom.AddCtrl(GetTestCtrl())
	// gocom.AddCtrl(GetKeyValCtrl())
	// gocom.AddCtrl(GetSecretCtrl())
	// gocom.AddCtrl(GetJWTCtrl())
	// gocom.AddCtrl(GetQueueCtrl())
	// gocom.AddCtrl(GetPubSubCtrl())
	// gocom.AddCtrl(GetDistLockCtrl())
	// gocom.AddCtrl(distobj.GetDistObjCtrl())

	pubsub.Get().QueueSubscribe("test01", "testgroup", func(name, msg string) {
		fmt.Println("masuk pak eko : ", msg)

		fmt.Println("------------")
	})

	pubsub.Get().QueueSubscribe("test01", "testgroup", func(name, msg string) {
		fmt.Println("masuk pak jokow : ", msg)

		fmt.Println("------------")
	})

	gocom.Start()
}
