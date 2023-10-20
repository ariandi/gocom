package main

import (
	"fmt"

	"github.com/adlindo/gocom"
)

func main() {

	fmt.Println("====>> ADL Common Lib Test <<====")

	//gocom.AddCtrl(GetTestCtrl())
	gocom.AddCtrl(GetKeyValCtrl())
	//gocom.AddCtrl(GetSecretCtrl())
	//gocom.AddCtrl(GetJWTCtrl())
	//gocom.AddCtrl(GetQueueCtrl())
	//gocom.AddCtrl(GetPubSubCtrl())
	//gocom.AddCtrl(GetDistLockCtrl())
	//gocom.AddCtrl(GetDistObjCtrl())

	gocom.Start()
}
