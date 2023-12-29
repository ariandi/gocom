package main

import (
	"fmt"

	"gitlab.axiatadigitallabs.com/adlindo/gocom"
	"gitlab.axiatadigitallabs.com/adlindo/gocom/pubsub"
)

func main() {

	fmt.Println("====>> CONSUMER 1 <<====")

	pubsub.Get().Subscribe("test01", func(name, msg string) {
		fmt.Println("Consumer 1 : ", msg)
	})

	gocom.Start()
}
