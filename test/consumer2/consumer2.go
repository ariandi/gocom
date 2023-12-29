package main

import (
	"fmt"

	"github.com/ariandi/gocom"
	"github.com/ariandi/gocom/pubsub"
)

func main() {

	fmt.Println("====>> CONSUMER 2<<====")

	pubsub.Get().Subscribe("test01", func(name, msg string) {
		fmt.Println("Consumer 2 : ", msg)
	})

	gocom.Start()
}
