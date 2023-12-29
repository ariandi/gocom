package main

import (
	"fmt"
	"strconv"

	"gitlab.axiatadigitallabs.com/adlindo/gocom/pubsub"
)

func main() {

	for i := 0; i < 100; i++ {
		msg := "message " + strconv.Itoa(i)

		fmt.Println(msg)
		pubsub.Get().Publish("test01", msg)
	}

	fmt.Println("Press the Enter Key to stop anytime")
	fmt.Scanln()
}
