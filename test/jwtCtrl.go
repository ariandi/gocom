package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/ariandi/gocom"
)

type JWTCtrl struct {
}

func (o *JWTCtrl) Init() {

	gocom.GET("/jwt", o.getJWT)
	gocom.POST("/jwt", o.postJWT)
}

func (o *JWTCtrl) getJWT(ctx gocom.Context) error {

	dataMap := map[string]interface{}{}
	dataMap["satu"] = "satu"
	dataMap["dua"] = 22
	dataMap["tiga"] = true

	val, err := gocom.NewJWT(dataMap, time.Minute)

	if err == nil {

		return ctx.SendResult(val)
	}

	return ctx.SendError(gocom.NewError(100, "Error get JWT :"+err.Error()))
}

func (o *JWTCtrl) postJWT(ctx gocom.Context) error {

	dto := &TestDTO{}
	ctx.Bind(dto)
	fmt.Println("====>", dto)

	val, err := gocom.ValidateJWT(dto.DataString)

	if err == nil {

		return ctx.SendResult("VALID : " + val["satu"].(string))
	}

	return ctx.SendError(gocom.NewError(100, "Invalid JWT "+err.Error()))
}

//-------------------------------------------------------

var jwtCtrl *JWTCtrl
var jwtCtrlOnce sync.Once

func GetJWTCtrl() *JWTCtrl {

	jwtCtrlOnce.Do(func() {

		jwtCtrl = &JWTCtrl{}
	})

	return jwtCtrl
}
