package main

import (
	"sync"

	"github.com/adlindo/gocom"
)

type SecretCtrl struct {
}

func (o *SecretCtrl) Init() {

	gocom.GET("/secret", o.getSecret)
}

func (o *SecretCtrl) getSecret(ctx gocom.Context) error {

	val, err := gocom.GetSecret("app.jwt.publickey")

	if err == nil {

		return ctx.SendResult(val)
	}

	return ctx.SendError(100, "Error get secret", err)
}

//-------------------------------------------------------

var secretCtrl *SecretCtrl
var secretCtrlOnce sync.Once

func GetSecretCtrl() *SecretCtrl {

	secretCtrlOnce.Do(func() {

		secretCtrl = &SecretCtrl{}
	})

	return secretCtrl
}
