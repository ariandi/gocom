package main

import (
	"sync"

	"github.com/ariandi/gocom"
	"github.com/ariandi/gocom/secret"
)

type SecretCtrl struct {
}

func (o *SecretCtrl) Init() {

	gocom.GET("/secret", o.getSecret)
}

func (o *SecretCtrl) getSecret(ctx gocom.Context) error {

	val, err := secret.Get("app.jwt.publickey")

	if err == nil {

		return ctx.SendResult(val)
	}

	return ctx.SendError(gocom.NewError(100, "Error get secret"+err.Error()))
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
