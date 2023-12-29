package distobj

import (
	"sync"

	"gitlab.axiatadigitallabs.com/adlindo/gocom"
	"gitlab.axiatadigitallabs.com/adlindo/gocom/distobj"
)

type DistObjCtrl struct {
}

func (o *DistObjCtrl) Init() {

	gocom.POST("/distobj/string", o.tryString)
	gocom.POST("/distobj/int", o.tryInt)
	gocom.POST("/distobj/bool", o.tryBool)
	gocom.POST("/distobj/error", o.tryError)
	gocom.POST("/distobj/array", o.tryArray)
	gocom.POST("/distobj/map", o.tryMap)
}

var distObjCtrl *DistObjCtrl
var distObjOnce sync.Once
var obj TestDist

func GetDistObjCtrl() *DistObjCtrl {

	distObjOnce.Do(func() {

		distObjCtrl = &DistObjCtrl{}
		obj = GetTestDistProxy()

		distobj.Register((*TestDist)(nil), &TestDistImpl{})
	})

	return distObjCtrl
}

type TestPayload struct {
	StrA  string
	StrB  string
	IntA  int
	IntB  int
	BoolA bool
	BoolB bool
}

func (o *DistObjCtrl) tryString(ctx gocom.Context) error {

	payload := TestPayload{}
	err := ctx.Bind(&payload)

	if err != nil {
		return ctx.SendError(gocom.NewError(1, err.Error()))
	}

	ret := obj.TestString(payload.StrA, payload.StrB)
	return ctx.SendResult(ret)
}

func (o *DistObjCtrl) tryInt(ctx gocom.Context) error {

	payload := TestPayload{}
	err := ctx.Bind(&payload)

	if err != nil {
		return ctx.SendError(gocom.NewError(1, err.Error()))
	}

	ret := GetTestDistProxy().TestInt(payload.IntA, payload.IntB)

	return ctx.SendResult(ret)
}

func (o *DistObjCtrl) tryBool(ctx gocom.Context) error {

	payload := TestPayload{}
	err := ctx.Bind(&payload)

	if err != nil {
		return ctx.SendError(gocom.NewError(1, err.Error()))
	}

	ret := GetTestDistProxy().TestBool(payload.BoolA, payload.BoolB)

	return ctx.SendResult(ret)
}

func (o *DistObjCtrl) tryError(ctx gocom.Context) error {

	payload := TestPayload{}
	err := ctx.Bind(&payload)

	if err != nil {
		return ctx.SendError(gocom.NewError(1, err.Error()))
	}

	ret, err := GetTestDistProxy().TestError(payload.IntA, payload.IntB)

	if err != nil {
		return ctx.SendError(gocom.NewError(1, err.Error()))
	}

	return ctx.SendResult(ret)
}

func (o *DistObjCtrl) tryArray(ctx gocom.Context) error {

	payload := TestPayload{}
	err := ctx.Bind(&payload)

	if err != nil {
		return ctx.SendError(gocom.NewError(1, err.Error()))
	}

	ret := GetTestDistProxy().TestArray(payload.IntA, payload.IntB)
	return ctx.SendResult(ret)
}

func (o *DistObjCtrl) tryMap(ctx gocom.Context) error {

	payload := TestPayload{}
	err := ctx.Bind(&payload)

	if err != nil {
		return ctx.SendError(gocom.NewError(1, err.Error()))
	}

	ret := GetTestDistProxy().TestMap(payload.StrA, payload.StrB)
	return ctx.SendResult(ret)
}
