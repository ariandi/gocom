package main

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ariandi/gocom"
	"github.com/ariandi/gocom/distobj"
)

type TestDist interface {
	TestString(a, b string) string
	TestInt(a, b int) int
	TestBool(a, b bool) bool
	TestError(a, b int) (int, error)
}

// PROXY ----------------------------------------------------------------------

type TestDistProxy struct {
	className string
	prefix    string
}

var __TestDistProxyMap map[string]*TestDistProxy = map[string]*TestDistProxy{}

func GetTestDistProxy(prefix ...string) TestDist {

	targetPrefix := ""
	if len(prefix) > 0 {
		targetPrefix = prefix[0]
	}

	ret, ok := __TestDistProxyMap[targetPrefix]

	if !ok {
		ret = &TestDistProxy{}
		ret.className = "TestDist"
		ret.prefix = targetPrefix

		__TestDistProxyMap[targetPrefix] = ret
	}

	return ret
}

func (o *TestDistProxy) TestString(a, b string) string {

	retList, err := distobj.Invoke(o.prefix, o.className, "TestString", a, b)

	if err != nil {
		return ""
	}

	ret0 := distobj.ToStr(retList[0])
	return ret0
}

func (o *TestDistProxy) TestInt(a, b int) int {

	retList, err := distobj.Invoke(o.prefix, o.className, "TestInt", a, b)

	if err != nil {
		fmt.Println("error TestInt : ", err)
		return 0
	}

	fmt.Println("mau masuk : ", retList[0])
	ret0 := distobj.ToInt(retList[0])
	return ret0
}

func (o *TestDistProxy) TestBool(a, b bool) bool {

	retList, err := distobj.Invoke(o.prefix, o.className, "TestBool", a, b)

	if err != nil {
		fmt.Println("error TestBool : ", err)
		return false
	}

	fmt.Println("mau masuk : ", retList[0])
	ret0 := distobj.ToBool(retList[0])
	return ret0
}

func (o *TestDistProxy) TestError(a, b int) (int, error) {

	retList, err := distobj.Invoke(o.prefix, o.className, "TestError", a, b)

	if err != nil {
		fmt.Println("error TestError : ", err)
		return 0, err
	}

	ret0 := distobj.ToInt(retList[0])
	ret1 := distobj.ToErr(retList[1])

	return ret0, ret1
}

// IMPL ----------------------------------------------------------------------

type TestDistImpl struct {
}

func (o *TestDistImpl) TestString(a, b string) string {

	return "Merge : " + a + "=>" + b
}

func (o *TestDistImpl) TestInt(a, b int) int {

	fmt.Println("masuk testint ", a, b)
	return a + b
}

func (o *TestDistImpl) TestBool(a, b bool) bool {

	fmt.Println("masuk testbool ", a, b)
	return a || b
}

func (o *TestDistImpl) TestError(a, b int) (int, error) {

	fmt.Println("masuk testerror ", a, b)

	if a == b {
		fmt.Println("error TestError")
		return 0, errors.New("can not same number")
	}

	fmt.Println("return TestError", a-b)
	return a - b, nil
}

//-------------------------------------------------------

type DistObjCtrl struct {
}

func (o *DistObjCtrl) Init() {

	gocom.POST("/distobj/string", o.tryString)
	gocom.POST("/distobj/int", o.tryInt)
	gocom.POST("/distobj/bool", o.tryBool)
	gocom.POST("/distobj/error", o.tryError)
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

	ret := GetTestDistProxy().TestString(payload.StrA, payload.StrB)
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

var distObjCtrl *DistObjCtrl
var distObjOnce sync.Once

func GetDistObjCtrl() *DistObjCtrl {

	distObjOnce.Do(func() {

		distObjCtrl = &DistObjCtrl{}

		distobj.AddImpl("", "TestDist", &TestDistImpl{})
	})

	return distObjCtrl
}
