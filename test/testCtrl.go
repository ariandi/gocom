package main

import (
	"fmt"
	"sync"

	"github.com/ariandi/gocom"
	"github.com/jinzhu/copier"
)

type TestCtrl struct {
}

func (o *TestCtrl) Init() {

	gocom.GET("/test/hello", o.getTestHello)

	gocom.GET("/test", o.TestMultiHandler, o.TestGet)
	gocom.GET("/test/:id", o.TestGetOne)
	gocom.POST("/test", o.TestPost)
	gocom.PUT("/test/:id", o.TestPut)
	gocom.PATCH("/test/:id", o.TestPut)
}

func (o *TestCtrl) getTestHello(ctx gocom.Context) error {

	return ctx.SendResult("Hello World !")
}

func (o *TestCtrl) TestGet(ctx gocom.Context) error {

	ctx.Set("aa", "111")
	fmt.Println("====>", ctx.Get("aa"))
	// lock := gocom.GetLock("Test", 0, 20*time.Second)

	// if lock != nil {
	// 	fmt.Println("Berhasil dapat lock")
	// } else {
	// 	fmt.Println("Gek Berhasil dapat lock")
	// }

	ret := GetTestRepo().GetAll()

	dtoRet := []TestDTO{}
	copier.Copy(&dtoRet, &ret)

	fmt.Println("Dalam TestGet")

	return ctx.SendResult(dtoRet)
}

func (o *TestCtrl) TestMultiHandler(ctx gocom.Context) error {

	fmt.Println("Dalam TestGet 2")

	return ctx.Next()
}

func (o *TestCtrl) TestGetOne(ctx gocom.Context) error {

	id := ctx.Param("id")

	ret := GetTestRepo().GetOne(id)

	if ret != nil {
		dtoRet := TestDTO{}
		copier.Copy(&dtoRet, &ret)

		return ctx.SendResult(dtoRet)
	}

	return ctx.SendError(gocom.NewError(1001, "Data not found"))
}

func (o *TestCtrl) TestPost(ctx gocom.Context) error {

	fmt.Println("Dalam TestPost")

	dto := &TestDTO{}
	ctx.Bind(dto)
	fmt.Println("====>", dto)

	mdl := &Test{}
	copier.Copy(mdl, dto)

	GetTestRepo().Create(mdl)

	fmt.Println(mdl)

	copier.Copy(dto, mdl)

	return ctx.SendResult(dto)
}

func (o *TestCtrl) TestPut(ctx gocom.Context) error {

	fmt.Println("Dalam TestPut")

	dto := &TestDTO{}
	ctx.Bind(dto)
	fmt.Println("====>", dto)

	mdl := &Test{}
	copier.Copy(mdl, dto)
	mdl.ID = ctx.Param("id")

	GetTestRepo().Update(mdl)

	fmt.Println(mdl)

	copier.Copy(dto, mdl)

	return ctx.SendResult(dto)
}

//-----------------------------------------------

var testCtrl *TestCtrl
var testCtrlOnce sync.Once

func GetTestCtrl() *TestCtrl {

	testCtrlOnce.Do(func() {

		testCtrl = &TestCtrl{}
	})

	return testCtrl
}
