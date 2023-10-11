package main

import "github.com/ariandi/gocom/distobj"

type TestProxy struct {
	className string
	prefix    string
}

var __TestProxyMap map[string]*TestProxy = map[string]*TestProxy{}

func GetTestProxy(prefix ...string) *TestProxy {

	targetPrefix := ""
	if len(prefix) > 0 {
		targetPrefix = prefix[0]
	}

	ret, ok := __TestProxyMap[targetPrefix]

	if !ok {
		ret = &TestProxy{}
		ret.className = "Test"
		ret.prefix = targetPrefix

		__TestProxyMap[targetPrefix] = ret
	}

	return ret
}

func (o *TestProxy) TestString(a string, b string) string {

	ret, err := distobj.Invoke(o.prefix, o.className, "TestString", a, b)

	if err != nil {
		return ""
	}

	ret0 := distobj.ToStr(ret[0])

	return ret0
}

func (o *TestProxy) TestInt(a int, b int) int {

	ret, err := distobj.Invoke(o.prefix, o.className, "TestInt", a, b)

	if err != nil {
		return 0
	}

	ret0 := distobj.ToInt(ret[0])

	return ret0
}

func (o *TestProxy) TestError() error {

	ret, err := distobj.Invoke(o.prefix, o.className, "TestError")

	if err != nil {
		return err
	}

	ret0 := distobj.ToErr(ret[0])

	return ret0
}

func (o *TestProxy) TestList() []string {

	ret, err := distobj.Invoke(o.prefix, o.className, "TestList")

	if err != nil {
		return nil
	}

	ret0 := distobj.ToArr(ret[0], "string").([]string)

	return ret0
}

func (o *TestProxy) TestIntList() []int {

	ret, err := distobj.Invoke(o.prefix, o.className, "TestIntList")

	if err != nil {
		return nil
	}

	ret0 := distobj.ToArr(ret[0], "int").([]int)

	return ret0
}

func (o *TestProxy) TestInterface(a interface{}) interface{} {

	ret, err := distobj.Invoke(o.prefix, o.className, "TestInterface", a)

	if err != nil {
		return nil
	}

	ret0 := ret[0]

	return ret0
}

func (o *TestProxy) TestInterfaceList(a []interface{}) []interface{} {

	ret, err := distobj.Invoke(o.prefix, o.className, "TestInterfaceList", a)

	if err != nil {
		return nil
	}

	ret0 := distobj.ToArr(ret[0], "interface{}").([]interface{})

	return ret0
}

func (o *TestProxy) TestMap() map[string]string {

	ret, err := distobj.Invoke(o.prefix, o.className, "TestMap")

	if err != nil {
		return nil
	}

	ret0 := distobj.ToMap(ret[0], "string", "string").(map[string]string)

	return ret0
}
