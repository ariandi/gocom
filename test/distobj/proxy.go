package distobj

import (
	"fmt"

	"github.com/ariandi/gocom/distobj"
)

type TestDistProxy struct {
	option    distobj.Option
	queueName string
}

func GetTestDistProxy(option ...distobj.Option) TestDist {

	var opt distobj.Option

	if len(option) > 0 {
		opt = option[0]
	} else {
		opt = distobj.GetDefaultOption()
	}

	return &TestDistProxy{
		option:    opt,
		queueName: distobj.GetQueueName((*TestDist)(nil), opt),
	}
}

func (o *TestDistProxy) TestString(a, b string) string {

	ret, err := distobj.Invoke(o.queueName, o.option.Config, "TestString", a, b)

	var r1 string

	if err == nil {

		r1 = ret[0].(string)
	}

	return r1
}

func (o *TestDistProxy) TestInt(a, b int) int {

	ret, err := distobj.Invoke(o.queueName, o.option.Config, "TestInt", a, b)

	var r1 int

	if err == nil {

		r1 = ret[0].(int)
	}

	return r1
}

func (o *TestDistProxy) TestBool(a, b bool) bool {

	ret, err := distobj.Invoke(o.queueName, o.option.Config, "TestBool", a, b)

	var r1 bool

	if err == nil {

		r1 = ret[0].(bool)
	}

	return r1
}

func (o *TestDistProxy) TestError(a, b int) (int, error) {

	ret, err := distobj.Invoke(o.queueName, o.option.Config, "TestError", a, b)

	var r1 int
	var r2 error

	if err == nil {

		r1 = ret[0].(int)

		if ret[1] != nil {
			fmt.Println("masuk errorrrrr ==>", ret[1])
			r2 = ret[1].(error)
		}
	}

	fmt.Println("balikan : ", r2)

	return r1, r2
}

func (o *TestDistProxy) TestArray(a, b int) []int {

	ret, err := distobj.Invoke(o.queueName, o.option.Config, "TestArray", a, b)

	var r1 []int

	if err == nil {

		if ret[0] != nil {
			r1, _ = ret[0].([]int)
		}
	}

	return r1
}

func (o *TestDistProxy) TestMap(a, b string) map[string]string {

	ret, err := distobj.Invoke(o.queueName, o.option.Config, "TestMap", a, b)

	var r1 map[string]string

	if err == nil {

		if ret[0] != nil {
			r1, _ = ret[0].(map[string]string)
		}
	}

	return r1
}
