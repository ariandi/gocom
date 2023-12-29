package distobj

import (
	"errors"
	"fmt"
)

type TestDistImpl struct {
}

func (o *TestDistImpl) TestString(a, b string) string {

	fmt.Println("masuk TestString ", a, b)
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
		fmt.Println("error balikin eror TestError")
		return 0, errors.New("can not same number")
	}

	fmt.Println("return TestError", a-b)
	return a - b, nil
}

func (o *TestDistImpl) TestArray(a, b int) []int {
	fmt.Println("masuk TestArray ", a, b)

	return []int{a, b}
}

func (o *TestDistImpl) TestMap(a, b string) map[string]string {

	ret := map[string]string{}
	ret[a] = b
	ret[b] = a

	return ret
}
