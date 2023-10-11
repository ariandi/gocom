package distobj

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ariandi/gocom/pubsub"
)

type DistObjReqMsg struct {
	Params []interface{}
}

type DistObjResMsg struct {
	IsErr  bool
	ErrMsg string
	Result []interface{}
}

func Invoke(prefix, className, methodName string, params ...interface{}) ([]interface{}, error) {

	req := DistObjReqMsg{
		Params: params,
	}

	targetName := className + ">>" + methodName

	if prefix != "" {
		targetName = prefix + "__" + targetName
	}

	targetName = "distobj::" + targetName

	retStr, err := pubsub.Get().Request(targetName, req, 5*time.Minute)

	if err != nil {
		return nil, err
	}

	ret := DistObjResMsg{}
	err = json.Unmarshal([]byte(retStr), &ret)

	if err != nil {
		fmt.Println("error 111111")
		return nil, err
	}

	fmt.Println("error 222222")

	if ret.IsErr {
		return nil, errors.New(ret.ErrMsg)
	}

	fmt.Println("error 33333", ret.Result)

	return ret.Result, err
}

func AddImpl(prefix, className string, impl interface{}) {

	implType := reflect.TypeOf(impl)

	for i := 0; i < implType.NumMethod(); i++ {

		proxy(prefix, className, impl, implType.Method(i))
	}
}

func proxy(prefix, className string, impl interface{}, method reflect.Method) {

	targetName := className + ">>" + method.Name

	if prefix != "" {
		targetName = prefix + "__" + targetName
	}

	targetName = "distobj::" + targetName

	pubsub.Get().RequestSubscribe(targetName,
		func(name, msg string) string {

			req := DistObjReqMsg{}
			res := DistObjResMsg{}

			err := json.Unmarshal([]byte(msg), &req)

			if err != nil {

				res.IsErr = true
				res.ErrMsg = err.Error()
			} else {

				args := make([]reflect.Value, method.Type.NumIn())
				args[0] = reflect.ValueOf(impl)

				for i, param := range req.Params {

					argType := method.Type.In(i + 1)

					param, err = fixType(param, reflect.TypeOf(param), argType)

					if err != nil {
						res.IsErr = true
						res.ErrMsg = fmt.Sprintf("Param %d : %s", i, err.Error())
						break
					}

					paramType := reflect.TypeOf(param)

					if !paramType.ConvertibleTo(argType) {
						res.IsErr = true
						res.ErrMsg = fmt.Sprintf("Param %d must type %s, got %s", i, argType.Name(), paramType.Name())
						break
					}

					args[i+1] = reflect.ValueOf(param)
				}

				if !res.IsErr {
					retVal := method.Func.Call(args)
					res.Result = []interface{}{}

					for _, elm := range retVal {

						if elm.Interface() != nil {

							res.Result = append(res.Result, elm.Interface())
						} else {
							res.Result = append(res.Result, elm.Interface())
						}

					}
				}
			}

			byteRet, _ := json.Marshal(res)
			return string(byteRet)
		})
}

func fixType(param interface{}, paramType, argType reflect.Type) (interface{}, error) {

	// if got float64 convert to correct type
	if paramType.Kind() == reflect.Float64 {

		if argType.Kind() != reflect.Interface {
			paramReal := param.(float64)

			switch argType.Kind() {
			case reflect.Int:
				param = int(paramReal)
			case reflect.Int8:
				param = int8(paramReal)
			case reflect.Int16:
				param = int16(paramReal)
			case reflect.Int32:
				param = int32(paramReal)
			case reflect.Int64:
				param = int64(paramReal)
			case reflect.Float32:
				param = float32(paramReal)
			case reflect.Float64:
			default:
				return nil, fmt.Errorf("Want %s, got %s", argType.Name(), paramType.Name())
			}
		}
	} else if paramType.Kind() == reflect.Array {

		ret := []interface{}{}
		paramArr := param.([]interface{})

		for _, elm := range paramArr {
			retElm, err := fixType(elm, reflect.TypeOf(elm), paramType.Elem())

			if err != nil {

				return nil, err
			}

			ret = append(ret, retElm)
		}

		param = ret
	} else if paramType.Kind() == reflect.Map {

		ret := map[string]interface{}{}
		paramMap := param.(map[string]interface{})

		for key, elm := range paramMap {
			retElm, err := fixType(elm, reflect.TypeOf(elm), paramType.Elem())

			if err != nil {

				return nil, err
			}

			ret[key] = retElm
		}

		param = ret
	}

	return param, nil
}

func ToStr(val interface{}) string {
	ret, _ := val.(string)
	return ret
}

func ToErr(val interface{}) error {

	fmt.Printf("ERROR ::====>> %v", val)

	valMap := val.(map[string]interface{})

	for a, b := range valMap {

		fmt.Print(a, "===>", b)
	}

	if val == nil {
		return nil
	}

	ret, _ := val.(string)
	return errors.New(ret)
}

func ToBool(val interface{}) bool {
	ret, _ := val.(bool)
	return ret
}

func ToInt(val interface{}) int {
	ret, _ := val.(float64)
	return int(ret)
}

func ToInt16(val interface{}) int16 {
	ret, _ := val.(float64)
	return int16(ret)
}

func ToInt32(val interface{}) int32 {
	ret, _ := val.(float64)
	return int32(ret)
}

func ToInt64(val interface{}) int64 {
	ret, _ := val.(float64)
	return int64(ret)
}

func ToFloat32(val interface{}) float32 {
	ret, _ := val.(float64)
	return float32(ret)
}

func ToFloat64(val interface{}) float64 {
	ret, _ := val.(float64)
	return ret
}

func ToArr(val interface{}, elmType string) interface{} {

	var ret interface{}

	arr, ok := val.([]interface{})

	if !ok {
		return nil
	}

	switch elmType {
	case "interface{}":
		ret = []interface{}{}

		for _, item := range arr {
			ret = append(ret.([]interface{}), item)
		}
	case "string":
		ret = []string{}

		for _, item := range arr {
			ret = append(ret.([]string), ToStr(item))
		}
	case "bool":
		ret = []string{}

		for _, item := range arr {
			ret = append(ret.([]bool), ToBool(item))
		}
	case "int":
		ret = []int{}

		for _, item := range arr {
			ret = append(ret.([]int), ToInt(item))
		}
	case "int16":
		ret = []int16{}

		for _, item := range arr {
			ret = append(ret.([]int16), ToInt16(item))
		}
	case "int32":
		ret = []int32{}

		for _, item := range arr {
			ret = append(ret.([]int32), ToInt32(item))
		}
	case "int64":
		ret = []int64{}

		for _, item := range arr {
			ret = append(ret.([]int64), ToInt64(item))
		}
	case "float32":
		ret = []float32{}

		for _, item := range arr {
			ret = append(ret.([]float32), ToFloat32(item))
		}
	case "float64":
		ret = []float64{}

		for _, item := range arr {
			ret = append(ret.([]float64), ToFloat64(item))
		}
	}

	return ret
}

func ToMap(val interface{}, keyType string, valType string) interface{} {

	var ret interface{}

	mapObj, ok := val.(map[interface{}]interface{})

	if !ok {
		return nil
	}

	switch keyType {
	case "interface{}":
		switch valType {
		case "interface{}":
			ret = map[interface{}]interface{}{}

			for key, item := range mapObj {
				ret.(map[interface{}]interface{})[key] = item
			}
		case "string":
			ret = map[interface{}]string{}

			for key, item := range mapObj {
				ret.(map[interface{}]string)[key] = ToStr(item)
			}
		case "bool":
			ret = map[interface{}]bool{}

			for key, item := range mapObj {
				ret.(map[interface{}]bool)[key] = ToBool(item)
			}
		case "int":
			ret = map[interface{}]int{}

			for key, item := range mapObj {
				ret.(map[interface{}]int)[key] = ToInt(item)
			}
		case "int16":
			ret = map[interface{}]int16{}

			for key, item := range mapObj {
				ret.(map[interface{}]int16)[key] = ToInt16(item)
			}
		case "int32":
			ret = map[interface{}]int32{}

			for key, item := range mapObj {
				ret.(map[interface{}]int32)[key] = ToInt32(item)
			}
		case "int64":
			ret = map[interface{}]int64{}

			for key, item := range mapObj {
				ret.(map[interface{}]int64)[key] = ToInt64(item)
			}
		case "float32":
			ret = map[interface{}]float32{}

			for key, item := range mapObj {
				ret.(map[interface{}]float32)[key] = ToFloat32(item)
			}
		case "float64":
			ret = map[interface{}]float64{}

			for key, item := range mapObj {
				ret.(map[interface{}]float64)[key] = ToFloat64(item)
			}
		}
	case "string":
	case "bool":
	case "int":
	case "int16":
	case "int32":
	case "int64":
	case "float32":
	case "float64":
	}

	return ret
}
