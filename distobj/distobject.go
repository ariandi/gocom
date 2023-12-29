package distobj

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ariandi/gocom/queue"
)

type Request struct {
	MethodName string
	Params     []interface{}
}

type Response struct {
	Results []interface{}
}

type StringError struct {
	Msg string
}

type Option struct {
	Config    string
	NameSpace string
	NumWorker int
}

func GetDefaultOption() Option {

	ret := Option{
		Config:    "default",
		NameSpace: "package",
		NumWorker: 2,
	}

	return ret
}

var errType reflect.Type = reflect.TypeOf(errors.New(""))

func mergeOption(target *Option, src Option) {

	if src.Config != "" {
		target.Config = src.NameSpace
	}

	if src.NameSpace != "" {
		target.NameSpace = src.NameSpace
	}
}

func Register(iface interface{}, obj interface{}, options ...Option) error {

	ifaceType := reflect.TypeOf(iface)

	if ifaceType == nil {
		return errors.New("unable to get interface type")
	}

	implType := reflect.TypeOf(obj)

	opt := GetDefaultOption()

	if len(options) > 0 {
		mergeOption(&opt, options[0])
	}

	for i := 0; i < ifaceType.Elem().NumMethod(); i++ {

		mtd, found := implType.MethodByName(ifaceType.Elem().Method(i).Name)

		if found {
			proxy(iface, obj, opt, mtd)
		}
	}

	return nil
}

func proxy(iface interface{}, obj interface{}, opt Option, method reflect.Method) {

	for i := 0; i < opt.NumWorker; i++ {

		queue.Get(opt.Config).ReplyRaw(GetQueueName(iface, opt)+":"+method.Name, handleFunc(i, obj, method))
	}
}

func handleFunc(workerNo int, obj interface{}, method reflect.Method) queue.QueueRawReqHandler {

	return func(name string, msg []byte) []byte {

		// decode request
		inBuf := bytes.NewBuffer(msg)
		dec := gob.NewDecoder(inBuf)

		req := Request{}
		err := dec.Decode(&req)

		if err != nil {
			fmt.Println("Unable to parse request :" + err.Error())
			return response(errors.New("Unable to parse request :" + err.Error()))
		}

		args := make([]reflect.Value, method.Type.NumIn())
		args[0] = reflect.ValueOf(obj)

		for i, param := range req.Params {
			args[i+1] = reflect.ValueOf(param)
		}

		retVal := method.Func.Call(args)
		result := []interface{}{}

		for _, elm := range retVal {

			if elm.Interface() != nil && elm.Type().Name() == "error" {
				result = append(result, StringError{Msg: elm.Interface().(error).Error()})
			} else {
				result = append(result, elm.Interface())
			}
		}

		return response(nil, result...)
	}
}

func response(err error, resList ...interface{}) []byte {

	res := Response{
		Results: resList,
	}

	// encode response
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	inErr := enc.Encode(res)
	if inErr != nil {
		fmt.Println("===>>> ERROR WHEN SERIALIZING response :", err)
	}

	return buf.Bytes()
}

func Invoke(path string, config string, methodName string, params ...interface{}) ([]interface{}, error) {

	req := Request{
		MethodName: methodName,
		Params:     params,
	}

	// encode
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	err := enc.Encode(req)
	if err != nil {
		fmt.Println("===>>> ERROR WHEN SERIALIZING Request :", err)
		return nil, errors.New("Unable to serialize request :" + err.Error())
	}

	ret, err := queue.Get(config).RequestRaw(path+":"+methodName, buf.Bytes())

	if err != nil {
		fmt.Println("Unable call queue request :" + err.Error())
		return nil, errors.New("Unable call queue request :" + err.Error())
	}

	// decode
	inBuf := bytes.NewBuffer(ret)
	dec := gob.NewDecoder(inBuf)

	res := Response{}
	err = dec.Decode(&res)

	if err != nil {
		fmt.Println("Unable to parse result :" + err.Error())
		return nil, errors.New("Unable to parse result :" + err.Error())
	}

	retList := []interface{}{}

	for _, val := range res.Results {

		valx, ok := val.(StringError)

		if ok {
			retList = append(retList, errors.New(valx.Msg))
		} else {
			retList = append(retList, val)
		}
	}
	return retList, nil
}

func GetQueueName(iface interface{}, option Option) string {

	ifaceType := reflect.TypeOf(iface)

	if ifaceType == nil {
		return ""
	}

	if option.NameSpace == "" || option.NameSpace == "package" {
		return "distobj/" + ifaceType.Elem().PkgPath() + "/" + ifaceType.Elem().Name()
	} else if option.NameSpace == "global" {
		return "distobj/" + ifaceType.Elem().Name()
	}

	return "distobj/" + option.NameSpace + "/" + ifaceType.Elem().Name()
}

func init() {

	gob.Register(time.Time{})
	gob.Register(errors.New(""))
	gob.Register(Request{})
	gob.Register(Response{})
	gob.Register(StringError{})
	gob.Register(map[string]string{})
	gob.Register(map[string]int{})
	gob.Register(map[string]int32{})
	gob.Register(map[string]int64{})
	gob.Register(map[string]float32{})
	gob.Register(map[string]float64{})
}
