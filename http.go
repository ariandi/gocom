package gocom

import (
	"sync"

	"github.com/adlindo/gocom/config"
)

// ----------------------------------------

var controllerList []Controller = []Controller{}
var app App
var appOnce sync.Once
var appCreatorMap map[string]HttpAppCreatorFunc = map[string]HttpAppCreatorFunc{}

type HttpAppCreatorFunc func() App

func RegAppCreator(name string, creator HttpAppCreatorFunc) {
	appCreatorMap[name] = creator
}

// ---------------------------------------

type App interface {
	Get(path string, handlers ...HandlerFunc)
	Post(path string, handlers ...HandlerFunc)
	Put(path string, handlers ...HandlerFunc)
	Patch(path string, handlers ...HandlerFunc)
	Delete(path string, handlers ...HandlerFunc)
	Start()
}

type Context interface {
	Status(code int) Context
	Body() []byte
	Param(key string, defaulVal ...string) string
	Query(key string, defaulVal ...string) string
	FormValue(key string, defaulVal ...string) string
	Bind(target interface{}) error
	SetHeader(key string, value string)
	GetHeader(key string) string
	Set(key string, value string)
	Get(key string) string
	SendString(data string) error
	SendJSON(data interface{}) error
	SendPaged(data interface{}, currPage, totalPage int) error
	SendFile(filePath string, fileName string) error
	SendFileBytes(data []byte, fileName string) error
	SendResult(data interface{}) error
	SendError(err *CodedError) error
	Next() error
	InvokeNativeCtx(handlerFunc interface{}) error
}

type Result struct {
	Code     int         `json:"code"`
	Messages string      `json:"message"`
	Data     interface{} `json:"data"`
}

type ResultPaged struct {
	Result
	CurrPage  int `json:"currPage"`
	TotalPage int `json:"totalPage"`
}

type HandlerFunc func(ctx Context) error

type Controller interface {
	Init()
}

// Funcs ----------------------------------------

func init() {
	config.SetDefault("app.http.port", 9494)
	config.SetDefault("app.http.address", "")
}

func getApp() App {

	appOnce.Do(func() {

		appType := config.Get("app.http.type", "fiber")

		creator := appCreatorMap[appType]

		if creator != nil {
			app = creator()
		}
	})

	return app
}

func Start() {

	for _, ctrl := range controllerList {

		ctrl.Init()
	}

	getApp().Start()
}

func cors(ctx Context) error {

	var headerList map[string]string = map[string]string{"X-Content-Type-Options": "nosniff",
		"X-Xss-Protection":                  "1; mode=block",
		"Strict-Transport-Security":         "max-age=31536000; includeSubdomains; preload",
		"Cache-Control":                     "no-cache",
		"Referrer-Policy":                   "same-origin",
		"X-Frame-Options":                   "SAMEORIGIN",
		"Access-Control-Allow-Methods":      "DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT",
		"X-Permitted-Cross-Domain-Policies": "none",
		"Permissions-Policy":                "geolocation=(self)",
		"Expect-Ct":                         "max-age=31536000, report-uri='https://1106b534557f22696ba5ea23646882e4.report-uri.com/r/d/ct/reportOnly'",
		"Access-Control-Allow-Headers":      "Authorization,*",
		"Access-Control-Allow-Origin":       "*",
		"Content-Security-Policy":           "default-src 'self' 'unsafe-inline' 'unsafe-eval' https://fonts.googleapis.com;font-src 'self' https://fonts.gstatic.com;img-src ",
	}

	for name, val := range headerList {
		ctx.SetHeader(name, val)
	}

	return ctx.Next()
}

func GET(path string, handlers ...HandlerFunc) {

	handlers = append([]HandlerFunc{cors}, handlers...)
	getApp().Get(path, handlers...)
}

func POST(path string, handlers ...HandlerFunc) {

	handlers = append([]HandlerFunc{cors}, handlers...)
	getApp().Post(path, handlers...)
}

func PUT(path string, handlers ...HandlerFunc) {

	handlers = append([]HandlerFunc{cors}, handlers...)
	getApp().Put(path, handlers...)
}

func PATCH(path string, handlers ...HandlerFunc) {

	handlers = append([]HandlerFunc{cors}, handlers...)
	getApp().Patch(path, handlers...)
}

func DELETE(path string, handlers ...HandlerFunc) {

	handlers = append([]HandlerFunc{cors}, handlers...)
	getApp().Delete(path, handlers...)
}

func AddCtrl(ctrl Controller) {

	controllerList = append(controllerList, ctrl)
}
