package gocom

import (
	"io/ioutil"
	"os"
	"strconv"

	"github.com/adlindo/gocom/config"
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"
)

// FiberContext -------------------------------------------

type FiberContext struct {
	ctx  *fiber.Ctx
	data map[string]string
}

func (o *FiberContext) Status(code int) Context {

	o.ctx.Status(code)
	return o
}

func (o *FiberContext) Body() []byte {

	return o.ctx.Body()
}

func (o *FiberContext) Param(key string, defaultVal ...string) string {

	return o.ctx.Params(key, defaultVal...)
}

func (o *FiberContext) Query(key string, defaultVal ...string) string {

	return o.ctx.Query(key, defaultVal...)
}

func (o *FiberContext) FormValue(key string, defaultVal ...string) string {

	return o.ctx.FormValue(key, defaultVal...)
}

func (o *FiberContext) Bind(target interface{}) error {

	return o.ctx.BodyParser(target)
}

func (o *FiberContext) SetHeader(key, value string) {

	o.ctx.Set(key, value)
}

func (o *FiberContext) GetHeader(key string) string {

	return o.ctx.Get(key)
}

func (o *FiberContext) Set(key string, value string) {

	if o.data == nil {
		o.data = map[string]string{}
	}

	o.data[key] = value
}

func (o *FiberContext) Get(key string) string {

	if o.data == nil {
		o.data = map[string]string{}
	}

	return o.data[key]
}

func (o *FiberContext) SendString(data string) error {

	return o.ctx.SendString(data)
}

func (o *FiberContext) SendResult(data interface{}) error {

	return o.ctx.JSON(&Result{Code: 0, Messages: "Success", Data: data})
}

func (o *FiberContext) SendPaged(data interface{}, currPage, totalPage int) error {

	return o.ctx.JSON(&ResultPaged{Result: Result{Code: 0, Messages: "Success", Data: data},
		CurrPage:  currPage,
		TotalPage: totalPage})
}

func (o *FiberContext) SendError(err *CodedError) error {

	return o.ctx.Status(fiber.StatusBadRequest).JSON(&Result{Code: err.Code, Messages: err.Message})
}

func (o *FiberContext) SendJSON(data interface{}) error {

	return o.ctx.JSON(data)
}

func (o *FiberContext) SendFile(filePath string, fileName string) error {

	return o.ctx.SendFile(filePath)
}

func (o *FiberContext) SendFileBytes(data []byte, fileName string) error {

	file, err := ioutil.TempFile("dir", "sendFile*_"+fileName)

	if err == nil {
		defer os.Remove(file.Name())

		o.ctx.SendFile(file.Name())
	}

	return err
}

func (o *FiberContext) Next() error {

	return o.ctx.Next()
}

func (o *FiberContext) InvokeNativeCtx(handlerFunc interface{}) error {
	fiberHandler, okHandler := handlerFunc.(fiber.Handler)
	if okHandler {
		return fiberHandler(o.ctx)
	}
	return nil
}

// FiberApp -----------------------------------------------

type FiberApp struct {
	app *fiber.App
}

func toFiberHandler(handler HandlerFunc) fiber.Handler {

	return func(ctx *fiber.Ctx) error {

		return handler(&FiberContext{ctx: ctx})
	}
}

func toFiberHandlers(handlers []HandlerFunc) []fiber.Handler {

	ret := []fiber.Handler{}

	for _, handler := range handlers {

		ret = append(ret, toFiberHandler(handler))
	}

	return ret
}

func (o *FiberApp) Get(path string, handlers ...HandlerFunc) {

	o.app.Get(path, toFiberHandlers(handlers)...)
}

func (o *FiberApp) Post(path string, handlers ...HandlerFunc) {

	o.app.Post(path, toFiberHandlers(handlers)...)
}

func (o *FiberApp) Put(path string, handlers ...HandlerFunc) {

	o.app.Put(path, toFiberHandlers(handlers)...)
}

func (o *FiberApp) Patch(path string, handlers ...HandlerFunc) {

	o.app.Patch(path, toFiberHandlers(handlers)...)
}

func (o *FiberApp) Delete(path string, handlers ...HandlerFunc) {

	o.app.Delete(path, toFiberHandlers(handlers)...)
}

func (o *FiberApp) Start() {

	addr := config.Get("app.http.address")
	port := config.GetInt("app.http.port")

	totalAddr := addr + ":" + strconv.Itoa(port)

	// o.app.Use(cors.New())

	prometheus := fiberprometheus.New("service")
	prometheus.RegisterAt(o.app, "/metrics")
	o.app.Use(prometheus.Middleware)

	o.app.Use(func(ctx *fiber.Ctx) error {

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
			ctx.Append(name, val)
		}

		return ctx.Next()
	})

	o.app.Listen(totalAddr)
}

func init() {

	RegAppCreator("fiber", func() App {
		ret := &FiberApp{}
		ret.app = fiber.New()

		return ret
	})
}
