package gocom

import (
	"fmt"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"io/ioutil"
	"mime/multipart"
	"os"
	"strconv"

	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/ariandi/gocom/config"
	"github.com/gofiber/fiber/v2"
)

// FiberContext -------------------------------------------

type FiberContext struct {
	ctx *fiber.Ctx
}

func (o *FiberContext) Status(code int) Context {

	o.ctx.Status(code)
	return o
}

func (o *FiberContext) GetFullURL() string {

	ctx := o.ctx

	protocol := "http"
	if ctx.Secure() {
		protocol = "https"
	}

	host := string(ctx.Request().Host())

	path := ctx.OriginalURL()

	return fmt.Sprintf("%s://%s%s", protocol, host, path)
}

func (o *FiberContext) GetRequestHeaders() map[string]string {
	headers := make(map[string]string)
	o.ctx.Request().Header.VisitAll(func(key, value []byte) {
		headers[string(key)] = string(value)
	})
	return headers
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

func (o *FiberContext) FormFile(key string) (*multipart.FileHeader, error) {
	return o.ctx.FormFile(key)
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

	o.ctx.Request().Header.Set(key, value)
}

func (o *FiberContext) Get(key string) string {

	return o.ctx.Get(key)
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

	prometheus := fiberprometheus.New("service")
	prometheus.RegisterAt(o.app, "/metrics")
	o.app.Use(prometheus.Middleware)

	o.app.Listen(totalAddr)
}

func init() {

	RegAppCreator("fiber", func() App {
		ret := &FiberApp{}
		ret.app = fiber.New()

		ret.app.Use(cors.New(cors.Config{
			AllowHeaders:     "*",
			AllowOrigins:     "*",
			AllowCredentials: true,
			AllowMethods:     "GET,POST,HEAD,PUT,DELETE,PATCH,OPTIONS",
		}))

		return ret
	})
}
