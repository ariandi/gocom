package secret

import (
	"fmt"

	"gitlab.axiatadigitallabs.com/adlindo/gocom/config"
)

var secretFuncMap map[string]SecretFunc = map[string]SecretFunc{}

type SecretFunc func(dsn string, key string) (string, error)

func RegSecretFunc(typeName string, secFunc SecretFunc) {

	secretFuncMap[typeName] = secFunc
}

func Get(name string) (string, error) {

	ret := ""

	theFunc, ok := secretFuncMap[config.Get("app.secret.type")]

	if ok {
		var err error
		ret, err = theFunc(config.Get("app.secret.dsn"), name)

		if err != nil {
			fmt.Println("no secret found for name ", name)

			ret = config.Get(name)
		}

		return ret, nil
	}

	return "", fmt.Errorf("get secret error : no implementation for %s", config.Get("app.secret.type"))
}
