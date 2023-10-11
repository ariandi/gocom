package gocom

import (
	"sync"

	"github.com/ariandi/gocom/config"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var dbMap map[string]*gorm.DB = map[string]*gorm.DB{}
var dbMutex sync.Mutex
var dbCreatorMap map[string]DBCreatorFunc = map[string]DBCreatorFunc{}

type DBCreatorFunc func(dsn string) (*gorm.DB, error)

func RegDBCreator(typeName string, creator DBCreatorFunc) {

	dbCreatorMap[typeName] = creator
}

func DB(name ...string) *gorm.DB {

	var targetName string

	if len(name) > 0 {
		targetName = name[0]
	}

	if targetName == "" {
		targetName = "default"
	}

	ret, ok := dbMap[targetName]

	if !ok {

		dbMutex.Lock()
		defer dbMutex.Unlock()

		// check whether other thread already create the db
		ret, ok = dbMap[targetName]

		if !ok {

			if config.HasConfig("app.db."+targetName+".type") && config.HasConfig("app.db."+targetName+".dsn") {

				dbType := config.Get("app.db." + targetName + ".type")

				creator, ok := dbCreatorMap[dbType]

				if ok {

					dsn := config.Get("app.db." + targetName + ".dsn")
					poolSize := config.GetInt("app.db."+targetName+".poolSize", 20)
					debug := config.GetBool("app.db."+targetName+".debug", false)

					var err error
					ret, err = creator(dsn)

					if err == nil {

						dbMap[targetName] = ret

						if debug {
							ret.Debug()
						}

						if poolSize > 0 {

							db, errDB := ret.DB()

							if errDB == nil {

								db.SetMaxOpenConns(poolSize)
							}
						}

						Logger().Info("Conected to DB", zap.Any("dbname", targetName))
					} else {

						Logger().Info("Error Conected to DB", zap.Error(err))
					}
				}
			}
		}
	}

	return ret
}

func init() {

	RegDBCreator("postgresql", func(dsn string) (*gorm.DB, error) {

		return gorm.Open(postgres.Open(dsn), &gorm.Config{})
	})
}
