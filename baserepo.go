package gocom

import (
	"gorm.io/gorm"
)

type BaseRepo struct {
	ConnName string
}

func (o *BaseRepo) AutoMigrate(value ...interface{}) error {
	return DB(o.ConnName).AutoMigrate(value...)
}

func (o *BaseRepo) Create(value interface{}) *gorm.DB {

	return DB(o.ConnName).Create(value)
}

func (o *BaseRepo) Update(value interface{}) *gorm.DB {

	return DB(o.ConnName).Save(value)
}

func (o *BaseRepo) Delete(value interface{}) *gorm.DB {

	return DB(o.ConnName).Delete(value)
}

func (o *BaseRepo) First(dest interface{}, conds ...interface{}) *gorm.DB {

	return DB(o.ConnName).First(dest, conds...)
}

func (o *BaseRepo) Find(dest interface{}, conds ...interface{}) *gorm.DB {

	return DB(o.ConnName).Find(dest, conds...)
}

func (o *BaseRepo) Where(query interface{}, args ...interface{}) *gorm.DB {

	return DB(o.ConnName).Where(query, args...)
}

func (o *BaseRepo) Model(value interface{}) *gorm.DB {

	return DB(o.ConnName).Model(value)
}

func (o *BaseRepo) Table(name string) *gorm.DB {

	return DB(o.ConnName).Table(name)
}

func (o *BaseRepo) Raw(sql string, args ...interface{}) *gorm.DB {

	return DB(o.ConnName).Raw(sql, args...)
}

func (o *BaseRepo) Exec(sql string, args ...interface{}) *gorm.DB {

	return DB(o.ConnName).Exec(sql, args...)
}
