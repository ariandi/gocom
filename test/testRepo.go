package main

import (
	"sync"

	"github.com/oklog/ulid/v2"
	"gitlab.axiatadigitallabs.com/adlindo/gocom"
	"gorm.io/gorm"
)

// Model --------------------------------------

type Test struct {
	gorm.Model
	ID         string
	DataString string
	DataInt    int
	DataBool   bool
}

func (o *Test) BeforeCreate(db *gorm.DB) error {

	if o.ID == "" {

		o.ID = ulid.Make().String()
	}

	return nil
}

// Repo --------------------------------------

type TestRepo struct {
	gocom.BaseRepo
}

var testRepo *TestRepo
var testRepoOnce sync.Once

func (o *TestRepo) GetOne(id string) *Test {

	ret := &Test{}

	if o.Where("id = ?", id).First(ret).Error == nil {
		return ret
	}

	return nil
}

func (o *TestRepo) GetAll() []*Test {

	var ret []*Test = []*Test{}

	o.Model(&Test{}).Order("id asc").Find(&ret)
	return ret
}

func GetTestRepo() *TestRepo {

	testRepoOnce.Do(func() {

		testRepo = &TestRepo{}
		testRepo.AutoMigrate(&Test{})
	})

	return testRepo
}
