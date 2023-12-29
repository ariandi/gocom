package worker

import (
	"github.com/alitto/pond"
	"gitlab.axiatadigitallabs.com/adlindo/gocom/config"
	"time"
)

var mapPond = make(map[string]*Pond)

func CreateNewPond(targetName string) *Pond {
	prefix := "app.worker."
	if config.HasConfig(prefix + targetName + ".type") {
		max := config.GetInt(prefix + targetName + ".max")
		min := config.GetInt(prefix + targetName + ".min")
		poolSize := config.GetInt(prefix+targetName+".poolSize", 200)
		return NewPond(max, poolSize, pond.MinWorkers(min))
	}
	return NewPond(100, 200, pond.MinWorkers(5))
}

func Get(name ...string) Worker {
	if !config.GetBool("app.worker.enable", false) {
		return &DirectWorker{}
	}

	targetName := "default"
	if len(name) > 0 {
		targetName = name[0]
	}

	p, ok := mapPond[targetName]
	if !ok {
		p = CreateNewPond(targetName)
		mapPond[targetName] = p
	}

	return p
}

type Worker interface {
	Submit(task func())
	SubmitAndWait(task func())
	SubmitBefore(task func(), dur time.Duration)
}
