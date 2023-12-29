package keyval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyVal ------------------------------------------------------------------------------

type RedisKeyVal struct {
	ctx    context.Context
	client *redis.Client
}

func (o *RedisKeyVal) Set(key string, val interface{}, ttl ...time.Duration) error {

	targetTTL := time.Second * 0

	if len(ttl) > 0 {
		targetTTL = ttl[0]
	}

	switch val.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, []byte:
		return o.client.Set(o.ctx, key, val, targetTTL).Err()
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return err
		}
		return o.client.Set(o.ctx, key, b, targetTTL).Err()
	}
}

func (o *RedisKeyVal) SetNX(key string, val interface{}, ttl ...time.Duration) bool {

	targetTTL := time.Second * 0

	if len(ttl) > 0 {
		targetTTL = ttl[0]
	}

	cmd := o.client.SetNX(o.ctx, key, val, targetTTL)

	if cmd.Err() == nil {

		return cmd.Val()
	}

	return false
}

func (o *RedisKeyVal) Get(key string) string {

	cmd := o.client.Get(o.ctx, key)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return ""
}

func (o *RedisKeyVal) Incr(key string) int64 {

	cmd := o.client.Incr(o.ctx, key)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return 0
}

func (o *RedisKeyVal) Decr(key string) int64 {

	cmd := o.client.Decr(o.ctx, key)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return 0
}

func (o *RedisKeyVal) GetInt(key string) int {

	cmd := o.client.Get(o.ctx, key)

	if cmd.Err() == nil {
		val, err := strconv.Atoi(cmd.Val())

		if err == nil {
			return val
		}
	}

	return 0
}

func (o *RedisKeyVal) GetObject(key string, obj interface{}) error {
	b, err := o.client.Get(o.ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return errors.New("[cache] not found")
		}
		return err
	}
	return json.Unmarshal(b, obj)
}

func (o *RedisKeyVal) Del(key string) error {

	cmd := o.client.Del(o.ctx, key)

	if cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

func (o *RedisKeyVal) LPush(key string, val interface{}) error {

	cmd := o.client.LPush(o.ctx, key, val)

	if cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

func (o *RedisKeyVal) LPop(key string) string {

	cmd := o.client.LPop(o.ctx, key)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return ""
}

func (o *RedisKeyVal) LPopCount(key string, count int) []string {

	cmd := o.client.LPopCount(o.ctx, key, count)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return []string{}
}

func (o *RedisKeyVal) LPopInt(key string) int {

	cmd := o.client.LPop(o.ctx, key)

	if cmd.Err() == nil {
		val, err := strconv.Atoi(cmd.Val())

		if err == nil {
			return val
		}
	}

	return 0
}

func (o *RedisKeyVal) RPush(key string, val interface{}) error {

	cmd := o.client.RPush(o.ctx, key, val)

	if cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

func (o *RedisKeyVal) RPop(key string) string {

	cmd := o.client.RPop(o.ctx, key)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return ""
}

func (o *RedisKeyVal) RPopCount(key string, count int) []string {

	cmd := o.client.RPopCount(o.ctx, key, count)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return []string{}
}

func (o *RedisKeyVal) RPopInt(key string) int {

	cmd := o.client.RPop(o.ctx, key)

	if cmd.Err() == nil {
		val, err := strconv.Atoi(cmd.Val())

		if err == nil {
			return val
		}
	}

	return 0
}

func (o *RedisKeyVal) Len(key string) int64 {

	cmd := o.client.LLen(o.ctx, key)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return 0
}

func (o *RedisKeyVal) AtIndex(key string, index int64) string {

	cmd := o.client.LIndex(o.ctx, key, index)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return ""
}

func (o *RedisKeyVal) AtIndexInt(key string, index int64) int {

	cmd := o.client.LIndex(o.ctx, key, index)

	if cmd.Err() == nil {
		val, err := strconv.Atoi(cmd.Val())

		if err == nil {
			return val
		}
	}

	return 0
}

func (o *RedisKeyVal) Range(key string, start int64, stop int64) []string {

	cmd := o.client.LRange(o.ctx, key, start, stop)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return nil
}

func (o *RedisKeyVal) HSet(key string, values map[string]interface{}) error {

	keyval := []interface{}{}

	for name, val := range values {
		keyval = append(keyval, name)
		keyval = append(keyval, val)
	}

	return o.client.HSet(o.ctx, key, keyval...).Err()
}

func (o *RedisKeyVal) HSetNX(key string, values map[string]interface{}) error {

	for name, val := range values {

		o.client.HSetNX(o.ctx, key, name, val)
	}

	return nil
}

func (o *RedisKeyVal) HGet(key, field string) string {

	cmd := o.client.HGet(o.ctx, key, field)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return ""
}

func (o *RedisKeyVal) HGetAll(key string) map[string]string {

	cmd := o.client.HGetAll(o.ctx, key)

	if cmd.Err() == nil {
		return cmd.Val()
	}

	return nil
}

func (o *RedisKeyVal) HDel(key string, fields ...string) error {

	return o.client.HDel(o.ctx, key, fields...).Err()
}

func (o *RedisKeyVal) HLen(key string) int {

	cmd := o.client.HLen(o.ctx, key)

	if cmd.Err() == nil {
		return int(cmd.Val())
	}

	return 0
}

func (o *RedisKeyVal) HScan(key, pattern string, from, count int) map[string]string {

	ret := map[string]string{}
	keys, _, err := o.client.HScan(o.ctx, key, uint64(from), pattern, int64(count)).Result()

	if err != nil {
		fmt.Println("HSCAN ERROR : ", err)
		return ret
	}

	keyPos := false
	itemKey := ""

	for _, val := range keys {

		keyPos = !keyPos

		if keyPos {
			itemKey = val
		} else {
			ret[itemKey] = val
		}
	}

	return ret
}

func (o *RedisKeyVal) Expire(key string, ttl time.Duration) error {

	return o.client.Expire(o.ctx, key, ttl).Err()
}

func (o *RedisKeyVal) Exist(key string) bool {
	return o.client.Exists(o.ctx, key).Val() > 0
}

// Init -------------------------------------------------------------------------------

func init() {
	RegKeyValCreator("redis", func(url string) (KeyValClient, error) {
		ret := &RedisKeyVal{
			ctx: context.Background(),
		}

		opt, err := redis.ParseURL(url)

		if err != nil {
			return nil, fmt.Errorf("unable to parse redis url %w", err)
		}

		ret.client = redis.NewClient(opt)

		if err != nil {
			return nil, fmt.Errorf("unable to connect redis %w", err)
		}

		return ret, nil
	})
}
