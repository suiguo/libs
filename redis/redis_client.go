package redis

import (
	"encoding/json"
	json2 "encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/suiguo/libs/logger"

	"github.com/go-redis/redis"
)

var instanceMap = make(map[string]*Client)
var redis_lock sync.RWMutex

// Client is for
type Client struct {
	Cc      *redis.Client
	log     logger.Logger
	srvName string
}

const ResultNil = redis.Nil

type RedisCfg struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Dbname   int    `json:"dbname"`
	PassWord string `json:"pwd"`
}

func GetInstance(log logger.Logger, cfg *RedisCfg) (*Client, error) {
	if log == nil || cfg == nil {
		return nil, fmt.Errorf("log or cfg is nil")
	}
	if key, err := json.Marshal(cfg); err == nil {
		redis_lock.RLock()
		r := instanceMap[string(key)]
		redis_lock.RUnlock()
		if r == nil {
			rdb := redis.NewClient(&redis.Options{
				Addr:     cfg.Host + ":" + strconv.Itoa(cfg.Port),
				Password: cfg.PassWord, // no password set
				DB:       cfg.Dbname,   // use default DB

			})
			errors := rdb.Ping().Err()
			if errors != nil {
				return nil, errors
			}
			r = &Client{
				Cc:  rdb,
				log: log,
			}
			redis_lock.Lock()
			instanceMap[string(key)] = r
			redis_lock.Unlock()
		}
		return r, nil
	} else {
		return nil, err
	}
}

// EXPIRE is for
func (c *Client) EXPIRE(key string, dur time.Duration) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	c.Cc.Expire(key, dur)
	return nil
}

//
func (c *Client) SPOP(key string) (string, error) {
	if c == nil || c.Cc == nil {
		return "", fmt.Errorf("redis not init")
	}
	return c.Cc.SPop(key).Result()
}

// SADD is for
func (c *Client) SADD(key string, values ...interface{}) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	return c.Cc.SAdd(key, values...).Err()
}

// SREM is for
func (c *Client) SREM(key string, values ...interface{}) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	return c.Cc.SRem(key, values...).Err()
}

// SMEMBERS is ofr
func (c *Client) SMEMBERS(key string) ([]string, error) {
	if c == nil || c.Cc == nil {
		return []string{}, fmt.Errorf("redis not init ")
	}
	return c.Cc.SMembers(key).Result()
}

//BLPOP is for
func (c *Client) BLPOP(key string, t time.Duration) ([]string, error) {
	if c == nil || c.Cc == nil {
		return []string{}, fmt.Errorf("redis not init ")
	}
	return c.Cc.BLPop(t, key).Result()
}

//RPUSH is for
func (c *Client) RPUSH(key string, value ...interface{}) (int64, error) {
	if c == nil || c.Cc == nil {
		return 0, fmt.Errorf("redis not init ")
	}
	return c.Cc.RPush(key, value).Result()
}

//RPUSH is for
func (c *Client) LRANGE(key string, start, stop int64) ([]string, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	return c.Cc.LRange(key, start, stop).Result()
}

//SET is for
func (c *Client) SET(key string, value interface{}, t time.Duration) (string, error) {
	if c == nil || c.Cc == nil {
		return "", fmt.Errorf("redis not init ")
	}
	return c.Cc.Set(key, value, t).Result()
}

//SETNX is for
func (c *Client) SETNX(key string, value interface{}, t time.Duration) (bool, error) {
	if c == nil || c.Cc == nil {
		return false, fmt.Errorf("redis not init ")
	}
	return c.Cc.SetNX(key, value, t).Result()
}

//DEL is for
func (c *Client) DEL(key ...string) (int64, error) {
	if c == nil || c.Cc == nil {
		return 0, fmt.Errorf("redis not init ")
	}
	return c.Cc.Del(key...).Result()
}

//GET is for
func (c *Client) GET(key string) (string, error) {
	if c == nil || c.Cc == nil {
		return "", fmt.Errorf("redis not init ")
	}
	return c.Cc.Get(key).Result()
}

//XADDJSON is for add message to stream, if stream not exist, stream will create.
//XADDJSON will format all interface{} value to json str.
func (c *Client) XADDJSON(stream string, vals map[string]interface{}) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	//reformat interface to json str
	for k, v := range vals {
		res, _ := json2.Marshal(v)
		vals[k] = res
	}
	xargs := redis.XAddArgs{
		Stream: stream,
		Values: vals,
	}
	stat := c.Cc.XAdd(&xargs)
	return stat.Err()
}

//XADD interface{} value is only support string and numeric value.
func (c *Client) XADD(stream, id string, maxlen int64, vals map[string]interface{}) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	xargs := redis.XAddArgs{
		Stream:       stream,
		ID:           id,
		MaxLenApprox: maxlen,
		Values:       vals,
	}
	stat := c.Cc.XAdd(&xargs)
	return stat.Err()
}

//XGROUP_CREATE create consumer group.
func (c *Client) XGROUP_CREATE(stream, group, start string) (string, error) {
	if c == nil || c.Cc == nil {
		return "", fmt.Errorf("redis not init ")
	}
	stat := c.Cc.XGroupCreate(stream, group, start)
	return stat.Result()
}

//XGROUP_DELETE delete consumer group.
func (c *Client) XGROUP_DELETE(stream, group string) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	stat := c.Cc.XGroupDestroy(stream, group)
	return stat.Err()
}

//XGROUP_SETID modify consumer group start.
func (c *Client) XGROUP_SETID(stream, group, start string) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	stat := c.Cc.XGroupSetID(stream, group, start)
	return stat.Err()
}

//XGROUP_DELCONSUMER delete consumer from group.
func (c *Client) XGROUP_DELCONSUMER(stream, group, consumer string) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	stat := c.Cc.XGroupDelConsumer(stream, group, consumer)
	return stat.Err()
}

//XGROUP_READ is for read message from stream
func (c *Client) XGROUP_READ(stream, group, consumer, start string, count int64, timeout time.Duration, noAck bool, jsonDecode bool) (*map[string]map[string]interface{}, error) {
	rgArgs := redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		// List of streams and ids.
		Streams: []string{stream, start},
		Count:   count,
		Block:   timeout,
		NoAck:   noAck,
	}
	stat := c.Cc.XReadGroup(&rgArgs)
	err := stat.Err()
	if err != nil {
		return nil, err
	}
	xstreams, err := stat.Result()
	if err != nil {
		return nil, err
	}
	result := make(map[string]map[string]interface{})
	for _, stream := range xstreams {
		for _, msg := range stream.Messages {
			ID := msg.ID
			if jsonDecode {
				for k, v := range msg.Values {
					var tmp interface{}
					err = json2.Unmarshal([]byte(fmt.Sprint(v)), &tmp)
					if err != nil {
						return nil, err
					}
					msg.Values[k] = tmp
				}
			}
			values := msg.Values
			result[ID] = values
		}
	}
	return &result, nil
}

//XPENDING_SCAN is for read message only received not ack.
func (c *Client) XPENDING_SCAN(stream, group, consumer, start, end string, count int64) (*map[string]map[string]interface{}, error) {
	rgArgs := redis.XPendingExtArgs{
		Stream:   stream,
		Group:    group,
		Start:    start,
		End:      end,
		Count:    count,
		Consumer: consumer,
	}
	stat := c.Cc.XPendingExt(&rgArgs)
	err := stat.Err()
	if err != nil {
		return nil, err
	}
	xstreams, err := stat.Result()
	if err != nil {
		return nil, err
	}
	result := make(map[string]map[string]interface{})
	for _, stream := range xstreams {
		tmp := make(map[string]interface{})
		tmp["consumer"] = stream.Consumer
		tmp["idle"] = stream.Idle
		tmp["retryCount"] = stream.RetryCount
		result[stream.Id] = tmp
	}
	return &result, nil
}

//XACK ack target message.
func (c *Client) XACK(stream, group string, ids ...string) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	stat := c.Cc.XAck(stream, group, ids...)
	return stat.Err()
}

//XINFO_GROUPS is for get stream group info
func (c *Client) XINFO_GROUPS(stream string) (*map[string]map[string]interface{}, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	args := []interface{}{"xinfo", "groups"}
	args = append(args, stream)
	cmd := redis.NewSliceCmd(args...)
	err := c.Cc.Process(cmd)
	if err != nil {
		return nil, err
	}
	res, err := cmd.Result()
	if err != nil {
		return nil, err
	}
	output := make(map[string]map[string]interface{})
	for _, v := range res {
		sliceV, ok := v.([]interface{})
		if !ok {
			return nil, fmt.Errorf("convert type error ")
		}
		tmp := make(map[string]interface{})
		tmp["consumers"] = sliceV[3]
		tmp["pending"] = sliceV[5]
		tmp["last-delivered-id"] = sliceV[7]

		keyStr, ok := sliceV[1].(string)
		if !ok {
			return nil, fmt.Errorf("convert type error ")
		}
		output[keyStr] = tmp
	}
	return &output, nil
}

//XCLAIM reclaim pending message to target consumer.
func (c *Client) XCLAIM(stream, group, consumer string, minIdle time.Duration, ids ...string) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	rgArgs := redis.XClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}
	stat := c.Cc.XClaim(&rgArgs)
	return stat.Err()
}

//HGET is for
func (c *Client) HGET(key string, field string) (string, error) {
	if c == nil || c.Cc == nil {
		return "", fmt.Errorf("redis not init ")
	}
	return c.Cc.HGet(key, field).Result()
}

//HGETALL is for
func (c *Client) HGETALL(key string) (map[string]string, error) {
	if c == nil || c.Cc == nil {
		return map[string]string{}, fmt.Errorf("redis not init ")
	}
	return c.Cc.HGetAll(key).Result()
}

//HSET is for
func (c *Client) HSET(name string, key string, value interface{}) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	return c.Cc.HSet(name, key, value).Err()
}

//HMGET is for
func (c *Client) HMGET(name string, keys ...string) ([]interface{}, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	return c.Cc.HMGet(name, keys...).Result()
}

//HMSET is for
func (c *Client) HMSET(name string, kv map[string]interface{}) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	return c.Cc.HMSet(name, kv).Err()
}

//HGET is for
func (c *Client) HDEL(key string, field string) (int64, error) {
	if c == nil || c.Cc == nil {
		return 0, fmt.Errorf("redis not init ")
	}
	return c.Cc.HDel(key, field).Result()
}

func (c *Client) HEXISTS(key string, field string) (bool, error) {
	if c == nil || c.Cc == nil {
		return false, fmt.Errorf("redis not init ")
	}
	return c.Cc.HExists(key, field).Result()
}

func (c *Client) EXISTS(key string) (bool, error) {
	if c == nil || c.Cc == nil {
		return false, fmt.Errorf("redis not init ")
	}
	r, err := c.Cc.Exists(key).Result()
	if err != nil {
		return false, err
	}
	return r > 0, err
}

//ZADD is for
func (c *Client) ZADD(name string, score float64, member interface{}) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	tmp := redis.Z{
		Score:  score,
		Member: member,
	}
	return c.Cc.ZAdd(name, tmp).Err()
}

//ZRevRangeByScore is for
func (c *Client) ZRevRangeByScore(key, min, max string, count int64) ([]string, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	opt := redis.ZRangeBy{
		Min:   min,
		Max:   max,
		Count: count,
	}
	return c.Cc.ZRevRangeByScore(key, opt).Result()
}

//ZRANGE is for
func (c *Client) ZRANGE(key string, start int64, stop int64) ([]string, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	return c.Cc.ZRange(key, start, stop).Result()
}

//ZREM is for
func (c *Client) ZREM(name string, member string) error {
	if c == nil || c.Cc == nil {
		return fmt.Errorf("redis not init ")
	}
	return c.Cc.ZRem(name, member).Err()
}

//ZPOPMIN is for
func (c *Client) ZPOPMIN(key string) ([]redis.Z, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	return c.Cc.ZPopMin(key).Result()
}

//ZPOPMAX is for
func (c *Client) ZPOPMAX(key string) ([]redis.Z, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	return c.Cc.ZPopMax(key).Result()
}

func (c *Client) ScriptLoad(script string) (string, error) {
	if c == nil || c.Cc == nil {
		return "", fmt.Errorf("redis not init ")
	}
	return c.Cc.ScriptLoad(script).Result()
}
func (c *Client) ScriptExists(sha ...string) ([]bool, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	return c.Cc.ScriptExists(sha...).Result()
}

func (c *Client) EvalSha(sha1 string, keys []string, args ...interface{}) (interface{}, error) {
	if c == nil || c.Cc == nil {
		return "", fmt.Errorf("redis not init ")
	}
	return c.Cc.EvalSha(sha1, keys, args...).Result()
}

func (c *Client) Eval(script string, keys []string, args ...interface{}) (interface{}, error) {
	if c == nil || c.Cc == nil {
		return "", fmt.Errorf("redis not init ")
	}
	return c.Cc.Eval(script, keys, args...).Result()
}

//INCR is for
func (c *Client) INCR(key string, value int64) (int64, error) {
	if c == nil || c.Cc == nil {
		return 0, fmt.Errorf("redis not init ")
	}
	return c.Cc.IncrBy(key, value).Result()
}

func (c *Client) HINCR(key string, feild string, value int64) (int64, error) {
	if c == nil || c.Cc == nil {
		return 0, fmt.Errorf("redis not init ")
	}
	if r := c.Cc.HIncrBy(key, feild, value); r != nil {
		return r.Result()
	}

	return 0, fmt.Errorf("result is nil")
}

func (c *Client) HINCRFLOAT(key string, feild string, value float64) (float64, error) {
	if c == nil || c.Cc == nil {
		return 0, fmt.Errorf("redis not init ")
	}
	if r := c.Cc.HIncrByFloat(key, feild, value); r != nil {
		return r.Result()
	}

	return 0, fmt.Errorf("result is nil")
}

func (c *Client) HKEYS(key string) ([]string, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	if r := c.Cc.HKeys(key); r != nil {
		return r.Result()
	}
	return nil, fmt.Errorf("result is nil")
}

func (c *Client) KEYS(key string) ([]string, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	if r := c.Cc.Keys(key); r != nil {
		return r.Result()
	}
	return nil, fmt.Errorf("result is nil")
}

// HIncr

func (c *Client) Pipeline() (redis.Pipeliner, error) {
	if c == nil || c.Cc == nil {
		return nil, fmt.Errorf("redis not init ")
	}
	return c.Cc.Pipeline(), nil
}
