package rabbitMQ

import (
	"fmt"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/os/gmlock"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/streadway/amqp"
	"github.com/xgd16/gf-x-rabbitMQ/types"
	"sync"
	"sync/atomic"
)

// MQCoroutineLock MQ 协程 锁
var MQCoroutineLock = gmlock.New()

// GetConn 获取 Rabbit MQ 连接信息
func GetConn() (conn *amqp.Connection, err error) {
	// 从配置获取连接信息
	cfg, err := g.Cfg().Get(gctx.New(), "rabbitMQ")
	// 处理获取配置错误
	if err != nil {
		return
	}
	// 将获取到的配置信息转换为map组
	cfgArr := cfg.MapStrVar()

	conn, err = amqp.Dial(
		fmt.Sprintf(
			"amqp://%s:%s@%s:%s/",
			cfgArr["username"].String(),
			cfgArr["password"].String(),
			cfgArr["host"].String(),
			cfgArr["port"].String(),
		),
	)

	return
}

// CreateChannel 创建一个连接通道
func CreateChannel(conn *amqp.Connection, taskName string, durable bool) (channel *amqp.Channel, err error) {
	channel, err = conn.Channel()
	// 处理开启通道失败
	if err != nil {
		return
	}
	// 定义一个队列来保存消息
	_, err = channel.QueueDeclare(
		taskName,
		durable,
		false,
		false,
		false,
		nil,
	)
	// 处理失败
	if err != nil {
		return
	}
	return
}

// FastPublish 快速进行队列消息推送
func FastPublish(channel *amqp.Channel, taskName string, data any, deliveryMode bool) (err error) {
	var deliveryModeNumber uint8 = 0
	// 消息持久存储模式
	if deliveryMode {
		deliveryModeNumber = amqp.Persistent
	}
	// 将 data 转换为 json
	jsonData, err := gjson.Encode(data)
	// 处理json解析错误
	if err != nil {
		return
	}
	// 推送队列消息
	if err = channel.Publish(
		"",
		taskName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: deliveryModeNumber,
			Body:         jsonData,
		},
	); err != nil {
		return
	}

	return
}

// CreateConsumer 创建一个消费者
func CreateConsumer(taskName string, conn *amqp.Connection, deliveryMode bool) (msg <-chan amqp.Delivery, err error) {
	channel, err := CreateChannel(conn, taskName, deliveryMode)
	// 处理错误
	if err != nil {
		return
	}

	// 创建消费者
	return channel.Consume(
		taskName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
}

// ConsumerCoroutineSameTypeDataLock 消费者 协程 同类型数据锁
func ConsumerCoroutineSameTypeDataLock(msg <-chan amqp.Delivery, taskName string, fieldName []string, syncNumber int, callback func(item *gjson.Json) error) {
	ctx := gctx.New()

	aChan := make(chan int, syncNumber)
	lockFreed := gmap.NewStrAnyMap(true)

	createLockF := func(fieldLockName string) *types.RabbitMQCoroutineLockFreed {
		d := &types.RabbitMQCoroutineLockFreed{
			Operate: new(sync.WaitGroup),
			Number:  0,
		}
		lockFreed.Set(fieldLockName, d)
		return d
	}
	// 启动 流任务处理
	for queueItem := range msg {
		// 将数据当作json对象进行解析
		bodyData, err := gjson.DecodeToJson(queueItem.Body)
		// 处理解析失败
		if err != nil {
			g.Log("queueInfo").Warning(ctx, "解析获取到的数据失败 已跳过", string(queueItem.Body))
			continue
		}
		aChan <- 1
		// 将解析后的数据调动用到 回调
		fieldData := func() string {
			var fieldData []string
			for _, s := range fieldName {
				fieldData = append(fieldData, bodyData.Get(s).String())
			}
			return gstr.Join(fieldData, "_")
		}()
		// 创建字段锁名称
		fieldLockName := fmt.Sprintf("%s_%s_%s_lock", taskName, gstr.Join(fieldName, "_"), fieldData)
		// 开启线程处理
		go func(aChan chan int, fieldLockName string, bodyData *gjson.Json) {
			defer func() {
				// 退出函数时调用
				if recoverErr := recover(); recoverErr != nil {
					g.Log("queueInfo").Error(ctx, recoverErr)
				}
				<-aChan
			}()
			g.Log("queueInfo").Printf(ctx, "运行任务 %s\n", fieldLockName)
			// 判断是否正在释放锁(进行 remove 操作) 防止发生死锁
			var l *types.RabbitMQCoroutineLockFreed
			if lData := lockFreed.Get(fieldLockName); lData == nil {
				l = createLockF(fieldLockName)
			} else {
				l = lData.(*types.RabbitMQCoroutineLockFreed)
			}
			// 当有操作时发起阻塞
			l.Operate.Wait()
			// 当开启一个锁时检测释放计数加1
			atomic.AddInt64(&l.Number, 1)
			MQCoroutineLock.Lock(fieldLockName)
			if handlerErr := callback(bodyData); handlerErr != nil {
				g.Log().Error(ctx, fmt.Sprintf("%s 队列出错", taskName), handlerErr)
			}
			MQCoroutineLock.Unlock(fieldLockName)
			// 释放锁时计数减1
			atomic.AddInt64(&l.Number, -1)
			// 判断计数为0时 触发回收内存
			if l.Number <= 0 {
				l.Operate.Add(1)
				lockFreed.Remove(fieldLockName)
				MQCoroutineLock.Remove(fieldLockName)
				l.Operate.Done()
			}
		}(aChan, fieldLockName, bodyData)
	}
}

// CreateConsumerHandler 创建一个执行消费者
func CreateConsumerHandler[T any](taskData *types.RegisterHandler[T]) (err error) {
	// 获取连接
	conn, err := GetConn()
	if err != nil {
		return
	}
	// 创建一个消费者
	consumer, err := CreateConsumer(taskData.TaskName, conn, false)
	if err != nil {
		return
	}
	// 循环接受 消费者数据
	go ConsumerCoroutineSameTypeDataLock(
		consumer,
		taskData.TaskName,
		[]string{"uid"},
		30,
		func(item *gjson.Json) error {
			data := new(T)
			if err := item.Scan(data); err != nil {
				return err
			}
			return taskData.Handler(data)
		},
	)
	return
}

// 开启
func sendQueue(taskName string, data any, deliveryMode bool) (err error) {
	// 开启获取 MQ 连接
	conn, err := GetConn()
	// 处理开启连接失败
	if err != nil {
		return
	}
	// 退出函数时关闭连接
	defer func() { _ = conn.Close() }()
	// 开启管道
	channel, err := CreateChannel(conn, taskName, deliveryMode)
	// 处理开启管道失败
	if err != nil {
		return
	}
	// 退出函数时关闭管道
	defer func() { _ = channel.Close() }()
	// 提交 队列任务
	if err = FastPublish(channel, taskName, data, deliveryMode); err != nil {
		return
	}

	return
}
