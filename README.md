# GF 的 rabbit MQ 接入
### GITHUB [gf-x-rabbitMQ](https://github.com/xgd16/gf-x-rabbitMQ)

```go
package main

import (
	"github.com/xgd16/gf-x-rabbitMQ/rabbitMQ"
	"github.com/xgd16/gf-x-rabbitMQ/types"
)

func main() {
	rabbitMQ.QueueService(map[string]error{
		"测试队列": rabbitMQ.CreateConsumerHandler(&types.RegisterHandler[types.TestQueueData]{
			Handler:    handlers.TestQueue, // 执行的函数
			TaskName:   "TestQueue",        // 订阅的任务名称
			SyncNum:    30,                 // 同时运行的携程数
			FieldNames: []string{"age"},    // 基于哪些字段进行单线程限制
		}),
	})
}
```