package rabbitMQ

import (
	"fmt"
)

func QueueService(list map[string]error) {
	for s, err := range list {
		if err != nil {
			fmt.Printf("[%s] 启动遇到错误: %s\n", s, err.Error())
		} else {
			fmt.Printf("[%s] 已启动\n", s)
		}
	}
}
