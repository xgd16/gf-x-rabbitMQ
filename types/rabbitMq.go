package types

import "sync"

type RabbitMQCoroutineLockFreed struct {
	Operate *sync.WaitGroup
	Number  int64
}

type SpellGroupQueueData struct {
	SerialNumber string `json:"serialNumber"` // 编号
	UserId       int    `json:"userId"`       // 用户id
	SpellId      int    `json:"spellId"`      // 需要加入的拼团id
	GroupGoodsId int    `json:"groupGoodsId"` // 团商品id
	PayType      int    `json:"payType"`      // 支付类型 1 余额 2 红包
	AddressId    int    `json:"addressId"`    // 收货地址id
}

type BindRelationQueueData struct {
	UserId    int `json:"userId"`
	CaptainId int `json:"captainId"`
}

type RegisterHandler[T any] struct {
	Handler    func(*T) error
	TaskName   string
	SyncNum    int
	FieldNames []string
}
