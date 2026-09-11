package models

import "time"

type PingRecord struct {
	Client     string    `json:"client" gorm:"type:varchar(36);not null;index"`
	ClientInfo Client    `json:"client_info" gorm:"foreignKey:Client;references:UUID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	TaskId     uint      `json:"task_id" gorm:"not null;index"`
	Task       PingTask  `json:"task" gorm:"foreignKey:TaskId;references:Id;constraint:OnDelete:CASCADE,OnUpdate:CASCADE;"`
	Time       time.Time `json:"time" gorm:"index;not null"`
	Value      int       `json:"value" gorm:"type:int;not null"` // Ping 值，单位毫秒
}

// PingTask 表示一次延迟监测任务配置。
type PingTask struct {
	Id        uint        `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	Weight    int         `json:"weight" gorm:"type:int;not null;default:0;index"`
	Name      string      `json:"name" gorm:"type:varchar(255);not null;index"`
	Clients   StringArray `json:"clients" gorm:"type:longtext"`
	DefaultOn bool        `json:"default_on" gorm:"column:all_clients;not null;default:false"` // 新加入的服务器是否自动开启此监测；现有服务器不受此字段影响
	Type      string      `json:"type" gorm:"type:varchar(12);not null;default:'icmp'"`        // icmp tcp http
	Target    string      `json:"target" gorm:"type:varchar(255);not null"`                    // Ping 目标地址
	Interval      int         `json:"interval" gorm:"type:int;not null;default:60"`                // 间隔时间
	IsReverse     bool        `json:"is_reverse" gorm:"default:false"`                             // 是否为反向监测任务
	ReverseSource string      `json:"reverse_source" gorm:"type:varchar(36);default:''"`          // 执行探测的探针源服务器 UUID
	ReverseTarget string      `json:"reverse_target" gorm:"type:varchar(36);default:''"`          // 接收指标数据并展示的受测目标服务器 UUID
	Port          int         `json:"port" gorm:"type:int;default:0"`                              // TCP 探测的目标端口 (默认 22)
	IPPreference  string      `json:"ip_preference" gorm:"type:varchar(10);default:'ipv4'"`       // ipv4 | ipv6 | auto
}

// AppliesToClient 判断当前 PingTask 是否适用于指定服务器。
func (task PingTask) AppliesToClient(uuid string) bool {
	if uuid == "" {
		return false
	}
	if task.IsReverse {
		if task.ReverseSource == uuid {
			return false // 探针源本身不作为受测展示目标
		}
		if task.ReverseTarget != "" && task.ReverseTarget == uuid {
			return true // 兼容历史单目标任务
		}
		for _, client := range task.Clients {
			if client == uuid {
				return true
			}
		}
		return false
	}
	for _, client := range task.Clients {
		if client == uuid {
			return true
		}
	}
	return false
}
