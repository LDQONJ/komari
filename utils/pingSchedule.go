package utils

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/internal/scheduler"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	agent_runtime "github.com/komari-monitor/komari/web/agent"
)

// PingTaskManager 管理定时器和任务
type PingTaskManager struct {
	mu    sync.Mutex
	tasks map[int][]models.PingTask
}

var manager = &PingTaskManager{
	tasks: make(map[int][]models.PingTask),
}

// Reload 重载时间表
func (m *PingTaskManager) Reload(pingTasks []models.PingTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	scheduler.RemovePrefix("ping:")
	m.tasks = make(map[int][]models.PingTask)

	// 按Interval分组任务
	taskGroups := make(map[int][]models.PingTask)
	for _, task := range pingTasks {
		if task.Interval <= 0 {
			continue
		}
		taskGroups[task.Interval] = append(taskGroups[task.Interval], task)
	}

	// 为每个唯一的Interval创建协程
	for interval, tasks := range taskGroups {
		interval := interval
		tasks := append([]models.PingTask(nil), tasks...)
		m.tasks[interval] = tasks
		if err := scheduler.AddContextFunc(fmt.Sprintf("ping:%d", interval), scheduler.Every(time.Duration(interval)*time.Second), false, func(ctx context.Context) {
			for _, task := range tasks {
				go executePingTask(ctx, task)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// WireTarget 描述反向探测受测目标的解析后信息
type WireTarget struct {
	TargetUUID string
	Address    string
	WireTaskID uint
}

// ReverseTargetResolver 用于将反向探测任务解析为各个受测目标的链路任务列表
var ReverseTargetResolver func(task models.PingTask) []WireTarget

// TargetResolver 保持向下兼容
var TargetResolver func(task models.PingTask) string

// executePingTask 执行单个PingTask
func executePingTask(ctx context.Context, task models.PingTask) {
	if task.IsReverse {
		if ReverseTargetResolver != nil && task.ReverseSource != "" {
			wireTargets := ReverseTargetResolver(task)
			for _, wt := range wireTargets {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var message struct {
					TaskID  uint   `json:"ping_task_id"`
					Message string `json:"message"`
					Type    string `json:"ping_type"`
					Target  string `json:"ping_target"`
				}
				message.Message = "ping"
				message.TaskID = wt.WireTaskID
				message.Type = task.Type
				message.Target = wt.Address

				agent_runtime.DispatchPing(task.ReverseSource, message, v2.PingParams{
					TaskID: wt.WireTaskID,
					Type:   task.Type,
					Target: wt.Address,
				})
			}
		}
		return
	}

	pingTarget := task.Target
	var message struct {
		TaskID  uint   `json:"ping_task_id"`
		Message string `json:"message"`
		Type    string `json:"ping_type"`
		Target  string `json:"ping_target"`
	}

	message.Message = "ping"
	message.TaskID = task.Id
	message.Type = task.Type
	message.Target = pingTarget

	for _, clientUUID := range targetPingClientUUIDs(task) {
		select {
		case <-ctx.Done():
			// Context was canceled, stop sending pings.
			return
		default:
			// Context is still active, continue.
		}

		agent_runtime.DispatchPing(clientUUID, message, v2.PingParams{TaskID: task.Id, Type: task.Type, Target: pingTarget})
	}
}

// targetPingClientUUIDs 根据任务配置计算本次调度需要下发的在线服务器列表。
func targetPingClientUUIDs(task models.PingTask) []string {
	return task.Clients
}

// ReloadPingSchedule 加载或重载时间表
func ReloadPingSchedule(pingTasks []models.PingTask) error {
	return manager.Reload(pingTasks)
}
