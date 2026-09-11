package tasks

import (
	"fmt"
	"strings"
	"sync"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"
)

func init() {
	utils.ReverseTargetResolver = ResolveReverseTaskTargets
	utils.TargetResolver = ResolveReverseTargetAddress
}

var (
	pingTaskCacheLock sync.RWMutex
	pingTaskCacheMap  = make(map[uint]models.PingTask)
)

// UpdatePingTaskCache 更新内存中所有 PingTask 的高速缓存
func UpdatePingTaskCache(tasks []models.PingTask) {
	pingTaskCacheLock.Lock()
	defer pingTaskCacheLock.Unlock()

	newMap := make(map[uint]models.PingTask, len(tasks))
	for _, t := range tasks {
		newMap[t.Id] = t
	}
	pingTaskCacheMap = newMap
}

// GetPingTaskByID 获取指定 ID 的 PingTask（优先读取内存缓存以达到 0ms 开销）
func GetPingTaskByID(id uint) (*models.PingTask, error) {
	pingTaskCacheLock.RLock()
	if t, ok := pingTaskCacheMap[id]; ok {
		pingTaskCacheLock.RUnlock()
		return &t, nil
	}
	pingTaskCacheLock.RUnlock()

	db := dbcore.GetDBInstance()
	var task models.PingTask
	if err := db.Where("id = ?", id).First(&task).Error; err != nil {
		return nil, err
	}

	pingTaskCacheLock.Lock()
	pingTaskCacheMap[id] = task
	pingTaskCacheLock.Unlock()

	return &task, nil
}

// ReverseWireTarget 记录链路任务与真实任务、受测目标的对应关系
type ReverseWireTarget struct {
	TaskID     uint
	TargetUUID string
}

var (
	reverseWireLock   sync.RWMutex
	reverseWireMap    = make(map[string]uint)             // "taskID:targetUUID" -> wireTaskID
	reverseWireLookup = make(map[uint]ReverseWireTarget) // wireTaskID -> ReverseWireTarget
	nextWireID        uint = 1000000
)

// GetOrAllocateWireTaskID 获取或分配唯一的链路任务编号（WireTaskID），保证下发给探针的每个目标任务 ID 独立不重复
func GetOrAllocateWireTaskID(taskID uint, targetUUID string) uint {
	key := fmt.Sprintf("%d:%s", taskID, targetUUID)
	reverseWireLock.Lock()
	defer reverseWireLock.Unlock()

	if id, ok := reverseWireMap[key]; ok {
		return id
	}
	nextWireID++
	id := nextWireID
	reverseWireMap[key] = id
	reverseWireLookup[id] = ReverseWireTarget{
		TaskID:     taskID,
		TargetUUID: targetUUID,
	}
	return id
}

// LookupReverseWireTarget 根据链路任务编号还原出真实的 TaskID 与受测目标 ClientUUID
func LookupReverseWireTarget(wireTaskID uint) (ReverseWireTarget, bool) {
	reverseWireLock.RLock()
	defer reverseWireLock.RUnlock()

	target, ok := reverseWireLookup[wireTaskID]
	return target, ok
}

// AddReversePingTask 创建单一反向监测任务。
// 由 reverseSource（如家宽 Home）作为执行探针，探测 targets 列表中的各个目标 VPS，
// targets 将直接保存在任务的 Clients 中，对外界呈现为一条完整的单一任务。
func AddReversePingTask(
	name string,
	reverseSource string,
	targets []string,
	pingType string,
	port int,
	interval int,
	ipPreference string,
) (uint, error) {
	if name == "" || reverseSource == "" || len(targets) == 0 {
		return 0, fmt.Errorf("name, reverse_source, and at least one target are required")
	}
	if interval <= 0 {
		interval = 60
	}
	if pingType == "" {
		pingType = "tcp"
	}
	if pingType == "tcp" && port <= 0 {
		port = 22
	}
	if ipPreference == "" {
		ipPreference = "ipv4"
	}

	cleanTargets := make([]string, 0, len(targets))
	seen := make(map[string]bool)
	for _, t := range targets {
		if t != "" && t != reverseSource && !seen[t] {
			cleanTargets = append(cleanTargets, t)
			seen[t] = true
		}
	}
	if len(cleanTargets) == 0 {
		return 0, fmt.Errorf("no valid target server specified")
	}

	placeholderTarget := fmt.Sprintf(":%d", port)
	if pingType == "icmp" {
		placeholderTarget = "icmp"
	}

	task := models.PingTask{
		Name:          name,
		Clients:       cleanTargets,
		DefaultOn:     false,
		Type:          pingType,
		Target:        placeholderTarget,
		Interval:      interval,
		IsReverse:     true,
		ReverseSource: reverseSource,
		ReverseTarget: "", // 统一多目标任务，受测节点统一置于 Clients 中
		Port:          port,
		IPPreference:  ipPreference,
	}

	db := dbcore.GetDBInstance()
	if err := db.Create(&task).Error; err != nil {
		return 0, err
	}

	_ = db.Model(&models.PingTask{}).Where("id = ?", task.Id).Update("weight", int(task.Id)).Error

	ReloadPingSchedule()
	return task.Id, nil
}

// AddReversePingTasks 保持接口兼容性，内部统一创建为单任务并返回 TaskID 切片
func AddReversePingTasks(
	name string,
	reverseSource string,
	targets []string,
	pingType string,
	port int,
	interval int,
	ipPreference string,
) ([]uint, error) {
	id, err := AddReversePingTask(name, reverseSource, targets, pingType, port, interval, ipPreference)
	if err != nil {
		return nil, err
	}
	return []uint{id}, nil
}

// ResolveReverseTaskTargets 将一个反向 PingTask 解析为下发给探针的各个目标链路任务（包含实时目标 IP 与 WireTaskID）
func ResolveReverseTaskTargets(task models.PingTask) []utils.WireTarget {
	if !task.IsReverse || task.ReverseSource == "" {
		return nil
	}

	var targetUUIDList []string
	if task.DefaultOn {
		var allClients []models.Client
		db := dbcore.GetDBInstance()
		_ = db.Select("uuid").Find(&allClients).Error
		targetUUIDList = make([]string, 0, len(allClients))
		for _, c := range allClients {
			if c.UUID != task.ReverseSource {
				targetUUIDList = append(targetUUIDList, c.UUID)
			}
		}
	} else {
		targetUUIDList = make([]string, 0, len(task.Clients))
		for _, c := range task.Clients {
			if c != "" && c != task.ReverseSource {
				targetUUIDList = append(targetUUIDList, c)
			}
		}
		if len(targetUUIDList) == 0 && task.ReverseTarget != "" {
			targetUUIDList = []string{task.ReverseTarget}
		}
	}

	if len(targetUUIDList) == 0 {
		return nil
	}

	db := dbcore.GetDBInstance()
	var targetClients []models.Client
	if err := db.Where("uuid IN ?", targetUUIDList).Find(&targetClients).Error; err != nil {
		return nil
	}

	clientMap := make(map[string]models.Client, len(targetClients))
	for _, c := range targetClients {
		clientMap[c.UUID] = c
	}

	wireTargets := make([]utils.WireTarget, 0, len(targetUUIDList))
	for _, targetUUID := range targetUUIDList {
		client, ok := clientMap[targetUUID]
		if !ok {
			continue
		}

		var ip string
		pref := strings.ToLower(task.IPPreference)
		if pref == "ipv6" {
			ip = client.IPv6
			if ip == "" {
				ip = client.IPv4
			}
		} else {
			ip = client.IPv4
			if ip == "" {
				ip = client.IPv6
			}
		}

		if ip == "" {
			continue // 该受测目标暂无有效 IP，跳过
		}

		var address string
		if task.Type == "tcp" {
			port := task.Port
			if port <= 0 {
				port = 22
			}
			if strings.Contains(ip, ":") {
				address = fmt.Sprintf("[%s]:%d", ip, port)
			} else {
				address = fmt.Sprintf("%s:%d", ip, port)
			}
		} else {
			address = ip
		}

		wireID := GetOrAllocateWireTaskID(task.Id, targetUUID)
		wireTargets = append(wireTargets, utils.WireTarget{
			TargetUUID: targetUUID,
			Address:    address,
			WireTaskID: wireID,
		})
	}

	return wireTargets
}

// ResolveReverseTargetAddress 保持向下兼容的单地址解析函数
func ResolveReverseTargetAddress(task models.PingTask) string {
	targets := ResolveReverseTaskTargets(task)
	if len(targets) > 0 {
		return targets[0].Address
	}
	return task.Target
}

// MigrateSplitReverseTasks 自动合并此前创建的分散同名同源反向任务为单一任务
func MigrateSplitReverseTasks() {
	db := dbcore.GetDBInstance()
	if db == nil {
		return
	}

	var reverseTasks []models.PingTask
	if err := db.Where("is_reverse = ?", true).Find(&reverseTasks).Error; err != nil || len(reverseTasks) == 0 {
		return
	}

	// 按 (Name, ReverseSource, Port, Type, Interval) 分组
	groups := make(map[string][]models.PingTask)
	for _, t := range reverseTasks {
		// 仅合并设置了单目标 ReverseTarget 的拆分任务
		if t.ReverseTarget != "" {
			key := fmt.Sprintf("%s|%s|%d|%s|%d", t.Name, t.ReverseSource, t.Port, t.Type, t.Interval)
			groups[key] = append(groups[key], t)
		}
	}

	for _, taskList := range groups {
		if len(taskList) <= 1 {
			continue
		}

		primary := taskList[0]
		seen := make(map[string]bool)
		var mergedClients []string
		for _, c := range primary.Clients {
			if c != "" && !seen[c] {
				mergedClients = append(mergedClients, c)
				seen[c] = true
			}
		}

		var redundantIDs []uint
		for _, t := range taskList {
			target := t.ReverseTarget
			if target != "" && !seen[target] {
				mergedClients = append(mergedClients, target)
				seen[target] = true
			}
			for _, c := range t.Clients {
				if c != "" && !seen[c] {
					mergedClients = append(mergedClients, c)
					seen[c] = true
				}
			}
			if t.Id != primary.Id {
				redundantIDs = append(redundantIDs, t.Id)
			}
		}

		// 更新主任务
		_ = db.Model(&models.PingTask{}).Where("id = ?", primary.Id).Updates(map[string]interface{}{
			"clients":        models.StringArray(mergedClients),
			"reverse_target": "",
		}).Error

		// 清理冗余任务并迁移历史探测数据至主任务
		if len(redundantIDs) > 0 {
			_ = db.Where("id IN ?", redundantIDs).Delete(&models.PingTask{}).Error
			_ = db.Model(&models.PingRecord{}).Where("task_id IN ?", redundantIDs).Update("task_id", primary.Id).Error
		}
	}
}
