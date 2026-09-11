package tasks

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

func TestReversePingAppliesToClient(t *testing.T) {
	// 常规任务：通过 Clients 列表匹配
	forwardTask := models.PingTask{
		Id:        1,
		IsReverse: false,
		Clients:   models.StringArray{"vps-1", "vps-2"},
	}
	if !forwardTask.AppliesToClient("vps-1") {
		t.Errorf("expected forwardTask to apply to vps-1")
	}
	if forwardTask.AppliesToClient("vps-3") {
		t.Errorf("expected forwardTask NOT to apply to vps-3")
	}

	// 统一反向任务：包含多个目标 VPS，但不包含探针源
	unifiedReverseTask := models.PingTask{
		Id:            10,
		IsReverse:     true,
		ReverseSource: "home-node",
		Clients:       models.StringArray{"vps-1", "vps-2", "vps-3"},
	}
	if !unifiedReverseTask.AppliesToClient("vps-1") {
		t.Errorf("expected unifiedReverseTask to apply to vps-1")
	}
	if !unifiedReverseTask.AppliesToClient("vps-2") {
		t.Errorf("expected unifiedReverseTask to apply to vps-2")
	}
	if unifiedReverseTask.AppliesToClient("home-node") {
		t.Errorf("expected unifiedReverseTask NOT to apply to probe source home-node")
	}
	if unifiedReverseTask.AppliesToClient("vps-other") {
		t.Errorf("expected unifiedReverseTask NOT to apply to unlisted vps-other")
	}
}

func TestWireTaskIDAllocationAndLookup(t *testing.T) {
	taskID := uint(10)
	target1 := "vps-uuid-1"
	target2 := "vps-uuid-2"

	wireID1 := GetOrAllocateWireTaskID(taskID, target1)
	wireID2 := GetOrAllocateWireTaskID(taskID, target2)

	if wireID1 == wireID2 {
		t.Fatalf("expected wire IDs to be distinct, got %d and %d", wireID1, wireID2)
	}

	// 幂等分配
	if again := GetOrAllocateWireTaskID(taskID, target1); again != wireID1 {
		t.Fatalf("expected idempotent wire ID %d, got %d", wireID1, again)
	}

	// 反向查询
	lookup1, ok1 := LookupReverseWireTarget(wireID1)
	if !ok1 || lookup1.TaskID != taskID || lookup1.TargetUUID != target1 {
		t.Fatalf("failed to lookup wireID1: %+v", lookup1)
	}

	lookup2, ok2 := LookupReverseWireTarget(wireID2)
	if !ok2 || lookup2.TaskID != taskID || lookup2.TargetUUID != target2 {
		t.Fatalf("failed to lookup wireID2: %+v", lookup2)
	}

	// 不存在的 ID
	if _, ok := LookupReverseWireTarget(99999999); ok {
		t.Fatalf("expected lookup of invalid wire ID to fail")
	}
}

func TestResolveReverseTargetAddressFormat(t *testing.T) {
	// 常规任务：原样返回 Target
	nonReverse := models.PingTask{
		Target:    "8.8.8.8",
		IsReverse: false,
	}
	if got := ResolveReverseTargetAddress(nonReverse); got != "8.8.8.8" {
		t.Errorf("expected 8.8.8.8, got %s", got)
	}
}

func TestPingTaskCache(t *testing.T) {
	sampleTask := models.PingTask{
		Id:            999,
		Name:          "CacheTest",
		IsReverse:     true,
		ReverseSource: "home",
		Clients:       models.StringArray{"vps"},
	}

	UpdatePingTaskCache([]models.PingTask{sampleTask})

	cached, err := GetPingTaskByID(999)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cached == nil || cached.Name != "CacheTest" {
		t.Fatalf("expected cached task with Name CacheTest, got %+v", cached)
	}
}
