package jsonrpc

import (
	"context"

	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/pkg/rpc"
)

// admin.reverse_ping.go
// 独立注册反向延迟监测（Reverse Ping）管理接口，完全解耦避免上游冲突。

func init() {
	RegisterWithGroupAndMeta("addReversePingTask", rpc.RoleAdmin, adminAddReversePingTask, &rpc.MethodMeta{
		Name:    "admin:addReversePingTask",
		Summary: "Create reverse ping tasks for multiple target servers",
		Returns: "{ task_ids: uint[] }",
	})
}

func adminAddReversePingTask(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		Name          string   `json:"name"`
		ReverseSource string   `json:"reverse_source"`
		Targets       []string `json:"targets"`
		Type          string   `json:"type"`
		Port          int      `json:"port"`
		Interval      int      `json:"interval"`
		IPPreference  string   `json:"ip_preference"`
	}

	if err := req.BindParams(&params); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, "invalid request parameters: "+err.Error(), nil)
	}

	if params.Name == "" || params.ReverseSource == "" || len(params.Targets) == 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "name, reverse_source, and at least one target are required", nil)
	}

	if params.Type == "" {
		params.Type = "tcp"
	}
	if params.Interval <= 0 {
		params.Interval = 60
	}
	if params.Type == "tcp" && params.Port <= 0 {
		params.Port = 22
	}

	taskIDs, err := tasks.AddReversePingTasks(
		params.Name,
		params.ReverseSource,
		params.Targets,
		params.Type,
		params.Port,
		params.Interval,
		params.IPPreference,
	)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}

	return map[string]any{"task_ids": taskIDs}, nil
}
