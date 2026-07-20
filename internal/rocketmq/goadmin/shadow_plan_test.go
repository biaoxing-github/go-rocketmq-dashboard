package goadmin

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDefaultM6ShadowPlanIsValid(t *testing.T) {
	samples := DefaultM6ShadowPlan()
	if err := ValidateShadowPlan(samples); err != nil {
		t.Fatalf("expected default M6 shadow plan to be valid: %v", err)
	}

	required := map[string]bool{
		"command-smoke":                          false,
		"known-message":                          false,
		"unique-key-message":                     false,
		"query-msg-trace-by-id":                  false,
		"offset-message":                         false,
		"recent-topic-message":                   false,
		"topic-status":                           false,
		"topic-route":                            false,
		"topic-route-list":                       false,
		"topic-cluster-list":                     false,
		"topic-list":                             false,
		"cluster-list":                           false,
		"cluster-list-more-stats":                false,
		"stats-all":                              false,
		"consumer-progress":                      false,
		"consumer-connection":                    false,
		"list-user":                              false,
		"get-user":                               false,
		"create-user":                            false,
		"update-user":                            false,
		"copy-user":                              false,
		"copy-acl":                               false,
		"create-acl":                             false,
		"update-acl":                             false,
		"delete-acl":                             false,
		"list-acl":                               false,
		"get-acl":                                false,
		"controller-metadata":                    false,
		"controller-config":                      false,
		"get-broker-config":                      false,
		"get-broker-config-c":                    false,
		"get-namesrv-config":                     false,
		"get-consumer-config":                    false,
		"get-cold-ctr-info":                      false,
		"get-cold-ctr-info-c":                    false,
		"allocate-mq":                            false,
		"broker-status":                          false,
		"broker-status-c":                        false,
		"print-message":                          false,
		"print-message-queue":                    false,
		"consume-message":                        false,
		"query-consume-queue":                    false,
		"check-msg-send-rt":                      false,
		"send-msg-status":                        false,
		"send-message":                           false,
		"send-message-trace":                     false,
		"reset-master-flush-offset":              false,
		"clean-expired-cq":                       false,
		"clean-expired-cq-c":                     false,
		"clean-unused-topic":                     false,
		"clean-unused-topic-c":                   false,
		"delete-expired-commit-log":              false,
		"delete-expired-commit-log-c":            false,
		"check-rocksdb-cq-write-progress":        false,
		"dump-compaction-log":                    false,
		"export-pop-record":                      false,
		"export-pop-record-b":                    false,
		"producer":                               false,
		"producer-connection":                    false,
		"rocksdb-config-to-json-local":           false,
		"rocksdb-config-to-json-groups-local":    false,
		"rocksdb-config-to-json-offsets-local":   false,
		"export-metadata-in-rocksdb-local":       false,
		"broker-consume-stats":                   false,
		"ha-status":                              false,
		"ha-status-c":                            false,
		"get-broker-epoch":                       false,
		"get-broker-epoch-c":                     false,
		"get-sync-state-set":                     false,
		"get-sync-state-set-c":                   false,
		"export-configs":                         false,
		"export-metadata":                        false,
		"export-metrics":                         false,
		"update-namesrv-config":                  false,
		"update-broker-config":                   false,
		"update-cold-data-flow-ctr-group-config": false,
		"remove-cold-data-flow-ctr-group-config": false,
		"update-topic":                           false,
		"update-topic-list":                      false,
		"delete-topic":                           false,
		"update-sub-group":                       false,
		"update-sub-group-list":                  false,
		"delete-sub-group":                       false,
		"wipe-write-perm":                        false,
		"add-write-perm":                         false,
		"update-kv-config":                       false,
		"delete-kv-config":                       false,
		"reset-offset-by-time-specified-queue":   false,
		"reset-offset-by-time-all-queues":        false,
		"update-order-conf-get":                  false,
		"message-chain-cold":                     false,
		"message-chain-warm":                     false,
	}
	for _, sample := range samples {
		if _, ok := required[sample.Name]; ok {
			required[sample.Name] = true
		}
	}
	for name, found := range required {
		if !found {
			t.Fatalf("expected default plan to include %s, got %#v", name, samples)
		}
	}
}

func TestDefaultM6ShadowPlanSerializesFixedBodyMessageTargets(t *testing.T) {
	required := map[string]string{
		"known-message":      "queryMsgById",
		"unique-key-message": "queryMsgByUniqueKey",
		"offset-message":     "queryMsgByOffset",
		"export-configs":     "exportConfigs",
		"export-metadata":    "exportMetadata",
		"export-metrics":     "exportMetrics",
	}
	for _, sample := range DefaultM6ShadowPlan() {
		command, ok := required[sample.Name]
		if !ok {
			continue
		}
		if !sample.SerialTargets {
			t.Fatalf("%s must serialize targets because %s writes fixed body files", sample.Name, command)
		}
		if !strings.Contains(sample.Notes, "串行") {
			t.Fatalf("%s notes should explain serial target requirement, got %q", sample.Name, sample.Notes)
		}
		delete(required, sample.Name)
	}
	if len(required) != 0 {
		t.Fatalf("expected default M6 plan to contain fixed-body samples, missing=%#v", required)
	}
}

func TestDefaultM6ShadowPlanSerializesBrokerConfigTargets(t *testing.T) {
	required := map[string]string{
		"get-broker-config":   "getBrokerConfig -b",
		"get-broker-config-c": "getBrokerConfig -c",
	}
	for _, sample := range DefaultM6ShadowPlan() {
		command, ok := required[sample.Name]
		if !ok {
			continue
		}
		if !sample.SerialTargets {
			t.Fatalf("%s must serialize targets because %s can run native and auto remoting reads against the same broker config concurrently", sample.Name, command)
		}
		if !strings.Contains(sample.Notes, "串行") {
			t.Fatalf("%s notes should explain serial target requirement, got %q", sample.Name, sample.Notes)
		}
		delete(required, sample.Name)
	}
	if len(required) != 0 {
		t.Fatalf("expected default M6 plan to contain broker config samples, missing=%#v", required)
	}
}

func TestDefaultM6ShadowPlanSerializesMutationTargets(t *testing.T) {
	required := map[string]struct {
		command   string
		noteToken string
	}{
		"wipe-write-perm":                        {command: "wipeWritePerm", noteToken: "恢复"},
		"add-write-perm":                         {command: "addWritePerm", noteToken: "恢复"},
		"update-kv-config":                       {command: "updateKvConfig", noteToken: "清理"},
		"delete-kv-config":                       {command: "deleteKvConfig", noteToken: "预置"},
		"update-order-conf-delete":               {command: "updateOrderConf", noteToken: "预置"},
		"update-order-conf-put":                  {command: "updateOrderConf", noteToken: "清理"},
		"set-consume-mode":                       {command: "setConsumeMode", noteToken: "恢复"},
		"reset-offset-by-time-specified-queue":   {command: "resetOffsetByTime", noteToken: "清理"},
		"reset-offset-by-time-all-queues":        {command: "resetOffsetByTime", noteToken: "清理"},
		"create-user":                            {command: "createUser", noteToken: "清理"},
		"update-user":                            {command: "updateUser", noteToken: "预置"},
		"copy-user":                              {command: "copyUser", noteToken: "清理"},
		"copy-acl":                               {command: "copyAcl", noteToken: "预置"},
		"create-acl":                             {command: "createAcl", noteToken: "清理"},
		"update-acl":                             {command: "updateAcl", noteToken: "预置"},
		"delete-acl":                             {command: "deleteAcl", noteToken: "预置"},
		"list-acl":                               {command: "listAcl", noteToken: "预置"},
		"get-acl":                                {command: "getAcl", noteToken: "预置"},
		"check-msg-send-rt":                      {command: "checkMsgSendRT", noteToken: "串行"},
		"send-msg-status":                        {command: "sendMsgStatus", noteToken: "串行"},
		"send-message":                           {command: "sendMessage", noteToken: "串行"},
		"send-message-trace":                     {command: "sendMessage", noteToken: "串行"},
		"reset-master-flush-offset":              {command: "resetMasterFlushOffset", noteToken: "串行"},
		"clean-expired-cq":                       {command: "cleanExpiredCQ", noteToken: "串行"},
		"clean-expired-cq-c":                     {command: "cleanExpiredCQ", noteToken: "串行"},
		"clean-unused-topic":                     {command: "cleanUnusedTopic", noteToken: "串行"},
		"clean-unused-topic-c":                   {command: "cleanUnusedTopic", noteToken: "串行"},
		"delete-expired-commit-log":              {command: "deleteExpiredCommitLog", noteToken: "串行"},
		"delete-expired-commit-log-c":            {command: "deleteExpiredCommitLog", noteToken: "串行"},
		"update-namesrv-config":                  {command: "updateNamesrvConfig", noteToken: "恢复"},
		"update-broker-config":                   {command: "updateBrokerConfig", noteToken: "恢复"},
		"update-cold-data-flow-ctr-group-config": {command: "updateColdDataFlowCtrGroupConfig", noteToken: "清理"},
		"remove-cold-data-flow-ctr-group-config": {command: "removeColdDataFlowCtrGroupConfig", noteToken: "预置"},
		"update-topic":                           {command: "updateTopic", noteToken: "清理"},
		"update-topic-perm":                      {command: "updateTopicPerm", noteToken: "恢复"},
		"delete-topic":                           {command: "deleteTopic", noteToken: "预置"},
		"update-sub-group":                       {command: "updateSubGroup", noteToken: "清理"},
		"update-sub-group-list":                  {command: "updateSubGroupList", noteToken: "清理"},
		"delete-sub-group":                       {command: "deleteSubGroup", noteToken: "预置"},
	}
	for _, sample := range DefaultM6ShadowPlan() {
		expectation, ok := required[sample.Name]
		if !ok {
			continue
		}
		if !sample.SerialTargets {
			t.Fatalf("%s must serialize targets because %s mutates shared RocketMQ state", sample.Name, expectation.command)
		}
		if !strings.Contains(sample.Notes, expectation.noteToken) {
			t.Fatalf("%s notes should explain mutation cleanup with %q, got %q", sample.Name, expectation.noteToken, sample.Notes)
		}
		delete(required, sample.Name)
	}
	if len(required) != 0 {
		t.Fatalf("expected default M6 plan to contain mutation samples, missing=%#v", required)
	}
}

// TestDefaultM6ShadowPlanContainsUpdateOrderConfDelete 验证顺序配置删除样本具备串行执行和性能采样约束。
func TestDefaultM6ShadowPlanContainsUpdateOrderConfDelete(t *testing.T) {
	for _, sample := range DefaultM6ShadowPlan() {
		if sample.Name != "update-order-conf-delete" {
			continue
		}
		if !sample.SerialTargets || sample.MinSamples != 20 || !sample.RequireP95 {
			t.Fatalf("update-order-conf-delete must serialize four providers and require 20 p95 samples, got %#v", sample)
		}
		if sample.Args[0] != "updateOrderConf" || !strings.Contains(sample.Notes, "put") || !strings.Contains(sample.Notes, "delete") {
			t.Fatalf("update-order-conf-delete should document the seed and cleanup commands, got %#v", sample)
		}
		return
	}
	t.Fatal("expected default M6 plan to contain update-order-conf-delete")
}

// TestDefaultM6ShadowPlanContainsUpdateOrderConfPut 验证顺序配置写入样本具备串行执行和性能采样约束。
func TestDefaultM6ShadowPlanContainsUpdateOrderConfPut(t *testing.T) {
	for _, sample := range DefaultM6ShadowPlan() {
		if sample.Name != "update-order-conf-put" {
			continue
		}
		if !sample.SerialTargets || sample.MinSamples != 20 || !sample.RequireP95 {
			t.Fatalf("update-order-conf-put must serialize four providers and require 20 p95 samples, got %#v", sample)
		}
		if sample.Args[0] != "updateOrderConf" ||
			!strings.Contains(sample.Notes, "put") ||
			!strings.Contains(sample.Notes, "delete") {
			t.Fatalf("update-order-conf-put should document the target and cleanup commands, got %#v", sample)
		}
		return
	}
	t.Fatal("expected default M6 plan to contain update-order-conf-put")
}

// TestDefaultM6ShadowPlanContainsSetConsumeMode 验证消费模式变更样本具备串行执行和性能采样约束。
func TestDefaultM6ShadowPlanContainsSetConsumeMode(t *testing.T) {
	for _, sample := range DefaultM6ShadowPlan() {
		if sample.Name != "set-consume-mode" {
			continue
		}
		if !sample.SerialTargets || sample.MinSamples != 20 || !sample.RequireP95 {
			t.Fatalf("set-consume-mode must serialize four providers and require 20 p95 samples, got %#v", sample)
		}
		if sample.Args[0] != "setConsumeMode" ||
			!strings.Contains(sample.Notes, "POP") ||
			!strings.Contains(sample.Notes, "PULL") ||
			!strings.Contains(sample.Notes, "恢复") {
			t.Fatalf("set-consume-mode should document the target and baseline restore, got %#v", sample)
		}
		return
	}
	t.Fatal("expected default M6 plan to contain set-consume-mode")
}

// TestDefaultM6ShadowPlanContainsResetOffsetByTimeSpecifiedQueue 验证指定队列位点重置样本具备串行执行和性能采样约束。
func TestDefaultM6ShadowPlanContainsResetOffsetByTimeSpecifiedQueue(t *testing.T) {
	for _, sample := range DefaultM6ShadowPlan() {
		if sample.Name != "reset-offset-by-time-specified-queue" {
			continue
		}
		if !sample.SerialTargets || sample.MinSamples != 20 || !sample.RequireP95 {
			t.Fatalf("reset-offset-by-time-specified-queue must serialize four providers and require 20 p95 samples, got %#v", sample)
		}
		args := strings.Join(sample.Args, " ")
		if sample.Args[0] != "resetOffsetByTime" ||
			!strings.Contains(args, "-b <reset-offset-specified-broker-addr>") ||
			!strings.Contains(args, "-q <reset-offset-specified-queue-id>") ||
			!strings.Contains(args, "-o <reset-offset-specified-offset>") ||
			!strings.Contains(sample.Notes, "deleteSubGroup") ||
			!strings.Contains(sample.Notes, "updateSubGroup") {
			t.Fatalf("reset-offset-by-time-specified-queue should document the target and lifecycle commands, got %#v", sample)
		}
		return
	}
	t.Fatal("expected default M6 plan to contain reset-offset-by-time-specified-queue")
}

// TestDefaultM6ShadowPlanContainsResetOffsetByTimeAllQueues 验证全队列位点重置样本具备串行执行和性能采样约束。
func TestDefaultM6ShadowPlanContainsResetOffsetByTimeAllQueues(t *testing.T) {
	for _, sample := range DefaultM6ShadowPlan() {
		if sample.Name != "reset-offset-by-time-all-queues" {
			continue
		}
		if !sample.SerialTargets || sample.MinSamples != 20 || !sample.RequireP95 {
			t.Fatalf("reset-offset-by-time-all-queues must serialize four providers and require 20 p95 samples, got %#v", sample)
		}
		args := strings.Join(sample.Args, " ")
		if sample.Args[0] != "resetOffsetByTime" ||
			strings.Contains(args, " -b ") ||
			strings.Contains(args, " -q ") ||
			strings.Contains(args, " -o ") ||
			!strings.Contains(sample.Notes, "全部队列") ||
			!strings.Contains(sample.Notes, "deleteSubGroup") ||
			!strings.Contains(sample.Notes, "updateSubGroup") {
			t.Fatalf("reset-offset-by-time-all-queues should document the all-queue lifecycle, got %#v", sample)
		}
		return
	}
	t.Fatal("expected default M6 plan to contain reset-offset-by-time-all-queues")
}

// TestDefaultM6ShadowPlanContainsUpdateTopicPerm 验证权限变更样本具备串行执行和性能采样约束。
func TestDefaultM6ShadowPlanContainsUpdateTopicPerm(t *testing.T) {
	for _, sample := range DefaultM6ShadowPlan() {
		if sample.Name != "update-topic-perm" {
			continue
		}
		if !sample.SerialTargets || sample.MinSamples != 20 || !sample.RequireP95 {
			t.Fatalf("update-topic-perm must serialize four providers and require 20 p95 samples, got %#v", sample)
		}
		if sample.Args[0] != "updateTopicPerm" || !strings.Contains(sample.Notes, "perm=6") {
			t.Fatalf("update-topic-perm should document the official command and baseline permission, got %#v", sample)
		}
		return
	}
	t.Fatal("expected default M6 plan to contain update-topic-perm")
}

func TestDefaultM6ShadowPlanSerializesUpdateTopicListTargets(t *testing.T) {
	for _, sample := range DefaultM6ShadowPlan() {
		if sample.Name != "update-topic-list" {
			continue
		}
		if !sample.SerialTargets {
			t.Fatal("update-topic-list must serialize targets because it mutates shared topic metadata")
		}
		if sample.Args[0] != "updateTopicList" || !strings.Contains(sample.Notes, "updateTopicList") {
			t.Fatalf("update-topic-list should document the official command and cleanup requirement, got %#v", sample)
		}
		return
	}
	t.Fatal("expected default M6 plan to contain update-topic-list")
}

func TestValidateShadowPlanRequiresOfficialProvider(t *testing.T) {
	err := ValidateShadowPlan([]ShadowSample{{
		Name:       "missing-official",
		Args:       []string{"clusterList"},
		Providers:  []ShadowProviderMode{ShadowProviderNative},
		MinSamples: 1,
	}})

	if err == nil || !strings.Contains(err.Error(), "official") {
		t.Fatalf("expected official provider error, got %v", err)
	}
}

func TestValidateShadowPlanRequiresEnoughP95Samples(t *testing.T) {
	err := ValidateShadowPlan([]ShadowSample{{
		Name:       "p95-too-small",
		Args:       []string{"messageChain", "-t", "TopicTest", "-k", "MessageKey"},
		Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
		MinSamples: 19,
		RequireP95: true,
	}})

	if err == nil || !strings.Contains(err.Error(), "MinSamples") {
		t.Fatalf("expected p95 MinSamples error, got %v", err)
	}
}

func TestDefaultM6ShadowPlanReturnsDefensiveCopies(t *testing.T) {
	first := DefaultM6ShadowPlan()
	if len(first) == 0 {
		t.Fatal("expected non-empty default plan")
	}
	first[0].Args[0] = "mutated-command"
	first[0].Providers[0] = ShadowProviderAuto

	second := DefaultM6ShadowPlan()
	if second[0].Args[0] == "mutated-command" {
		t.Fatalf("expected args to be copied between default plan calls, got %#v", second[0].Args)
	}
	if second[0].Providers[0] == ShadowProviderAuto {
		t.Fatalf("expected providers to be copied between default plan calls, got %#v", second[0].Providers)
	}
}

func TestApplyShadowFixtureOverridesMarksConcreteSamplesExecutable(t *testing.T) {
	samples, err := ApplyShadowFixtureOverrides(DefaultM6ShadowPlan(), ShadowFixtureOverrides{
		Samples: []ShadowSampleFixture{
			{
				Name: "known-message",
				Args: []string{"queryMsgById", "-i",
					"AC18000300002A9F0000000000000000"},
			},
			{
				Name: "recent-topic-message",
				Args: []string{"queryMsgByKey", "-t",
					"GoadminQueryKeyTest", "-k", "goadmin-query-key"},
			},
		},
	})
	if err != nil {
		t.Fatalf("apply fixture overrides: %v", err)
	}

	plan := PlanShadowBatch(samples)

	if plan.ExecutableSamples != 2 || plan.SkippedSamples != 94 {
		t.Fatalf("expected 2 executable and 94 skipped samples, got executable=%d skipped=%d plan=%#v",
			plan.ExecutableSamples, plan.SkippedSamples, plan)
	}
	if plan.Executable[0].Name != "known-message" {
		t.Fatalf("expected known-message executable sample first, got %#v", plan.Executable)
	}
	if !plan.Executable[0].SerialTargets {
		t.Fatalf("expected known-message fixture to preserve SerialTargets")
	}
	if strings.Contains(strings.Join(plan.Executable[0].Args, " "), "<known-message-id>") {
		t.Fatalf("expected known-message placeholder to be replaced, got %#v", plan.Executable[0].Args)
	}
	if DefaultM6ShadowPlan()[1].Args[2] != "<known-message-id>" {
		t.Fatalf("expected default plan to remain unchanged")
	}
}

func TestApplyShadowFixtureOverridesCanForceSerialTargetsPerFixture(t *testing.T) {
	var overrides ShadowFixtureOverrides
	if err := json.Unmarshal([]byte(`{"samples":[{"name":"command-smoke","args":["exportConfigs","-c","DefaultCluster","-f","/tmp/m6-command-smoke/exportConfigs"],"serialTargets":true}]}`), &overrides); err != nil {
		t.Fatalf("unmarshal fixture overrides: %v", err)
	}

	samples, err := ApplyShadowFixtureOverrides(DefaultM6ShadowPlan(), overrides)
	if err != nil {
		t.Fatalf("apply fixture overrides: %v", err)
	}
	plan := PlanShadowBatch(samples)

	if plan.ExecutableSamples != 1 {
		t.Fatalf("expected one executable sample, got %#v", plan)
	}
	if plan.Executable[0].Name != "command-smoke" || !plan.Executable[0].SerialTargets {
		t.Fatalf("expected command-smoke fixture to force serial targets, got %#v", plan.Executable[0])
	}
	if DefaultM6ShadowPlan()[0].SerialTargets {
		t.Fatalf("expected default command-smoke sample to remain concurrent")
	}
}

func TestApplyShadowFixtureOverridesExpandsRepeatedFixtures(t *testing.T) {
	var overrides ShadowFixtureOverrides
	if err := json.Unmarshal([]byte(`{"samples":[{"name":"message-chain-warm","args":["messageChain","-t","GoadminM6TraceRichTest","-i","AC18000300002A9F000000001AD666AF","-g","GoadminM6TraceRichGroup","--traceTopic","RMQ_SYS_TRACE_TOPIC"],"repeat":20}]}`), &overrides); err != nil {
		t.Fatalf("unmarshal fixture overrides: %v", err)
	}

	samples, err := ApplyShadowFixtureOverrides(DefaultM6ShadowPlan(), overrides)
	if err != nil {
		t.Fatalf("apply fixture overrides: %v", err)
	}
	plan := PlanShadowBatch(samples)

	if plan.ExecutableSamples != 20 || plan.SkippedSamples != 95 {
		t.Fatalf("expected repeat fixture to expand to 20 executable samples and 95 skipped samples, got executable=%d skipped=%d plan=%#v",
			plan.ExecutableSamples, plan.SkippedSamples, plan)
	}
	for index, sample := range plan.Executable {
		if sample.Name != "message-chain-warm" {
			t.Fatalf("expected repeated sample %d to keep name message-chain-warm, got %#v", index, sample)
		}
		if strings.Contains(strings.Join(sample.Args, " "), "<") {
			t.Fatalf("expected repeated sample %d to be concrete, got %#v", index, sample.Args)
		}
	}
}

func TestApplyShadowFixtureOverridesRejectsUnknownSample(t *testing.T) {
	_, err := ApplyShadowFixtureOverrides(DefaultM6ShadowPlan(), ShadowFixtureOverrides{
		Samples: []ShadowSampleFixture{{
			Name: "not-in-default-plan",
			Args: []string{"clusterList"},
		}},
	})

	if err == nil || !strings.Contains(err.Error(), "not-in-default-plan") {
		t.Fatalf("expected unknown sample error, got %v", err)
	}
}
