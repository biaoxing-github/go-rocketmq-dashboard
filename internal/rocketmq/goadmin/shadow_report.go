package goadmin

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ShadowReport 是可直接序列化为 JSONL 的 shadow compare 摘要，刻意不包含 stdout/stderr 原文。
type ShadowReport struct {
	// Command 是本次对照的 mqadmin 子命令名称。
	Command string `json:"command"`
	// Args 是本次对照使用的完整命令参数副本。
	Args []string `json:"args"`
	// Primary 是主路径 provider 的轻量结果摘要。
	Primary ShadowReportTarget `json:"primary"`
	// Targets 是每个旁路 provider 的轻量结果摘要。
	Targets []ShadowReportTarget `json:"targets"`
	// Diffs 是每个旁路 provider 相对主路径的差异摘要。
	Diffs []ShadowReportDiff `json:"diffs"`
}

// ShadowReportTarget 保存单个 provider 的日志安全结果摘要。
type ShadowReportTarget struct {
	// Name 是 provider 名称。
	Name string `json:"name"`
	// StdoutBytes 是 stdout 原文字节数，不记录 stdout 文本。
	StdoutBytes int `json:"stdout_bytes"`
	// StderrBytes 是 stderr 原文字节数，不记录 stderr 文本。
	StderrBytes int `json:"stderr_bytes"`
	// Error 是 provider 返回的错误文本；无错误时为空字符串。
	Error string `json:"error"`
	// DurationMs 是 provider 调用耗时的毫秒数。
	DurationMs int64 `json:"duration_ms"`
}

// ShadowReportDiff 保存单个 target 相对 primary 的 JSONL 差异字段。
type ShadowReportDiff struct {
	// Target 是被比较 provider 名称。
	Target string `json:"target"`
	// Matched 表示 stdout、stderr 与 error 文本全部一致。
	Matched bool `json:"matched"`
	// StdoutDifferent 表示归一化后的 stdout 与 primary 不一致。
	StdoutDifferent bool `json:"stdout_different"`
	// StderrDifferent 表示归一化后的 stderr 与 primary 不一致。
	StderrDifferent bool `json:"stderr_different"`
	// ErrorDifferent 表示 error 文本与 primary 不一致。
	ErrorDifferent bool `json:"error_different"`
	// DurationMs 是 target provider 调用耗时的毫秒数。
	DurationMs int64 `json:"duration_ms"`
}

// ShadowBatchSummaryReport 是批量 shadow compare 的日志安全汇总行，不包含 stdout/stderr 原文。
type ShadowBatchSummaryReport struct {
	// Total 是输入样本总数。
	Total int `json:"total"`
	// Executed 是真实执行的样本数。
	Executed int `json:"executed"`
	// Skipped 是因占位符等原因跳过的样本数。
	Skipped int `json:"skipped"`
	// Errors 是批量计划错误文本；不包含 provider 输出原文。
	Errors []string `json:"errors"`
	// Summary 是已执行结果的聚合统计。
	Summary ShadowBatchSummaryStats `json:"summary"`
}

// ShadowBatchSummaryStats 保存批量 shadow compare 的总体聚合指标。
type ShadowBatchSummaryStats struct {
	// Commands 是参与统计的命令数量。
	Commands int `json:"commands"`
	// Targets 是所有命令下的 provider 对照次数。
	Targets int `json:"targets"`
	// Mismatches 是 stdout/stderr/error 任一不一致的对照次数。
	Mismatches int `json:"mismatches"`
	// ByTarget 按 provider 名称聚合样本数、差异数和耗时毫秒统计。
	ByTarget map[string]ShadowBatchSummaryTargetStats `json:"by_target"`
}

// ShadowBatchSummaryTargetStats 保存单个 provider 的日志安全耗时统计。
type ShadowBatchSummaryTargetStats struct {
	// Samples 是该 provider 参与比较的次数。
	Samples int `json:"samples"`
	// Mismatches 是该 provider 与 primary 不一致的次数。
	Mismatches int `json:"mismatches"`
	// AvgDurationMs 是该 provider 的平均调用耗时毫秒数。
	AvgDurationMs int64 `json:"avg_duration_ms"`
	// MaxDurationMs 是该 provider 的最大调用耗时毫秒数。
	MaxDurationMs int64 `json:"max_duration_ms"`
	// P95DurationMs 是 nearest-rank 算法得到的 p95 调用耗时毫秒数。
	P95DurationMs int64 `json:"p95_duration_ms"`
}

// ShadowBatchPlanReport 是批量 shadow dry-run 计划的日志安全视图，不执行 provider runner。
type ShadowBatchPlanReport struct {
	// Total 是输入样本总数。
	Total int `json:"total"`
	// Executable 是不含占位符、可以真实执行的样本数。
	Executable int `json:"executable"`
	// Skipped 是因占位符等原因会被跳过的样本数。
	Skipped int `json:"skipped"`
	// ExecutableSamples 保存可执行样本的日志安全副本。
	ExecutableSamples []ShadowBatchPlanSampleReport `json:"executable_samples"`
	// SkippedSamples 保存跳过样本、首个占位符和跳过原因。
	SkippedSamples []ShadowBatchPlanSkippedReport `json:"skipped_samples"`
	// Errors 是计划结构错误文本。
	Errors []string `json:"errors"`
}

// ShadowBatchPlanSampleReport 保存单个 dry-run 样本的结构化描述。
type ShadowBatchPlanSampleReport struct {
	// Name 是样本类别名称。
	Name string `json:"name"`
	// Args 是传给 goadmin/mqadmin 的命令参数副本。
	Args []string `json:"args"`
	// Providers 是本样本需要对照的 provider 路径集合。
	Providers []ShadowProviderMode `json:"providers"`
	// MinSamples 是该类别至少需要采集的样本数量。
	MinSamples int `json:"min_samples"`
	// RequireP95 表示该样本需要在后续真实验证中统计 p95 延迟。
	RequireP95 bool `json:"require_p95"`
	// Notes 记录样本选择依据和后续采集注意事项。
	Notes string `json:"notes,omitempty"`
}

// ShadowBatchPlanSkippedReport 保存 dry-run 阶段被跳过的样本和首个占位符原因。
type ShadowBatchPlanSkippedReport struct {
	// Sample 是被跳过样本的日志安全副本。
	Sample ShadowBatchPlanSampleReport `json:"sample"`
	// Reason 是可读跳过原因，便于日志或 UI 直接展示。
	Reason string `json:"reason"`
	// Placeholder 是触发跳过的第一个占位符参数。
	Placeholder string `json:"placeholder"`
}

// NewShadowReport 将完整 ShadowResult 转成日志安全报告，只保留输出大小、错误文本与耗时。
func NewShadowReport(result ShadowResult) ShadowReport {
	report := ShadowReport{
		Command: result.Command,
		Args:    append([]string(nil), result.Args...),
		Primary: newShadowReportTarget(result.Primary),
		Targets: make([]ShadowReportTarget, 0, len(result.Targets)),
		Diffs:   make([]ShadowReportDiff, 0, len(result.Diffs)),
	}
	for _, target := range result.Targets {
		report.Targets = append(report.Targets, newShadowReportTarget(target))
	}
	for _, diff := range result.Diffs {
		report.Diffs = append(report.Diffs, ShadowReportDiff{
			Target:          diff.Target,
			Matched:         diff.Matched,
			StdoutDifferent: diff.StdoutDifferent,
			StderrDifferent: diff.StderrDifferent,
			ErrorDifferent:  diff.ErrorDifferent,
			DurationMs:      diff.Duration.Milliseconds(),
		})
	}
	return report
}

// NewShadowBatchSummaryReport 将批量结果转成单行日志汇总，只保留计数、错误文本和耗时统计。
func NewShadowBatchSummaryReport(result ShadowBatchResult) ShadowBatchSummaryReport {
	return ShadowBatchSummaryReport{
		Total:    result.TotalSamples,
		Executed: result.ExecutedSamples,
		Skipped:  result.SkippedSamples,
		Errors:   newShadowBatchSummaryErrors(result.Errors),
		Summary:  newShadowBatchSummaryStats(result.Summary),
	}
}

// NewShadowBatchPlanReport 将 dry-run 计划转成单行日志可用的结构化报告。
func NewShadowBatchPlanReport(plan ShadowBatchPlan) ShadowBatchPlanReport {
	return ShadowBatchPlanReport{
		Total:             plan.TotalSamples,
		Executable:        plan.ExecutableSamples,
		Skipped:           plan.SkippedSamples,
		ExecutableSamples: newShadowBatchPlanSampleReports(plan.Executable),
		SkippedSamples:    newShadowBatchPlanSkippedReports(plan.Skipped),
		Errors:            newShadowBatchSummaryErrors(plan.Errors),
	}
}

// MarshalShadowReportJSONLine 将 ShadowResult 序列化为单行 JSONL，调用方负责决定写入位置。
func MarshalShadowReportJSONLine(result ShadowResult) ([]byte, error) {
	line, err := json.Marshal(NewShadowReport(result))
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

// MarshalShadowBatchSummaryJSONLine 将批量汇总序列化为单行 JSONL，调用方负责决定写入位置。
func MarshalShadowBatchSummaryJSONLine(result ShadowBatchResult) ([]byte, error) {
	line, err := json.Marshal(NewShadowBatchSummaryReport(result))
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

// MarshalShadowBatchPlanJSONLine 将 dry-run 计划序列化为单行 JSONL，调用方负责决定写入位置。
func MarshalShadowBatchPlanJSONLine(plan ShadowBatchPlan) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(NewShadowBatchPlanReport(plan)); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// MarshalShadowBatchJSONLines 将批量结果按原顺序序列化为多行 JSONL；调用方负责写入日志或文件。
func MarshalShadowBatchJSONLines(result ShadowBatchResult) ([]byte, error) {
	if len(result.Results) == 0 {
		return []byte{}, nil
	}
	var buffer bytes.Buffer
	for _, shadowResult := range result.Results {
		if err := validateShadowReportDiffTargets(shadowResult); err != nil {
			return nil, err
		}
		line, err := MarshalShadowReportJSONLine(shadowResult)
		if err != nil {
			return nil, err
		}
		buffer.Write(line)
	}
	return buffer.Bytes(), nil
}

func newShadowBatchSummaryErrors(errors []error) []string {
	if len(errors) == 0 {
		return []string{}
	}
	messages := make([]string, 0, len(errors))
	for _, err := range errors {
		if err == nil {
			messages = append(messages, "")
			continue
		}
		messages = append(messages, err.Error())
	}
	return messages
}

func newShadowBatchPlanSampleReports(samples []ShadowSample) []ShadowBatchPlanSampleReport {
	if len(samples) == 0 {
		return []ShadowBatchPlanSampleReport{}
	}
	reports := make([]ShadowBatchPlanSampleReport, 0, len(samples))
	for _, sample := range samples {
		reports = append(reports, newShadowBatchPlanSampleReport(sample))
	}
	return reports
}

func newShadowBatchPlanSampleReport(sample ShadowSample) ShadowBatchPlanSampleReport {
	return ShadowBatchPlanSampleReport{
		Name:       sample.Name,
		Args:       append([]string(nil), sample.Args...),
		Providers:  append([]ShadowProviderMode(nil), sample.Providers...),
		MinSamples: sample.MinSamples,
		RequireP95: sample.RequireP95,
		Notes:      sample.Notes,
	}
}

func newShadowBatchPlanSkippedReports(skipped []ShadowSkippedSample) []ShadowBatchPlanSkippedReport {
	if len(skipped) == 0 {
		return []ShadowBatchPlanSkippedReport{}
	}
	reports := make([]ShadowBatchPlanSkippedReport, 0, len(skipped))
	for _, sample := range skipped {
		reports = append(reports, ShadowBatchPlanSkippedReport{
			Sample:      newShadowBatchPlanSampleReport(sample.Sample),
			Reason:      sample.Reason,
			Placeholder: sample.Placeholder,
		})
	}
	return reports
}

func newShadowBatchSummaryStats(summary ShadowSummary) ShadowBatchSummaryStats {
	stats := ShadowBatchSummaryStats{
		Commands:   summary.Commands,
		Targets:    summary.Targets,
		Mismatches: summary.Mismatches,
		ByTarget:   make(map[string]ShadowBatchSummaryTargetStats, len(summary.ByTarget)),
	}
	for target, targetStats := range summary.ByTarget {
		stats.ByTarget[target] = ShadowBatchSummaryTargetStats{
			Samples:       targetStats.Samples,
			Mismatches:    targetStats.Mismatches,
			AvgDurationMs: targetStats.AvgDuration.Milliseconds(),
			MaxDurationMs: targetStats.MaxDuration.Milliseconds(),
			P95DurationMs: targetStats.P95Duration.Milliseconds(),
		}
	}
	return stats
}

func validateShadowReportDiffTargets(result ShadowResult) error {
	targetNames := make(map[string]struct{}, len(result.Targets))
	for _, target := range result.Targets {
		targetNames[target.Name] = struct{}{}
	}
	for _, diff := range result.Diffs {
		if _, ok := targetNames[diff.Target]; !ok {
			return fmt.Errorf("shadow report command %q references unknown target %q; available targets: %v", result.Command, diff.Target, shadowReportTargetNames(result.Targets))
		}
	}
	return nil
}

func shadowReportTargetNames(targets []ShadowTargetResult) []string {
	names := make([]string, 0, len(targets))
	for _, target := range targets {
		names = append(names, target.Name)
	}
	return names
}

func newShadowReportTarget(result ShadowTargetResult) ShadowReportTarget {
	return ShadowReportTarget{
		Name:        result.Name,
		StdoutBytes: len([]byte(result.Stdout)),
		StderrBytes: len([]byte(result.Stderr)),
		Error:       result.Error,
		DurationMs:  result.Duration.Milliseconds(),
	}
}
