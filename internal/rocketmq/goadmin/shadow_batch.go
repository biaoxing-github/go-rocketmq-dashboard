package goadmin

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// ShadowBatch 描述一次 M6 批量 shadow compare 执行计划，调用方负责提供真实样本参数。
type ShadowBatch struct {
	// Primary 是用户主路径 provider，通常是 official、process、sidecar、native 或 auto。
	Primary ShadowTarget
	// Targets 是需要与主路径对比的旁路 provider 集合。
	Targets []ShadowTarget
	// Samples 是本次批量执行的样本清单；包含占位符的样本只统计为跳过，不会真实执行。
	Samples []ShadowSample
	// Normalizer 用于屏蔽时间戳、耗时、offset 等动态字段。
	Normalizer ShadowNormalizer
	// MaxConcurrency 控制 concrete 样本的最大并发数；小于等于 1 时按顺序执行。
	MaxConcurrency int
}

// ShadowBatchResult 保存一次批量 shadow compare 的执行结果和聚合统计。
type ShadowBatchResult struct {
	// TotalSamples 是输入样本总数。
	TotalSamples int
	// ExecutedSamples 是真实执行的样本数。
	ExecutedSamples int
	// SkippedSamples 是因占位符等原因跳过的样本数。
	SkippedSamples int
	// Results 是每个已执行样本的 shadow compare 明细。
	Results []ShadowResult
	// Summary 是 Results 的聚合统计。
	Summary ShadowSummary
	// Errors 是计划结构错误；runner 自身错误保留在 ShadowResult 内，不进入这里。
	Errors []error
}

// ShadowBatchPlan 是批量执行前的 dry-run 视图，用于确认哪些样本仍需填充真实参数。
type ShadowBatchPlan struct {
	// TotalSamples 是输入样本总数。
	TotalSamples int
	// ExecutableSamples 是不含占位符、可以真实执行的样本数。
	ExecutableSamples int
	// SkippedSamples 是因占位符等原因会被跳过的样本数。
	SkippedSamples int
	// Executable 保存可执行样本副本，调用方修改返回值不会影响原始计划。
	Executable []ShadowSample
	// Skipped 保存每个跳过样本及其原因。
	Skipped []ShadowSkippedSample
	// Errors 是计划结构错误；存在错误时不再分类样本。
	Errors []error
}

// ShadowSkippedSample 描述一个 dry-run 阶段被跳过的样本和首个占位符原因。
type ShadowSkippedSample struct {
	// Sample 是被跳过的样本副本。
	Sample ShadowSample
	// Reason 是可读跳过原因，便于日志或 UI 直接展示。
	Reason string
	// Placeholder 是触发跳过的第一个占位符参数。
	Placeholder string
}

// PlanShadowBatch 只分析 shadow 样本计划，不执行任何 provider runner。
func PlanShadowBatch(samples []ShadowSample) ShadowBatchPlan {
	plan := ShadowBatchPlan{
		TotalSamples: len(samples),
	}
	if err := ValidateShadowPlan(samples); err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	for _, sample := range samples {
		placeholder := shadowSamplePlaceholder(sample)
		if placeholder != "" {
			plan.SkippedSamples++
			plan.Skipped = append(plan.Skipped, ShadowSkippedSample{
				Sample:      cloneShadowSample(sample),
				Placeholder: placeholder,
				Reason:      fmt.Sprintf("sample %q contains placeholder %q", sample.Name, placeholder),
			})
			continue
		}
		plan.ExecutableSamples++
		plan.Executable = append(plan.Executable, cloneShadowSample(sample))
	}
	return plan
}

// RunShadowBatch 按顺序执行一批 concrete shadow 样本，并跳过仍含模板占位符的样本。
func RunShadowBatch(ctx context.Context, batch ShadowBatch) ShadowBatchResult {
	result := ShadowBatchResult{
		TotalSamples: len(batch.Samples),
		Results:      make([]ShadowResult, 0, len(batch.Samples)),
	}
	if err := ValidateShadowPlan(batch.Samples); err != nil {
		result.Errors = append(result.Errors, err)
		return result
	}
	for _, sample := range batch.Samples {
		if shadowSampleHasPlaceholder(sample) {
			result.SkippedSamples++
			continue
		}
		result.Results = append(result.Results, ShadowResult{})
		result.ExecutedSamples++
	}
	runConcreteShadowSamples(ctx, batch, result.Results)
	result.Summary = SummarizeShadowResults(result.Results)
	return result
}

func runConcreteShadowSamples(ctx context.Context, batch ShadowBatch, results []ShadowResult) {
	type shadowBatchJob struct {
		resultIndex int
		sample      ShadowSample
	}
	jobs := make([]shadowBatchJob, 0, len(results))
	resultIndex := 0
	for _, sample := range batch.Samples {
		if shadowSampleHasPlaceholder(sample) {
			continue
		}
		jobs = append(jobs, shadowBatchJob{resultIndex: resultIndex, sample: sample})
		resultIndex++
	}
	if len(jobs) == 0 {
		return
	}
	workers := batch.MaxConcurrency
	if workers <= 1 {
		for _, job := range jobs {
			results[job.resultIndex] = runShadowBatchSample(ctx, batch, job.sample)
		}
		return
	}
	if workers > len(jobs) {
		workers = len(jobs)
	}
	jobCh := make(chan shadowBatchJob)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				results[job.resultIndex] = runShadowBatchSample(ctx, batch, job.sample)
			}
		}()
	}
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)
	wg.Wait()
}

func runShadowBatchSample(ctx context.Context, batch ShadowBatch, sample ShadowSample) ShadowResult {
	return RunShadowCompare(ctx, sample.Args, batch.Primary, shadowTargetsForSample(batch.Targets, sample), batch.Normalizer)
}

func shadowSampleHasPlaceholder(sample ShadowSample) bool {
	return shadowSamplePlaceholder(sample) != ""
}

func shadowSamplePlaceholder(sample ShadowSample) string {
	for _, arg := range sample.Args {
		value := strings.TrimSpace(arg)
		if strings.HasPrefix(value, "<") && strings.HasSuffix(value, ">") {
			return value
		}
	}
	return ""
}

func cloneShadowSample(sample ShadowSample) ShadowSample {
	cloned := sample
	cloned.Args = append([]string(nil), sample.Args...)
	cloned.Providers = append([]ShadowProviderMode(nil), sample.Providers...)
	return cloned
}

func shadowTargetsForSample(targets []ShadowTarget, sample ShadowSample) []ShadowTarget {
	if len(targets) == 0 || len(sample.Providers) == 0 {
		return nil
	}
	enabled := make(map[string]bool, len(sample.Providers))
	for _, provider := range sample.Providers {
		enabled[string(provider)] = true
	}
	filtered := make([]ShadowTarget, 0, len(targets))
	for _, target := range targets {
		if enabled[target.Name] {
			filtered = append(filtered, target)
		}
	}
	return filtered
}
