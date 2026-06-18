package goadmin

import (
	"sort"
	"time"
)

// ShadowSummary 汇总一批 shadow compare 结果，供 M6 性能表和日志聚合使用。
type ShadowSummary struct {
	// Commands 是参与统计的命令数量。
	Commands int
	// Targets 是所有命令下的 provider 对照次数。
	Targets int
	// Mismatches 是 stdout/stderr/error 任一不一致的对照次数。
	Mismatches int
	// ByTarget 按 provider 名称聚合样本数、差异数和耗时统计。
	ByTarget map[string]ShadowTargetStats
}

// ShadowTargetStats 保存单个 provider 的 shadow compare 聚合指标。
type ShadowTargetStats struct {
	// Samples 是该 provider 参与比较的次数。
	Samples int
	// Mismatches 是该 provider 与 primary 不一致的次数。
	Mismatches int
	// AvgDuration 是该 provider 的平均调用耗时。
	AvgDuration time.Duration
	// MaxDuration 是该 provider 的最大调用耗时。
	MaxDuration time.Duration
	// P95Duration 是 nearest-rank 算法得到的 p95 调用耗时。
	P95Duration time.Duration
}

type shadowTargetAccumulator struct {
	stats     ShadowTargetStats
	total     time.Duration
	durations []time.Duration
}

// SummarizeShadowResults 聚合多次 shadow compare 结果；它只读入参，不执行任何真实命令。
func SummarizeShadowResults(results []ShadowResult) ShadowSummary {
	accumulators := make(map[string]*shadowTargetAccumulator)
	summary := ShadowSummary{
		Commands: len(results),
		ByTarget: make(map[string]ShadowTargetStats),
	}
	for _, result := range results {
		for _, diff := range result.Diffs {
			summary.Targets++
			if !diff.Matched {
				summary.Mismatches++
			}
			accumulator := accumulators[diff.Target]
			if accumulator == nil {
				accumulator = &shadowTargetAccumulator{}
				accumulators[diff.Target] = accumulator
			}
			accumulator.stats.Samples++
			if !diff.Matched {
				accumulator.stats.Mismatches++
			}
			accumulator.total += diff.Duration
			if diff.Duration > accumulator.stats.MaxDuration {
				accumulator.stats.MaxDuration = diff.Duration
			}
			accumulator.durations = append(accumulator.durations, diff.Duration)
		}
	}
	for target, accumulator := range accumulators {
		stats := accumulator.stats
		if stats.Samples > 0 {
			stats.AvgDuration = accumulator.total / time.Duration(stats.Samples)
		}
		stats.P95Duration = shadowP95Duration(accumulator.durations)
		summary.ByTarget[target] = stats
	}
	return summary
}

func shadowP95Duration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i int, j int) bool {
		return sorted[i] < sorted[j]
	})
	index := (95*len(sorted) + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}
