package goadmin

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ShadowRunner 表示一个可被 shadow compare 调用的 provider，例如 official、sidecar 或 native。
type ShadowRunner interface {
	RunShadow(ctx context.Context, args []string) (ShadowOutput, error)
}

// ShadowOutput 保存一次 provider 调用产生的可比较输出。
type ShadowOutput struct {
	// Stdout 是 provider 标准输出文本，默认按字节级字符串严格比较。
	Stdout string
	// Stderr 是 provider 标准错误文本，默认按字节级字符串严格比较。
	Stderr string
}

// ShadowTarget 描述一次 shadow compare 中的命名 provider。
type ShadowTarget struct {
	// Name 是差异报告中的 provider 名称。
	Name string
	// Runner 执行同一组 goadmin command args 并返回输出。
	Runner ShadowRunner
}

// ShadowNormalizer 用于在比较前消除时间戳、耗时、offset 等动态字段。
type ShadowNormalizer func(ShadowOutput) ShadowOutput

// ComposeShadowNormalizers 按传入顺序串联多个归一化器，nil 归一化器会被跳过。
func ComposeShadowNormalizers(normalizers ...ShadowNormalizer) ShadowNormalizer {
	return func(output ShadowOutput) ShadowOutput {
		for _, normalizer := range normalizers {
			if normalizer == nil {
				continue
			}
			output = normalizer(output)
		}
		return output
	}
}

// ReplaceShadowText 在 stdout 和 stderr 中执行相同文本替换，用于屏蔽耗时、时间戳等动态片段。
func ReplaceShadowText(old string, new string) ShadowNormalizer {
	return func(output ShadowOutput) ShadowOutput {
		output.Stdout = strings.ReplaceAll(output.Stdout, old, new)
		output.Stderr = strings.ReplaceAll(output.Stderr, old, new)
		return output
	}
}

// ReplaceShadowRegexp 使用标准库 regexp 在 stdout 和 stderr 中替换动态片段，pattern 非法时按 MustCompile 立即失败。
func ReplaceShadowRegexp(pattern string, repl string) ShadowNormalizer {
	re := regexp.MustCompile(pattern)
	return func(output ShadowOutput) ShadowOutput {
		output.Stdout = re.ReplaceAllString(output.Stdout, repl)
		output.Stderr = re.ReplaceAllString(output.Stderr, repl)
		return output
	}
}

// DefaultM6ShadowNormalizer 屏蔽批量 shadow 验证里最常见的时间、耗时和 offset 动态字段。
func DefaultM6ShadowNormalizer() ShadowNormalizer {
	return ComposeShadowNormalizers(
		ReplaceShadowRegexp(`\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\b`, "<datetime>"),
		ReplaceShadowRegexp(`\b(timestamp|bornTimestamp|storeTimestamp)=[0-9]+`, `$1=<dynamic>`),
		ReplaceShadowRegexp(`\b(cost|elapsed|duration|took)=[0-9]+ms`, `$1=<duration>`),
		ReplaceShadowRegexp(`\btook [0-9]+ms\b`, "took <duration>"),
		ReplaceShadowRegexp(`\b(queueOffset|commitOffset|consumerOffset|brokerOffset)=[0-9]+`, `$1=<offset>`),
	)
}

// NormalizeShadowLines 对 stdout 和 stderr 的每一行分别执行归一化，保留原始换行结构。
func NormalizeShadowLines(fn func(string) string) ShadowNormalizer {
	return func(output ShadowOutput) ShadowOutput {
		if fn == nil {
			return output
		}
		output.Stdout = normalizeShadowTextLines(output.Stdout, fn)
		output.Stderr = normalizeShadowTextLines(output.Stderr, fn)
		return output
	}
}

// ShadowTargetResult 保存单个 provider 的原始调用结果。
type ShadowTargetResult struct {
	// Name 是 provider 名称。
	Name string
	// Stdout 是原始标准输出文本。
	Stdout string
	// Stderr 是原始标准错误文本。
	Stderr string
	// Error 是 err.Error() 文本；nil error 记录为空字符串。
	Error string
	// Duration 是 provider 调用耗时。
	Duration time.Duration
}

// ShadowDiff 保存 target 相对 primary 的归一化比较结论。
type ShadowDiff struct {
	// Target 是被比较 provider 名称。
	Target string
	// Matched 表示 stdout、stderr 与 error 文本全部一致。
	Matched bool
	// StdoutDifferent 表示归一化后的 stdout 与 primary 不一致。
	StdoutDifferent bool
	// StderrDifferent 表示归一化后的 stderr 与 primary 不一致。
	StderrDifferent bool
	// ErrorDifferent 表示 error 文本与 primary 不一致。
	ErrorDifferent bool
	// Duration 是 target provider 调用耗时。
	Duration time.Duration
}

// ShadowResult 是一次命令 shadow compare 的完整结果。
type ShadowResult struct {
	// Command 是 args 中的第一个元素，便于日志按 mqadmin 子命令聚合。
	Command string
	// Args 是本次传给所有 provider 的命令参数副本。
	Args []string
	// Primary 是用户路径 provider 的原始输出。
	Primary ShadowTargetResult
	// Targets 是所有旁路 provider 的原始输出。
	Targets []ShadowTargetResult
	// Diffs 是所有旁路 provider 相对 Primary 的差异结论。
	Diffs []ShadowDiff
}

// RunShadowCompare 对同一组 command args 执行 primary 和多个旁路 provider，并返回严格差异报告。
func RunShadowCompare(ctx context.Context, args []string, primary ShadowTarget, targets []ShadowTarget, normalizer ShadowNormalizer) ShadowResult {
	result := ShadowResult{
		Command: shadowCommand(args),
		Args:    append([]string(nil), args...),
		Targets: make([]ShadowTargetResult, len(targets)),
		Diffs:   make([]ShadowDiff, len(targets)),
	}
	primaryResult, primaryOutput := runShadowTarget(ctx, args, primary)
	result.Primary = primaryResult
	normalizedPrimary := normalizeShadowOutput(primaryOutput, normalizer)

	runShadowTargetComparisons(ctx, args, targets, normalizedPrimary, normalizer, result.Targets, result.Diffs)
	return result
}

// runShadowTargetComparisons 并发执行所有旁路 provider，对外仍按 targets 入参顺序写回报告。
func runShadowTargetComparisons(ctx context.Context, args []string, targets []ShadowTarget, normalizedPrimary shadowComparableOutput, normalizer ShadowNormalizer, targetResults []ShadowTargetResult, diffs []ShadowDiff) {
	if len(targets) == 0 {
		return
	}
	if len(targets) == 1 {
		targetResults[0], diffs[0] = runShadowTargetComparison(ctx, args, targets[0], normalizedPrimary, normalizer)
		return
	}
	var wg sync.WaitGroup
	for index, target := range targets {
		index, target := index, target
		wg.Add(1)
		go func() {
			defer wg.Done()
			targetResults[index], diffs[index] = runShadowTargetComparison(ctx, args, target, normalizedPrimary, normalizer)
		}()
	}
	wg.Wait()
}

// runShadowTargetComparison 执行单个旁路 provider 并生成它相对 primary 的归一化差异。
func runShadowTargetComparison(ctx context.Context, args []string, target ShadowTarget, normalizedPrimary shadowComparableOutput, normalizer ShadowNormalizer) (ShadowTargetResult, ShadowDiff) {
	targetResult, targetOutput := runShadowTarget(ctx, args, target)
	diff := compareShadowOutput(normalizedPrimary, normalizeShadowOutput(targetOutput, normalizer))
	diff.Target = target.Name
	diff.Duration = targetResult.Duration
	return targetResult, diff
}

func runShadowTarget(ctx context.Context, args []string, target ShadowTarget) (ShadowTargetResult, shadowComparableOutput) {
	startedAt := time.Now()
	output, err := target.Runner.RunShadow(ctx, append([]string(nil), args...))
	duration := time.Since(startedAt)
	errText := shadowErrorText(err)
	return ShadowTargetResult{
			Name:     target.Name,
			Stdout:   output.Stdout,
			Stderr:   output.Stderr,
			Error:    errText,
			Duration: duration,
		}, shadowComparableOutput{
			Stdout: output.Stdout,
			Stderr: output.Stderr,
			Error:  errText,
		}
}

type shadowComparableOutput struct {
	Stdout string
	Stderr string
	Error  string
}

func normalizeShadowOutput(output shadowComparableOutput, normalizer ShadowNormalizer) shadowComparableOutput {
	if normalizer == nil {
		return output
	}
	normalized := normalizer(ShadowOutput{
		Stdout: output.Stdout,
		Stderr: output.Stderr,
	})
	output.Stdout = normalized.Stdout
	output.Stderr = normalized.Stderr
	return output
}

func normalizeShadowTextLines(text string, fn func(string) string) string {
	if text == "" {
		return ""
	}
	parts := strings.SplitAfter(text, "\n")
	for i, part := range parts {
		if part == "" {
			continue
		}
		if strings.HasSuffix(part, "\n") {
			parts[i] = fn(strings.TrimSuffix(part, "\n")) + "\n"
			continue
		}
		parts[i] = fn(part)
	}
	return strings.Join(parts, "")
}

func compareShadowOutput(primary shadowComparableOutput, target shadowComparableOutput) ShadowDiff {
	diff := ShadowDiff{
		StdoutDifferent: primary.Stdout != target.Stdout,
		StderrDifferent: primary.Stderr != target.Stderr,
		ErrorDifferent:  primary.Error != target.Error,
	}
	diff.Matched = !diff.StdoutDifferent && !diff.StderrDifferent && !diff.ErrorDifferent
	return diff
}

func shadowCommand(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

func shadowErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
