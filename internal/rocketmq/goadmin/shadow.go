package goadmin

import (
	"context"
	"errors"
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
	// Artifacts 保存命令生成的文件产物正文，key 使用稳定的相对文件名。
	Artifacts map[string]string
}

// ShadowTarget 描述一次 shadow compare 中的命名 provider。
type ShadowTarget struct {
	// Name 是差异报告中的 provider 名称。
	Name string
	// Runner 执行同一组 goadmin command args 并返回输出。
	Runner ShadowRunner
	// BeforeRun 在 provider 调用前执行，用于为会修改共享状态的命令恢复同一初始条件。
	BeforeRun func(context.Context, []string) error
	// AfterRun 在 provider 调用后执行，用于清理本次调用留下的共享状态。
	AfterRun func(context.Context, []string) error
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
		output.Artifacts = replaceShadowArtifactText(output.Artifacts, func(value string) string {
			return strings.ReplaceAll(value, old, new)
		})
		return output
	}
}

// ReplaceShadowRegexp 使用标准库 regexp 在 stdout 和 stderr 中替换动态片段，pattern 非法时按 MustCompile 立即失败。
func ReplaceShadowRegexp(pattern string, repl string) ShadowNormalizer {
	re := regexp.MustCompile(pattern)
	return func(output ShadowOutput) ShadowOutput {
		output.Stdout = re.ReplaceAllString(output.Stdout, repl)
		output.Stderr = re.ReplaceAllString(output.Stderr, repl)
		output.Artifacts = replaceShadowArtifactText(output.Artifacts, func(value string) string {
			return re.ReplaceAllString(value, repl)
		})
		return output
	}
}

// DefaultM6ShadowNormalizer 屏蔽批量 shadow 验证里最常见的时间、耗时和 offset 动态字段。
func DefaultM6ShadowNormalizer() ShadowNormalizer {
	return ComposeShadowNormalizers(
		ReplaceShadowRegexp(`\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\b`, "<datetime>"),
		ReplaceShadowRegexp(`\b(timestamp|bornTimestamp|storeTimestamp)=[0-9]+`, `$1=<dynamic>`),
		ReplaceShadowRegexp(`("exportTime"\s*:\s*)[0-9]+`, `${1}<dynamic>`),
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
		output.Artifacts = replaceShadowArtifactText(output.Artifacts, func(value string) string {
			return normalizeShadowTextLines(value, fn)
		})
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
	// Artifacts 是本次命令读取到的文件产物正文。
	Artifacts map[string]string
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
	// ArtifactsDifferent 表示归一化后的文件产物与 primary 不一致。
	ArtifactsDifferent bool
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
	return RunShadowCompareWithOptions(ctx, args, primary, targets, normalizer, false)
}

// RunShadowCompareWithOptions 支持为单个样本显式控制旁路 provider 是否串行执行。
func RunShadowCompareWithOptions(ctx context.Context, args []string, primary ShadowTarget, targets []ShadowTarget, normalizer ShadowNormalizer, serialTargets bool) ShadowResult {
	command := shadowCommand(args)
	result := ShadowResult{
		Command: command,
		Args:    append([]string(nil), args...),
		Targets: make([]ShadowTargetResult, len(targets)),
		Diffs:   make([]ShadowDiff, len(targets)),
	}
	primaryResult, primaryOutput := runShadowTarget(ctx, args, primary)
	result.Primary = primaryResult
	normalizedPrimary := normalizeShadowOutputForCommand(command, primaryOutput, normalizer)

	runShadowTargetComparisons(ctx, args, command, targets, normalizedPrimary, normalizer, result.Targets, result.Diffs, serialTargets)
	return result
}

// runShadowTargetComparisons 并发执行所有旁路 provider，对外仍按 targets 入参顺序写回报告。
func runShadowTargetComparisons(ctx context.Context, args []string, command string, targets []ShadowTarget, normalizedPrimary shadowComparableOutput, normalizer ShadowNormalizer, targetResults []ShadowTargetResult, diffs []ShadowDiff, serialTargets bool) {
	if len(targets) == 0 {
		return
	}
	if serialTargets || len(targets) == 1 {
		for index, target := range targets {
			targetResults[index], diffs[index] = runShadowTargetComparison(ctx, args, command, target, normalizedPrimary, normalizer)
		}
		return
	}
	var wg sync.WaitGroup
	for index, target := range targets {
		index, target := index, target
		wg.Add(1)
		go func() {
			defer wg.Done()
			targetResults[index], diffs[index] = runShadowTargetComparison(ctx, args, command, target, normalizedPrimary, normalizer)
		}()
	}
	wg.Wait()
}

// runShadowTargetComparison 执行单个旁路 provider 并生成它相对 primary 的归一化差异。
func runShadowTargetComparison(ctx context.Context, args []string, command string, target ShadowTarget, normalizedPrimary shadowComparableOutput, normalizer ShadowNormalizer) (ShadowTargetResult, ShadowDiff) {
	targetResult, targetOutput := runShadowTarget(ctx, args, target)
	diff := compareShadowOutput(normalizedPrimary, normalizeShadowOutputForCommand(command, targetOutput, normalizer))
	diff.Target = target.Name
	diff.Duration = targetResult.Duration
	return targetResult, diff
}

func runShadowTarget(ctx context.Context, args []string, target ShadowTarget) (ShadowTargetResult, shadowComparableOutput) {
	if target.BeforeRun != nil {
		if err := target.BeforeRun(ctx, append([]string(nil), args...)); err != nil {
			errText := shadowErrorText(err)
			return ShadowTargetResult{
				Name:  target.Name,
				Error: errText,
			}, shadowComparableOutput{Error: errText}
		}
	}
	startedAt := time.Now()
	output, err := target.Runner.RunShadow(ctx, append([]string(nil), args...))
	duration := time.Since(startedAt)
	if target.AfterRun != nil {
		if afterErr := target.AfterRun(ctx, append([]string(nil), args...)); afterErr != nil {
			if err == nil {
				err = afterErr
			} else {
				err = errors.Join(err, afterErr)
			}
		}
	}
	errText := shadowErrorText(err)
	return ShadowTargetResult{
			Name:      target.Name,
			Stdout:    output.Stdout,
			Stderr:    output.Stderr,
			Artifacts: cloneShadowArtifacts(output.Artifacts),
			Error:     errText,
			Duration:  duration,
		}, shadowComparableOutput{
			Stdout:    output.Stdout,
			Stderr:    output.Stderr,
			Artifacts: cloneShadowArtifacts(output.Artifacts),
			Error:     errText,
		}
}

type shadowComparableOutput struct {
	Stdout    string
	Stderr    string
	Artifacts map[string]string
	Error     string
}

func normalizeShadowOutput(output shadowComparableOutput, normalizer ShadowNormalizer) shadowComparableOutput {
	if normalizer == nil {
		return output
	}
	normalized := normalizer(ShadowOutput{
		Stdout:    output.Stdout,
		Stderr:    output.Stderr,
		Artifacts: cloneShadowArtifacts(output.Artifacts),
	})
	output.Stdout = normalized.Stdout
	output.Stderr = normalized.Stderr
	output.Artifacts = cloneShadowArtifacts(normalized.Artifacts)
	return output
}

func normalizeShadowOutputForCommand(command string, output shadowComparableOutput, normalizer ShadowNormalizer) shadowComparableOutput {
	output = normalizeShadowOutput(output, normalizer)
	if normalizer == nil {
		return output
	}
	if command == "brokerStatus" {
		normalized := normalizeBrokerStatusShadowOutput(ShadowOutput{
			Stdout: output.Stdout,
			Stderr: output.Stderr,
		})
		output.Stdout = normalized.Stdout
		output.Stderr = normalized.Stderr
	}
	if command == "producer" {
		normalized := normalizeProducerShadowOutput(ShadowOutput{
			Stdout: output.Stdout,
			Stderr: output.Stderr,
		})
		output.Stdout = normalized.Stdout
		output.Stderr = normalized.Stderr
	}
	return output
}

// normalizeBrokerStatusShadowOutput 仅处理 brokerStatus 官方 KV 行里的运行态数值，避免污染其他命令的严格差异判断。
func normalizeBrokerStatusShadowOutput(output ShadowOutput) ShadowOutput {
	output.Stdout = normalizeShadowTextLines(output.Stdout, normalizeBrokerStatusShadowLine)
	return output
}

func normalizeBrokerStatusShadowLine(line string) string {
	searchStart := 0
	for {
		index := strings.Index(line[searchStart:], ": ")
		if index < 0 {
			return line
		}
		index += searchStart
		key := brokerStatusShadowKey(line[:index])
		if isBrokerStatusDynamicShadowKey(key) {
			return line[:index+2] + "<dynamic>"
		}
		searchStart = index + 2
	}
}

func brokerStatusShadowKey(prefix string) string {
	fields := strings.Fields(prefix)
	if len(fields) == 0 {
		return ""
	}
	return fields[len(fields)-1]
}

func isBrokerStatusDynamicShadowKey(key string) bool {
	return key == "runtime" || key == "timerReadBehind" || strings.HasSuffix(key, "Tps") || strings.HasSuffix(key, "TPS") || strings.HasSuffix(key, "Throughput")
}

var producerLastUpdateTimestampPattern = regexp.MustCompile(`\blastUpdateTimestamp=[0-9]+`)

// normalizeProducerShadowOutput 仅屏蔽 producer 在线连接表里的刷新时间，保留 group/client/version 等身份字段。
func normalizeProducerShadowOutput(output ShadowOutput) ShadowOutput {
	output.Stdout = producerLastUpdateTimestampPattern.ReplaceAllString(output.Stdout, "lastUpdateTimestamp=<dynamic>")
	output.Stderr = producerLastUpdateTimestampPattern.ReplaceAllString(output.Stderr, "lastUpdateTimestamp=<dynamic>")
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
		StdoutDifferent:    primary.Stdout != target.Stdout,
		StderrDifferent:    primary.Stderr != target.Stderr,
		ErrorDifferent:     primary.Error != target.Error,
		ArtifactsDifferent: !equalShadowArtifacts(primary.Artifacts, target.Artifacts),
	}
	diff.Matched = !diff.StdoutDifferent && !diff.StderrDifferent && !diff.ErrorDifferent && !diff.ArtifactsDifferent
	return diff
}

func replaceShadowArtifactText(artifacts map[string]string, replace func(string) string) map[string]string {
	if len(artifacts) == 0 || replace == nil {
		return artifacts
	}
	replaced := make(map[string]string, len(artifacts))
	for name, value := range artifacts {
		replaced[name] = replace(value)
	}
	return replaced
}

func cloneShadowArtifacts(artifacts map[string]string) map[string]string {
	if len(artifacts) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(artifacts))
	for name, value := range artifacts {
		cloned[name] = value
	}
	return cloned
}

func equalShadowArtifacts(left map[string]string, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for name, value := range left {
		if right[name] != value {
			return false
		}
	}
	return true
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
