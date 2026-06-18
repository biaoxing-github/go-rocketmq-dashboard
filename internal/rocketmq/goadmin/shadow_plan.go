package goadmin

import (
	"fmt"
	"strings"
)

// ShadowProviderMode 表示 M6 shadow 验证计划中的 provider 执行路径。
type ShadowProviderMode string

const (
	// ShadowProviderOfficial 表示官方 mqadmin JVM 命令路径，作为差异比较基准。
	ShadowProviderOfficial ShadowProviderMode = "official"
	// ShadowProviderSidecar 表示常驻 JVM sidecar 路径，用于验证热进程输出一致性。
	ShadowProviderSidecar ShadowProviderMode = "sidecar"
	// ShadowProviderNative 表示 Go native remoting 路径，用于验证高性能原生实现。
	ShadowProviderNative ShadowProviderMode = "native"
	// ShadowProviderAuto 表示 auto provider 选择路径，用于验证生产默认路由效果。
	ShadowProviderAuto ShadowProviderMode = "auto"
)

// ShadowSample 描述 M6 后续批量 shadow/provider 验证的一类样本，不直接执行真实命令。
type ShadowSample struct {
	// Name 是样本类别名称，例如 command-smoke 或 message-chain-warm。
	Name string
	// Args 是传给 goadmin/mqadmin 的命令参数模板。
	Args []string
	// Providers 是本样本需要对照的 provider 路径集合。
	Providers []ShadowProviderMode
	// MinSamples 是该类别至少需要采集的样本数量。
	MinSamples int
	// RequireP95 表示该样本需要在后续真实验证中统计 p95 延迟。
	RequireP95 bool
	// Notes 记录样本选择依据和后续采集注意事项。
	Notes string
}

// ShadowFixtureOverrides 保存 M6 dry-run 样本的真实参数覆盖列表，用于把默认模板样本转换成可执行样本。
type ShadowFixtureOverrides struct {
	// Samples 是按默认样本名称匹配的参数覆盖；同一名称可出现多次以展开多组真实样本。
	Samples []ShadowSampleFixture `json:"samples"`
}

// ShadowSampleFixture 描述一个默认 shadow 样本的完整命令参数覆盖。
type ShadowSampleFixture struct {
	// Name 是要覆盖的默认样本名称，例如 known-message 或 message-chain-warm。
	Name string `json:"name"`
	// Args 是完整 goadmin/mqadmin 命令参数；覆盖后不能再包含 <...> 占位符才会被 dry-run 判为 executable。
	Args []string `json:"args"`
}

var defaultM6ShadowSamples = []ShadowSample{
	{
		Name:       "command-smoke",
		Args:       []string{"<command>", "<args>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 93,
		Notes:      "覆盖已枚举的 93 个官方命令名，只验证退出码和 stdout/stderr 基线，不在计划层执行命令。",
	},
	{
		Name:       "known-message",
		Args:       []string{"queryMsgById", "-i", "<known-message-id>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 93,
		Notes:      "对已知消息样本做 official/sidecar/native/auto 输出差异比较。",
	},
	{
		Name:       "recent-topic-message",
		Args:       []string{"queryMsgByKey", "-t", "<topic>", "-k", "<recent-message-key>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		Notes:      "抽取最近 Topic 消息，验证动态消息查询输出在四路 provider 下保持一致。",
	},
	{
		Name:       "message-chain-cold",
		Args:       []string{"messageChain", "-t", "<topic>", "-k", "<message-key>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "冷路径样本用于统计 /api/message-chain 首次查询延迟和输出一致性。",
	},
	{
		Name:       "message-chain-warm",
		Args:       []string{"messageChain", "-t", "<topic>", "-k", "<message-key>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "热路径样本用于统计 /api/message-chain 复用 provider 后的 p95 延迟。",
	},
}

// DefaultM6ShadowPlan 返回 M6 批量验证的默认样本计划副本，调用方修改返回值不会污染全局模板。
func DefaultM6ShadowPlan() []ShadowSample {
	return cloneShadowSamples(defaultM6ShadowSamples)
}

// ApplyShadowFixtureOverrides 将真实 fixture 参数合入默认 shadow 样本；未提供覆盖的样本仍保留占位模板。
func ApplyShadowFixtureOverrides(samples []ShadowSample, overrides ShadowFixtureOverrides) ([]ShadowSample, error) {
	base := cloneShadowSamples(samples)
	if len(overrides.Samples) == 0 {
		return base, nil
	}

	templates := make(map[string]ShadowSample, len(base))
	for _, sample := range base {
		templates[sample.Name] = sample
	}
	grouped := make(map[string][]ShadowSampleFixture, len(overrides.Samples))
	for index, fixture := range overrides.Samples {
		name := strings.TrimSpace(fixture.Name)
		if name == "" {
			return nil, fmt.Errorf("shadow fixture %d name is empty", index)
		}
		if len(fixture.Args) == 0 {
			return nil, fmt.Errorf("shadow fixture %q args is empty", name)
		}
		if _, ok := templates[name]; !ok {
			return nil, fmt.Errorf("shadow fixture %q does not match any default sample", name)
		}
		fixture.Name = name
		fixture.Args = append([]string(nil), fixture.Args...)
		grouped[name] = append(grouped[name], fixture)
	}

	merged := make([]ShadowSample, 0, len(base)+len(overrides.Samples))
	for _, sample := range base {
		fixtures := grouped[sample.Name]
		if len(fixtures) == 0 {
			merged = append(merged, sample)
			continue
		}
		for _, fixture := range fixtures {
			concrete := sample
			concrete.Args = append([]string(nil), fixture.Args...)
			merged = append(merged, concrete)
		}
	}
	return merged, nil
}

// ValidateShadowPlan 检查 shadow 样本计划是否满足 M6 批量验证的最低结构约束。
func ValidateShadowPlan(samples []ShadowSample) error {
	for index, sample := range samples {
		name := strings.TrimSpace(sample.Name)
		if name == "" {
			return fmt.Errorf("shadow sample %d name is empty", index)
		}
		if len(sample.Args) == 0 {
			return fmt.Errorf("shadow sample %q args is empty", name)
		}
		if err := validateShadowProviders(name, sample.Providers); err != nil {
			return err
		}
		if sample.MinSamples <= 0 {
			return fmt.Errorf("shadow sample %q MinSamples must be greater than 0", name)
		}
		if sample.RequireP95 && sample.MinSamples < 20 {
			return fmt.Errorf("shadow sample %q MinSamples must be at least 20 when RequireP95 is true", name)
		}
	}
	return nil
}

func validateShadowProviders(sampleName string, providers []ShadowProviderMode) error {
	hasOfficial := false
	hasShadowProvider := false
	for _, provider := range providers {
		switch provider {
		case ShadowProviderOfficial:
			hasOfficial = true
		case ShadowProviderSidecar, ShadowProviderNative, ShadowProviderAuto:
			hasShadowProvider = true
		}
	}
	if !hasOfficial {
		return fmt.Errorf("shadow sample %q providers must include official", sampleName)
	}
	if !hasShadowProvider {
		return fmt.Errorf("shadow sample %q providers must include at least one shadow provider", sampleName)
	}
	return nil
}

func defaultShadowProviders() []ShadowProviderMode {
	return []ShadowProviderMode{
		ShadowProviderOfficial,
		ShadowProviderSidecar,
		ShadowProviderNative,
		ShadowProviderAuto,
	}
}

func cloneShadowSamples(samples []ShadowSample) []ShadowSample {
	cloned := make([]ShadowSample, len(samples))
	for i, sample := range samples {
		cloned[i] = sample
		cloned[i].Args = append([]string(nil), sample.Args...)
		cloned[i].Providers = append([]ShadowProviderMode(nil), sample.Providers...)
	}
	return cloned
}
