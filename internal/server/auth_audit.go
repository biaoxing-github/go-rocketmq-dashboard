package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Permission 描述一个需要显式授权的运营操作类别。
type Permission string

const (
	// PermissionTopicWrite 允许 Topic、消息和消费位点写入。
	PermissionTopicWrite Permission = "topic.write"
	// PermissionRuntimeConfig 允许 NameServer、Broker 和 Proxy 运行配置写入。
	PermissionRuntimeConfig Permission = "runtime-config.write"
	// PermissionAuditRead 允许读取持久化操作审计。
	PermissionAuditRead Permission = "audit.read"
	// operationReasonHeader 兼容已有自动化调用方传入的 ASCII 操作理由。
	operationReasonHeader = "X-RMQD-Operation-Reason"
	// operationReasonEncodedHeader 承载浏览器百分号编码后的 UTF-8 操作理由。
	operationReasonEncodedHeader = "X-RMQD-Operation-Reason-Encoded"
)

// Principal 是已认证操作者的稳定身份与角色集合。
type Principal struct {
	// Subject 是审计中展示的操作者标识。
	Subject string `json:"subject"`
	// Roles 是身份拥有的角色，例如 operator、config-admin、auditor 或 admin。
	Roles []string `json:"roles"`
}

// Authenticator 从 HTTP 请求解析经验证的操作者身份。
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// denyAuthenticator 用于未配置凭据的部署，保证写接口默认不开放。
type denyAuthenticator struct{}

func (denyAuthenticator) Authenticate(*http.Request) (Principal, error) {
	return Principal{}, errors.New("当前部署未配置写操作身份凭据")
}

// TokenCredential 是挂载在部署密钥文件中的一个静态 Bearer 凭据。
type TokenCredential struct {
	// Token 是仅存于凭据文件的 Bearer token，审计和日志不会写入该字段。
	Token string `json:"token"`
	// Subject 是该 token 对应的操作者标识。
	Subject string `json:"subject"`
	// Roles 控制该 token 允许执行的运营操作。
	Roles []string `json:"roles"`
}

// tokenCredentialFile 是凭据文件的稳定 JSON 结构。
type tokenCredentialFile struct {
	Tokens []TokenCredential `json:"tokens"`
}

// tokenAuthenticator 使用部署挂载的静态 Bearer token 认证写操作。
type tokenAuthenticator struct {
	tokens map[string]Principal
}

// NewTokenAuthenticator 构建可复用的静态 token 认证器。
func NewTokenAuthenticator(credentials []TokenCredential) (Authenticator, error) {
	tokens := make(map[string]Principal, len(credentials))
	for _, credential := range credentials {
		token := strings.TrimSpace(credential.Token)
		subject := strings.TrimSpace(credential.Subject)
		if token == "" || subject == "" {
			return nil, errors.New("身份凭据必须包含 token 和 subject")
		}
		if _, exists := tokens[token]; exists {
			return nil, errors.New("身份凭据 token 不能重复")
		}
		roles := normalizeRoles(credential.Roles)
		if len(roles) == 0 {
			return nil, fmt.Errorf("身份凭据 %s 未配置角色", subject)
		}
		tokens[token] = Principal{Subject: subject, Roles: roles}
	}
	if len(tokens) == 0 {
		return denyAuthenticator{}, nil
	}
	return tokenAuthenticator{tokens: tokens}, nil
}

// LoadTokenAuthenticator 从部署挂载的 JSON 文件读取静态凭据；空路径表示禁用写操作。
func LoadTokenAuthenticator(path string) (Authenticator, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return denyAuthenticator{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取身份凭据文件失败: %w", err)
	}
	var file tokenCredentialFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("解析身份凭据文件失败: %w", err)
	}
	return NewTokenAuthenticator(file.Tokens)
}

// Authenticate 校验 Authorization: Bearer <token> 并返回其绑定身份。
func (a tokenAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		return Principal{}, errors.New("写操作需要 Bearer 身份凭据")
	}
	token := strings.TrimSpace(authorization[len("Bearer "):])
	principal, ok := a.tokens[token]
	if !ok {
		return Principal{}, errors.New("身份凭据无效")
	}
	return principal, nil
}

// allows 判断角色是否包含给定权限。
func (p Principal) allows(permission Permission) bool {
	for _, role := range p.Roles {
		switch role {
		case "admin":
			return true
		case "operator":
			if permission == PermissionTopicWrite {
				return true
			}
		case "config-admin":
			if permission == PermissionRuntimeConfig {
				return true
			}
		case "auditor":
			if permission == PermissionAuditRead {
				return true
			}
		}
	}
	return false
}

// AuditRecord 是一次运营操作的持久化审计事件。
type AuditRecord struct {
	// OperationID 把开始、完成和回滚事件关联为同一个操作。
	OperationID string `json:"operationId"`
	// Phase 是 started、completed 或 denied。
	Phase string `json:"phase"`
	// TimestampUnixMilli 是事件写入审计日志的时间。
	TimestampUnixMilli int64 `json:"timestampUnixMilli"`
	// Actor 是已认证操作者；被拒绝的匿名请求会记录 anonymous。
	Actor Principal `json:"actor"`
	// Action 是稳定的业务操作名称。
	Action string `json:"action"`
	// ClusterID 是本次操作影响的固定集群。
	ClusterID string `json:"clusterId"`
	// Target 是面向操作者的目标摘要。
	Target string `json:"target"`
	// Reason 是操作者显式提交的操作原因。
	Reason string `json:"reason"`
	// Before 是执行前的请求摘要，避免写入消息正文等敏感载荷。
	Before json.RawMessage `json:"before,omitempty"`
	// After 是完成后的执行结果。
	After json.RawMessage `json:"after,omitempty"`
	// Verification 保存读回、Broker 确认或回滚结果。
	Verification json.RawMessage `json:"verification,omitempty"`
	// Result 是 pending、succeeded、failed、rolled_back 或 denied。
	Result string `json:"result"`
	// Error 记录失败或拒绝原因。
	Error string `json:"error,omitempty"`
	// RollbackOf 指向同一操作的起始事件，标记自动回滚关联。
	RollbackOf string `json:"rollbackOf,omitempty"`
	// RequestID 关联调用方传入或服务生成的请求标识。
	RequestID string `json:"requestId"`
}

// AuditStore 提供可追加和可读取的持久化审计存储。
type AuditStore interface {
	Append(ctx context.Context, record AuditRecord) error
	List(ctx context.Context, clusterID string, limit int) ([]AuditRecord, error)
}

// FileAuditStore 使用 JSONL 追加日志保存审计，便于卷挂载、备份和离线查询。
type FileAuditStore struct {
	path string
	mu   sync.Mutex
}

// NewFileAuditStore 创建 JSONL 审计存储，文件会在第一条审计事件时创建。
func NewFileAuditStore(path string) *FileAuditStore {
	return &FileAuditStore{path: path}
}

// Append 同步落盘一条审计记录；写操作在开始记录失败时会被拒绝。
func (s *FileAuditStore) Append(ctx context.Context, record AuditRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(s.path) == "" {
		return errors.New("审计日志路径未配置")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("序列化审计记录失败: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("创建审计目录失败: %w", err)
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("打开审计日志失败: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("写入审计日志失败: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("同步审计日志失败: %w", err)
	}
	return nil
}

// List 返回指定集群最近的审计事件，忽略日志尾部未完整写入的行。
func (s *FileAuditStore) List(ctx context.Context, clusterID string, limit int) ([]AuditRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return []AuditRecord{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取审计日志失败: %w", err)
	}
	records := make([]AuditRecord, 0)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record AuditRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if clusterID == "" || record.ClusterID == clusterID {
			records = append(records, record)
		}
	}
	sort.SliceStable(records, func(left, right int) bool {
		return records[left].TimestampUnixMilli > records[right].TimestampUnixMilli
	})
	if len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

// memoryAuditStore 是测试时可注入的内存审计实现。
type memoryAuditStore struct {
	mu      sync.Mutex
	records []AuditRecord
}

// NewMemoryAuditStore 构建测试用审计存储。
func NewMemoryAuditStore() AuditStore {
	return &memoryAuditStore{}
}

func (s *memoryAuditStore) Append(_ context.Context, record AuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)
	return nil
}

func (s *memoryAuditStore) List(_ context.Context, clusterID string, limit int) ([]AuditRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	records := make([]AuditRecord, 0, len(s.records))
	for _, record := range s.records {
		if clusterID == "" || record.ClusterID == clusterID {
			records = append(records, record)
		}
	}
	if len(records) > limit {
		records = records[len(records)-limit:]
	}
	return records, nil
}

// mutationAdmissionError 保留可以直接映射为 HTTP 状态码的写操作准入错误。
type mutationAdmissionError struct {
	status int
	err    error
}

func (e mutationAdmissionError) Error() string {
	return e.err.Error()
}

// mutationAudit 追踪一次已落盘开始事件的后续完成状态。
type mutationAudit struct {
	app    *App
	record AuditRecord
}

// operationReasonFromRequest 优先解码浏览器安全 Header，旧调用方仍可继续使用原始 Header。
func operationReasonFromRequest(r *http.Request) (string, error) {
	encoded := strings.TrimSpace(r.Header.Get(operationReasonEncodedHeader))
	if encoded == "" {
		return strings.TrimSpace(r.Header.Get(operationReasonHeader)), nil
	}
	reason, err := url.PathUnescape(encoded)
	if err != nil {
		return "", errors.New("写操作理由编码无效")
	}
	return strings.TrimSpace(reason), nil
}

// beginMutation 完成身份、权限、原因和开始审计记录四个写操作前置条件。
func (a *App) beginMutation(r *http.Request, permission Permission, action string, target string, before any) (*mutationAudit, error) {
	runtime := clusterRuntimeFromContext(r.Context())
	clusterID := ""
	if runtime != nil {
		clusterID = runtime.definition.ID
	}
	principal, err := a.authenticator.Authenticate(r)
	if err != nil {
		a.appendDeniedAudit(r.Context(), clusterID, action, target, Principal{Subject: "anonymous"}, err)
		return nil, mutationAdmissionError{status: http.StatusUnauthorized, err: err}
	}
	if !principal.allows(permission) {
		err := errors.New("当前身份没有执行该操作的权限")
		a.appendDeniedAudit(r.Context(), clusterID, action, target, principal, err)
		return nil, mutationAdmissionError{status: http.StatusForbidden, err: err}
	}
	reason, err := operationReasonFromRequest(r)
	if err != nil {
		a.appendDeniedAudit(r.Context(), clusterID, action, target, principal, err)
		return nil, mutationAdmissionError{status: http.StatusBadRequest, err: err}
	}
	if reason == "" {
		err := errors.New("写操作需要操作理由")
		a.appendDeniedAudit(r.Context(), clusterID, action, target, principal, err)
		return nil, mutationAdmissionError{status: http.StatusBadRequest, err: err}
	}
	operationID, err := newOperationID()
	if err != nil {
		return nil, mutationAdmissionError{status: http.StatusInternalServerError, err: err}
	}
	beforeJSON, err := marshalAuditValue(before)
	if err != nil {
		return nil, mutationAdmissionError{status: http.StatusInternalServerError, err: err}
	}
	record := AuditRecord{
		OperationID:        operationID,
		Phase:              "started",
		TimestampUnixMilli: time.Now().UnixMilli(),
		Actor:              principal,
		Action:             action,
		ClusterID:          clusterID,
		Target:             target,
		Reason:             reason,
		Before:             beforeJSON,
		Result:             "pending",
		RequestID:          requestID(r),
	}
	if err := a.auditStore.Append(r.Context(), record); err != nil {
		return nil, mutationAdmissionError{status: http.StatusServiceUnavailable, err: fmt.Errorf("写入操作审计失败: %w", err)}
	}
	return &mutationAudit{app: a, record: record}, nil
}

// complete 持久化操作完成状态；自动回滚会和对应开始事件关联。
func (m *mutationAudit) complete(ctx context.Context, after any, verification any, operationErr error, rolledBack bool) error {
	afterJSON, err := marshalAuditValue(after)
	if err != nil {
		return err
	}
	verificationJSON, err := marshalAuditValue(verification)
	if err != nil {
		return err
	}
	record := m.record
	record.Phase = "completed"
	record.TimestampUnixMilli = time.Now().UnixMilli()
	record.After = afterJSON
	record.Verification = verificationJSON
	record.Result = "succeeded"
	if operationErr != nil {
		record.Result = "failed"
		record.Error = operationErr.Error()
	}
	if rolledBack {
		record.Result = "rolled_back"
		record.RollbackOf = m.record.OperationID
	}
	return m.app.auditStore.Append(ctx, record)
}

// appendDeniedAudit 保留被拒绝写尝试的最小可追溯信息，避免泄露无效 token。
func (a *App) appendDeniedAudit(ctx context.Context, clusterID string, action string, target string, actor Principal, operationErr error) {
	if a.auditStore == nil {
		return
	}
	if strings.TrimSpace(actor.Subject) == "" {
		actor = Principal{Subject: "anonymous"}
	}
	operationID, err := newOperationID()
	if err != nil {
		return
	}
	_ = a.auditStore.Append(ctx, AuditRecord{
		OperationID:        operationID,
		Phase:              "denied",
		TimestampUnixMilli: time.Now().UnixMilli(),
		Actor:              actor,
		Action:             action,
		ClusterID:          clusterID,
		Target:             target,
		Result:             "denied",
		Error:              operationErr.Error(),
	})
}

// writeMutationAdmissionError 将统一准入错误映射为 HTTP 响应。
func writeMutationAdmissionError(w http.ResponseWriter, err error) {
	var admission mutationAdmissionError
	if errors.As(err, &admission) {
		writeError(w, admission.status, admission.err)
		return
	}
	writeError(w, http.StatusInternalServerError, err)
}

// handleAudit 返回当前集群最近的持久化审计事件，仅 auditor 或 admin 可读取。
func (a *App) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}
	principal, err := a.authenticator.Authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	if !principal.allows(PermissionAuditRead) {
		writeError(w, http.StatusForbidden, errors.New("当前身份没有读取审计的权限"))
		return
	}
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 || parsed > 200 {
			writeError(w, http.StatusBadRequest, errors.New("limit 必须是 1 到 200 的整数"))
			return
		}
		limit = parsed
	}
	runtime := clusterRuntimeFromContext(r.Context())
	records, err := a.auditStore.List(r.Context(), runtime.definition.ID, limit)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, responsePayload[[]AuditRecord]{
		Code:    0,
		Message: "ok",
		Data:    records,
	})
}

// newOperationID 创建不依赖进程内计数器的高熵操作关联标识。
func newOperationID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("生成操作标识失败: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// requestID 优先复用调用方关联标识，缺失时生成新的高熵标识。
func requestID(r *http.Request) string {
	if value := strings.TrimSpace(r.Header.Get("X-Request-ID")); value != "" {
		return value
	}
	id, err := newOperationID()
	if err != nil {
		return ""
	}
	return id
}

// marshalAuditValue 将结构化审计值转为 JSON，nil 保持为空字段。
func marshalAuditValue(value any) (json.RawMessage, error) {
	if value == nil {
		return nil, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("序列化审计内容失败: %w", err)
	}
	return data, nil
}

// normalizeRoles 清理空角色并稳定去重，避免角色配置因顺序差异产生审计噪声。
func normalizeRoles(roles []string) []string {
	seen := make(map[string]struct{}, len(roles))
	normalized := make([]string, 0, len(roles))
	for _, role := range roles {
		role = strings.ToLower(strings.TrimSpace(role))
		if role == "" {
			continue
		}
		if _, exists := seen[role]; exists {
			continue
		}
		seen[role] = struct{}{}
		normalized = append(normalized, role)
	}
	sort.Strings(normalized)
	return normalized
}
