//! 管理 API 路由处理器 —— 登录/设置密码、账号池状态、请求统计、模型列表、配置查看

use axum::{
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::Response,
};
use serde::{Deserialize, Serialize};

use super::handlers::AppState;
use crate::AccountSyncReport;
use crate::config::{AutoResumeSection, Config};

// ── 请求/响应类型 ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct SetupRequest {
    pub password: String,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Serialize)]
pub struct AdminStatusResponse {
    pub accounts: Vec<ds_core::AccountStatus>,
    pub total: usize,
    pub idle: usize,
    pub busy: usize,
    pub error: usize,
    pub invalid: usize,
    /// 后端二进制版本（`CARGO_PKG_VERSION`），WebUI 直接显示它，避免前端硬编码陈旧版本号
    pub version: String,
    /// 最近一次账号同步结果（新增账号被拒时面板据此提示原因）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sync: Option<AccountSyncReport>,
}

#[derive(Serialize)]
pub struct AdminAccountStatusesDetailedResponse {
    pub accounts: Vec<ds_core::AccountStatus>,
    pub total: usize,
    pub window_seconds: u64,
}

#[derive(Serialize)]
pub struct AdminStatsResponse {
    #[serde(flatten)]
    pub stats: super::stats::StatsSnapshot,
}

#[derive(Serialize)]
pub struct AdminConfigResponse {
    pub server: ServerConfigView,
    pub ds_core: DsCoreView,
    pub proxy: ProxyConfigView,
    /// OTA 自更新配置（对应 `[update]`）
    pub update: UpdateView,
    pub admin: AdminConfigView,
    pub api_keys: Vec<ApiKeyEntryView>,
}

#[derive(Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "管理面板往返 JSON 必须逐字段平铺，与 TOML 段一一对应"
)]
pub struct DsCoreView {
    pub accounts: Vec<AccountView>,
    pub api_base: String,
    pub wasm_url: String,
    pub user_agent: String,
    pub client_version: String,
    pub client_platform: String,
    pub client_locale: String,
    pub client_os: String,
    pub client_bundle_id: String,
    pub client_device_id: String,
    pub client_device_model: String,
    pub client_timezone_offset: String,
    /// 传输层拟态档位（okhttp4_12 / chrome136）
    pub emulation: String,
    /// 是否在 completion 请求上回传 x-hif-leim 风控令牌
    pub hif_enabled: bool,
    /// 启动时是否用一条 completion 做健康检查
    pub startup_health_check: bool,
    /// 会话策略：`per_request`（每轮建删）或 `reuse`（保留并复用）
    pub session_policy: String,
    /// 复用模式下会话空闲回收秒数（0 = 只在进程退出时删除）
    pub session_idle_secs: u64,
    pub model_types: Vec<String>,
    pub max_input_tokens: Vec<u32>,
    pub max_output_tokens: Vec<u32>,
    pub input_character_limits: Vec<u32>,
    pub model_aliases: Vec<String>,
    pub tool_call: ToolCallTagConfigView,
    /// 每账号每小时请求上限（0 = 不限制）
    pub hourly_request_quota: u64,
    /// 同账号两次请求的最小间隔秒数（0 = 不限制）
    pub request_min_interval_secs: u64,
    pub auto_resume: AutoResumeSection,
    /// 是否对 `/api/v0/users/*` POST 做官方加密
    pub login_encryption: bool,
    /// 未显式传 `web_search_options` 时是否默认开启搜索模式
    pub default_search_enabled: bool,
    /// Responses API `previous_response_id` 缓存条数上限
    pub responses_store_capacity: usize,
    /// Responses API 上下文缓存存活秒数
    pub responses_store_ttl_secs: u64,
    /// 模型元数据来源：`auto`（实测常量）或 `config`（按配置数组）
    pub model_metadata_source: String,
}

#[derive(Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "管理面板往返 JSON 必须逐字段平铺，与 TOML 段一一对应"
)]
pub struct UpdateView {
    pub enabled: bool,
    pub check_on_startup: bool,
    pub allow_apply: bool,
    pub repo: String,
    pub use_latest: bool,
    pub target_tag: String,
    pub api_base: String,
}

#[derive(Serialize)]
pub struct ServerConfigView {
    pub host: String,
    pub port: u16,
    pub cors_origins: Vec<String>,
}

#[derive(Serialize)]
pub struct ToolCallTagConfigView {
    pub extra_starts: Vec<String>,
    pub extra_ends: Vec<String>,
}

#[derive(Serialize)]
pub struct ProxyConfigView {
    pub url: Option<String>,
    pub bypass_hosts: Vec<String>,
}

#[derive(Serialize)]
pub struct AdminConfigView {
    pub password_set: bool,
    pub jwt_issued_at: u64,
}

#[derive(Serialize)]
pub struct ApiKeyEntryView {
    pub key: String,
    pub description: String,
}
#[derive(Serialize)]
pub struct AccountView {
    pub email: String,
    pub mobile: String,
    pub area_code: String,
    pub password: String,
    pub device_id: String,
}

// ── 脱敏 ─────────────────────────────────────────────────────────────────

fn mask_config(config: &Config) -> AdminConfigResponse {
    AdminConfigResponse {
        server: ServerConfigView {
            host: config.server.host.clone(),
            port: config.server.port,
            cors_origins: config.server.cors_origins.clone(),
        },
        ds_core: DsCoreView {
            accounts: config
                .ds_core
                .accounts
                .iter()
                .map(|a| AccountView {
                    email: a.email.clone(),
                    mobile: a.mobile.clone(),
                    area_code: a.area_code.clone(),
                    password: a.password.clone(),
                    device_id: a.device_id.clone(),
                })
                .collect(),
            api_base: config.ds_core.api_base.clone(),
            wasm_url: config.ds_core.wasm_url.clone(),
            user_agent: config.ds_core.user_agent.clone(),
            client_version: config.ds_core.client_version.clone(),
            client_platform: config.ds_core.client_platform.clone(),
            client_locale: config.ds_core.client_locale.clone(),
            client_os: config.ds_core.client_os.clone(),
            client_bundle_id: config.ds_core.client_bundle_id.clone(),
            client_device_id: config.ds_core.client_device_id.clone(),
            client_device_model: config.ds_core.client_device_model.clone(),
            client_timezone_offset: config.ds_core.client_timezone_offset.clone(),
            emulation: config.ds_core.emulation.clone(),
            hif_enabled: config.ds_core.hif_enabled,
            startup_health_check: config.ds_core.startup_health_check,
            session_policy: config.ds_core.session_policy.clone(),
            session_idle_secs: config.ds_core.session_idle_secs,
            model_types: config.ds_core.model_types.clone(),
            max_input_tokens: config.ds_core.max_input_tokens.clone(),
            max_output_tokens: config.ds_core.max_output_tokens.clone(),
            input_character_limits: config.ds_core.input_character_limits.clone(),
            model_aliases: config.ds_core.model_aliases.clone(),
            tool_call: ToolCallTagConfigView {
                extra_starts: config.ds_core.tool_call.extra_starts.clone(),
                extra_ends: config.ds_core.tool_call.extra_ends.clone(),
            },
            hourly_request_quota: config.ds_core.hourly_request_quota,
            request_min_interval_secs: config.ds_core.request_min_interval_secs,
            auto_resume: config.ds_core.auto_resume.clone(),
            login_encryption: config.ds_core.login_encryption,
            default_search_enabled: config.ds_core.default_search_enabled,
            responses_store_capacity: config.ds_core.responses_store_capacity,
            responses_store_ttl_secs: config.ds_core.responses_store_ttl_secs,
            model_metadata_source: config.ds_core.model_metadata_source.clone(),
        },
        update: UpdateView {
            enabled: config.update.enabled,
            check_on_startup: config.update.check_on_startup,
            allow_apply: config.update.allow_apply,
            repo: config.update.repo.clone(),
            use_latest: config.update.use_latest,
            target_tag: config.update.target_tag.clone(),
            api_base: config.update.api_base.clone(),
        },
        proxy: ProxyConfigView {
            url: config.proxy.url.clone(),
            bypass_hosts: config.proxy.bypass_hosts.clone(),
        },
        admin: AdminConfigView {
            password_set: !config.admin.password_hash.is_empty(),
            jwt_issued_at: config.admin.jwt_issued_at,
        },
        api_keys: config
            .api_keys
            .iter()
            .map(|k| ApiKeyEntryView {
                key: k.key.clone(),
                description: k.description.clone(),
            })
            .collect(),
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────

/// POST /admin/api/setup — 首次设置密码
pub(crate) async fn admin_setup(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Response {
    let req: SetupRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &format!("请求格式错误: {e}")),
    };

    match super::auth::setup_admin(&state.store, &state.login_limiter, &req.password).await {
        Ok(token) => json_response(&LoginResponse { token }),
        Err(msg) => {
            let status = if msg.contains("已设置") {
                StatusCode::FORBIDDEN
            } else if msg.contains("次数过多") {
                StatusCode::TOO_MANY_REQUESTS
            } else if msg.contains("至少 6 位") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            error_response(status, &msg)
        }
    }
}

pub(crate) async fn admin_login(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Response {
    let req: LoginRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &format!("请求格式错误: {e}")),
    };

    match super::auth::login_admin(&state.store, &state.login_limiter, &req.password).await {
        Ok(token) => json_response(&LoginResponse { token }),
        Err(msg) => {
            let status = if msg.contains("次数过多") {
                StatusCode::TOO_MANY_REQUESTS
            } else if msg.contains("未设置密码") {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::UNAUTHORIZED
            };
            error_response(status, &msg)
        }
    }
}

/// GET /admin/api/status
pub(crate) async fn admin_status(State(state): State<AppState>) -> Response {
    let statuses = state.adapter.account_statuses();
    let total = statuses.len();
    let busy = statuses.iter().filter(|a| a.state == "busy").count();
    let idle = statuses.iter().filter(|a| a.state == "idle").count();
    let error = statuses.iter().filter(|a| a.state == "error").count();
    let invalid = statuses.iter().filter(|a| a.state == "invalid").count();

    let resp = AdminStatusResponse {
        accounts: statuses,
        total,
        idle,
        busy,
        error,
        invalid,
        version: env!("CARGO_PKG_VERSION").to_string(),
        last_sync: state.adapter.last_sync_report(),
    };
    json_response(&resp)
}

/// GET /admin/api/account-statuses-detailed
/// Returns detailed account status including sliding window stats,
/// request intervals, and quota usage. Useful for testing and debugging.
pub(crate) async fn admin_account_statuses_detailed(State(state): State<AppState>) -> Response {
    let statuses = state.adapter.account_statuses_detailed();
    let total = statuses.len();
    let resp = AdminAccountStatusesDetailedResponse {
        accounts: statuses,
        total,
        window_seconds: 3600,
    };
    json_response(&resp)
}

/// GET /admin/api/sessions —— 账号会话列表（分页）
///
/// `updated_at` 为上一页末尾会话的更新时间戳（Unix 秒），不传取第一页。
/// 借用池中一个空闲账号发起；会话数据归属上游网页端（见 issue #110）。
#[derive(Debug, Deserialize)]
pub struct SessionsQuery {
    pub updated_at: Option<f64>,
}

#[derive(Serialize)]
pub struct AdminSessionsResponse {
    pub sessions: Vec<ds_core::ChatSessionInfo>,
    pub total: usize,
    /// 是否还有下一页（上游 fetch_page 响应）
    pub has_more: bool,
    /// 下一页游标 = 本页末尾会话的 updated_at；None = 无更多
    pub next_cursor: Option<f64>,
}

pub(crate) async fn admin_sessions(
    State(state): State<AppState>,
    Query(query): Query<SessionsQuery>,
) -> Response {
    match state.adapter.fetch_sessions(query.updated_at).await {
        Ok(data) => {
            let total = data.chat_sessions.len();
            let has_more = data.has_more.unwrap_or(false);
            let next_cursor = if has_more {
                data.chat_sessions.last().and_then(|s| s.updated_at)
            } else {
                None
            };
            let resp = AdminSessionsResponse {
                sessions: data.chat_sessions,
                total,
                has_more,
                next_cursor,
            };
            json_response(&resp)
        }
        Err(crate::openai_adapter::OpenAIAdapterError::Overloaded) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "没有可用账号：账号池中所有账号均不可用（可能全部被禁言或初始化失败）。\
             会话列表只从健康账号读取。",
        ),
        Err(e) => error_response(
            StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            &e.to_string(),
        ),
    }
}

/// GET /admin/api/stats
pub(crate) async fn admin_stats(State(state): State<AppState>) -> Response {
    let snapshot = state.stats.snapshot();
    let resp = AdminStatsResponse { stats: snapshot };
    json_response(&resp)
}

/// GET /admin/api/models
pub(crate) async fn admin_models(State(state): State<AppState>) -> Response {
    let models = state.adapter.list_models().await;
    json_response(&models)
}

/// GET /admin/api/config
pub(crate) async fn admin_config(State(state): State<AppState>) -> Response {
    let config_view = {
        let config = state.config.read().await;
        mask_config(&config)
    };
    json_response(&config_view)
}

/// PUT /admin/api/config — 更新并热重载配置
pub(crate) async fn admin_put_config(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Response {
    let mut new_config: Config = match serde_json::from_slice(&body) {
        Ok(c) => c,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &format!("JSON 解析失败: {e}")),
    };

    // Validate
    if let Err(e) = new_config.validate() {
        return error_response(StatusCode::BAD_REQUEST, &e.to_string());
    }
    // Merge: empty/"***" passwords keep existing values from current config;
    // API keys match by `id` (stable identifier), falling back to description for old-format migration.
    {
        let current = state.config.read().await;
        for a in &mut new_config.ds_core.accounts {
            if (a.password.is_empty() || a.password == "***")
                && let Some(existing) = current
                    .ds_core
                    .accounts
                    .iter()
                    .find(|e| e.email == a.email && e.mobile == a.mobile)
            {
                a.password.clone_from(&existing.password);
            }
            // device_id 为空时保留现有值（面板旧前端不发送该字段）
            if a.device_id.is_empty()
                && let Some(existing) = current
                    .ds_core
                    .accounts
                    .iter()
                    .find(|e| e.email == a.email && e.mobile == a.mobile)
            {
                a.device_id.clone_from(&existing.device_id);
            }
        }
        // Admin 配置：空的 password_hash/jwt_secret 保留现有值（前端不返回这些字段）
        if new_config.admin.password_hash.is_empty() {
            new_config
                .admin
                .password_hash
                .clone_from(&current.admin.password_hash);
        }
        if new_config.admin.jwt_secret.is_empty() {
            new_config
                .admin
                .jwt_secret
                .clone_from(&current.admin.jwt_secret);
        }
        // 密码修改：前端发 old_password + new_password
        if !new_config.admin.old_password.is_empty() || !new_config.admin.new_password.is_empty() {
            if new_config.admin.old_password.is_empty() || new_config.admin.new_password.is_empty()
            {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "修改密码需要同时提供旧密码和新密码",
                );
            }
            if !bcrypt::verify(&new_config.admin.old_password, &current.admin.password_hash)
                .unwrap_or(false)
            {
                return error_response(StatusCode::BAD_REQUEST, "旧密码不正确");
            }
            new_config.admin.password_hash =
                super::store::hash_password(&new_config.admin.new_password);
            new_config.admin.jwt_secret = super::store::generate_hex_secret();
            new_config.admin.jwt_issued_at += 1;
        }
        drop(current);
    }

    // Persist
    {
        let mut guard = state.config.write().await;
        *guard = new_config.clone();
        if let Err(e) = guard.save(&state.config_path) {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("保存失败: {e}"));
        }
    }

    // 账号同步：同步执行并把失败原因回报给面板
    //
    // 账号池「加了账号却报不出东西」的根因就在这里：旧实现把 add_account 的错误
    // 吞进 log::warn!，后台任务又只记 reload 的整体 Err，面板拿到的永远是 {"ok":true}。
    // 新增账号必须真的登录上游，所以要等它完成才能告诉用户「成功/被拒/为什么」。
    // 为防止慢上游把保存请求拖成 502，这里加超时——超时的账号按「未确认」回报，
    // 后台恢复任务会在 60s 内重试登录。
    let sync_report = tokio::time::timeout(
        ACCOUNT_SYNC_TIMEOUT,
        state
            .adapter
            .sync_accounts_now(&new_config.ds_core.accounts),
    )
    .await
    .unwrap_or_else(|_| {
        log::warn!(
            target: "http::server",
            "账号同步超时（{}s），未完成的新增账号将由后台恢复任务重试",
            ACCOUNT_SYNC_TIMEOUT.as_secs()
        );
        AccountSyncReport {
            timed_out: true,
            ..AccountSyncReport::default()
        }
    });

    // 客户端与模型注册表重建放后台：要重新拉 WASM，慢但失败只需重启即可
    let adapter = state.adapter.clone();
    let cfg = new_config.clone();
    tokio::spawn(async move {
        if let Err(e) = adapter.reload_config(&cfg).await {
            log::warn!(target: "http::server", "配置已保存但热重载失败（重启后生效）: {e}");
        }
    });

    // 有账号被拒 / 同步超时 → 200 仍返回 ok，但附带 accounts 明细供面板提示
    let body = serde_json::json!({
        "ok": true,
        "accounts": sync_report,
    });
    json_response(&body)
}

/// 账号同步在保存请求里的最长等待时间
///
/// 新增一个账号通常是一次登录往返（< 2s）；给到 20s 足够覆盖慢网络，
/// 又不会让网关把保存请求记成 502。
const ACCOUNT_SYNC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

#[derive(Deserialize)]
pub struct LogsQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
}

const fn default_limit() -> usize {
    50
}

/// GET /admin/api/logs — 获取最近的请求日志
pub(crate) async fn admin_logs(
    Query(query): Query<LogsQuery>,
    State(state): State<AppState>,
) -> Response {
    let logs = state.stats.recent_logs(query.limit);
    json_response(&logs)
}

#[derive(Deserialize)]
pub struct RuntimeLogsQuery {
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_runtime_limit")]
    pub limit: usize,
}

const fn default_runtime_limit() -> usize {
    100
}

/// GET /admin/api/runtime-logs — 分页查询运行日志
pub(crate) async fn admin_runtime_logs(Query(query): Query<RuntimeLogsQuery>) -> Response {
    let (total, logs) = super::runtime_log::query_logs(query.offset, query.limit).await;
    json_response(&serde_json::json!({
        "total": total,
        "offset": query.offset,
        "limit": query.limit,
        "logs": logs,
    }))
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn json_response<T: Serialize>(data: &T) -> Response {
    let bytes = serde_json::to_vec(data).unwrap();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(bytes))
        .unwrap()
}

fn error_response(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({"error": message});
    let bytes = serde_json::to_vec(&body).unwrap();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(bytes))
        .unwrap()
}

// ============================================================================
// 前后端契约测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> Config {
        toml::from_str(
            r#"
[server]
host = "127.0.0.1"
port = 22217
cors_origins = ["http://localhost:22217"]

[ds_core]
api_base = "https://example.invalid/api/v0"
wasm_url = "https://example.invalid/wasm.wasm"
user_agent = "test-agent"
client_version = "9.9.9"
client_platform = "android"
client_locale = "zh_CN"
model_types = ["default"]
max_input_tokens = [1048576]
max_output_tokens = [384000]
input_character_limits = [2621440]
model_aliases = ["alias-a"]
responses_store_capacity = 128
responses_store_ttl_secs = 7200
default_search_enabled = false
hourly_request_quota = 42
request_min_interval_secs = 120
login_encryption = false

[ds_core.auto_resume]
thinking_heartbeat_secs = 15
on_truncation = true
max_attempts = 3
empty_prompt = false

[[ds_core.accounts]]
email = "a@example.com"
mobile = ""
area_code = ""
password = "pw"
device_id = "dev-1"

[proxy]
url = "http://127.0.0.1:7890"

[update]
enabled = true
check_on_startup = true
allow_apply = false
repo = "owner/repo"
use_latest = false
target_tag = "v9.9.9"
api_base = "https://api.github.test"

[[api_keys]]
key = "sk-test"
description = "test"
"#,
        )
        .expect("sample config must parse")
    }

    /// 从 `web/src/lib/api.ts` 的 TS interface 中提取字段名
    ///
    /// 这是一个真实的**前后端契约测试**：后端序列化出的 JSON 必须包含
    /// 前端 `FullConfig` 类型声明的每一个字段，否则前端只能靠 `??` 兜底，
    /// 用户的自定义值会被默认值静默覆盖。
    fn ts_interface_fields(source: &str, name: &str) -> Vec<String> {
        let start = source
            .find(&format!("export interface {name} {{"))
            .unwrap_or_else(|| panic!("api.ts 中缺少 interface {name}"));
        let rest = &source[start..];
        let end = rest.find('}').expect("unterminated interface");
        rest[..end]
            .lines()
            .skip(1)
            .filter_map(|line| {
                let line = line.trim();
                if line.starts_with("//") || line.starts_with("/*") || line.starts_with('*') {
                    return None;
                }
                let (field, _) = line.split_once(':')?;
                let field = field.trim().trim_end_matches('?');
                (!field.is_empty() && field.chars().all(|c| c.is_alphanumeric() || c == '_'))
                    .then(|| field.to_string())
            })
            .collect()
    }

    const API_TS: &str = include_str!("../../web/src/lib/api.ts");

    #[test]
    fn admin_config_json_satisfies_frontend_ds_core_contract() {
        let view = mask_config(&sample_config());
        let json = serde_json::to_value(&view).expect("serialize admin config view");
        let ds_core = json.get("ds_core").expect("ds_core key").clone();

        for field in ts_interface_fields(API_TS, "DsCoreConfig") {
            assert!(
                ds_core.get(&field).is_some(),
                "admin GET /admin/api/config 缺少前端 DsCoreConfig 声明的字段: {field}\n实际: {ds_core}"
            );
        }
    }

    #[test]
    fn admin_config_json_satisfies_frontend_top_level_contract() {
        let view = mask_config(&sample_config());
        let json = serde_json::to_value(&view).expect("serialize admin config view");

        // FullConfig 的标量与对象字段
        for field in ["server", "ds_core", "proxy", "update", "admin", "api_keys"] {
            assert!(json.get(field).is_some(), "缺少顶层字段 {field}");
        }

        let admin = json.get("admin").expect("admin key");
        for field in ts_interface_fields(API_TS, "AdminConfigResponse") {
            assert!(admin.get(&field).is_some(), "admin view 缺少字段 {field}");
        }

        let server = json.get("server").expect("server key");
        for field in ts_interface_fields(API_TS, "ServerConfig") {
            assert!(server.get(&field).is_some(), "server view 缺少字段 {field}");
        }

        let proxy = json.get("proxy").expect("proxy key");
        for field in ts_interface_fields(API_TS, "ProxyConfig") {
            assert!(proxy.get(&field).is_some(), "proxy view 缺少字段 {field}");
        }

        let update = json.get("update").expect("update key");
        for field in ts_interface_fields(API_TS, "UpdateConfig") {
            assert!(update.get(&field).is_some(), "update view 缺少字段 {field}");
        }

        let key = json["api_keys"][0].clone();
        for field in ts_interface_fields(API_TS, "ApiKeyEntry") {
            assert!(key.get(&field).is_some(), "api key view 缺少字段 {field}");
        }

        let account = json["ds_core"]["accounts"][0].clone();
        for field in ts_interface_fields(API_TS, "AccountEntry") {
            assert!(
                account.get(&field).is_some(),
                "account view 缺少字段 {field}"
            );
        }
    }

    #[test]
    fn responses_store_fields_round_trip_through_admin_config() {
        let view = mask_config(&sample_config());
        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["ds_core"]["responses_store_capacity"], 128);
        assert_eq!(json["ds_core"]["responses_store_ttl_secs"], 7200);
        assert_eq!(json["ds_core"]["default_search_enabled"], false);
        assert_eq!(json["ds_core"]["hourly_request_quota"], 42);
        assert_eq!(json["ds_core"]["request_min_interval_secs"], 120);
        assert_eq!(
            json["ds_core"]["auto_resume"]["thinking_heartbeat_secs"],
            15
        );
        assert_eq!(json["ds_core"]["auto_resume"]["on_truncation"], true);
        assert_eq!(json["ds_core"]["auto_resume"]["max_attempts"], 3);
        assert_eq!(json["ds_core"]["auto_resume"]["empty_prompt"], false);
        assert_eq!(json["ds_core"]["login_encryption"], false);
    }

    #[test]
    fn index_aligned_arrays_match_model_types_length() {
        let view = mask_config(&sample_config());
        let json = serde_json::to_value(&view).unwrap();
        let ds = &json["ds_core"];
        let n = ds["model_types"].as_array().unwrap().len();
        for field in [
            "max_input_tokens",
            "max_output_tokens",
            "input_character_limits",
            "model_aliases",
        ] {
            assert_eq!(
                ds[field].as_array().unwrap().len(),
                n,
                "{field} 必须与 model_types 等长（前端按 index 对齐渲染）"
            );
        }
    }
}

// ── 自更新（OTA）────────────────────────────────────────────────────────────

/// OTA 运行时状态：进程内单例，管理面板从这里读当前进度
pub(crate) struct UpdateState {
    status: tokio::sync::RwLock<crate::self_update::UpdateStatus>,
    /// 同一时间只允许一个更新任务
    busy: std::sync::atomic::AtomicBool,
}

impl UpdateState {
    pub(crate) fn new() -> Self {
        Self {
            status: tokio::sync::RwLock::new(crate::self_update::UpdateStatus::Idle),
            busy: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[derive(serde::Serialize)]
pub(crate) struct UpdateInfoResponse {
    pub current: String,
    pub deploy_id: String,
    pub enabled: bool,
    pub status: crate::self_update::UpdateStatus,
}

/// GET /admin/api/update — 当前版本 + 最近一次检查/更新的状态
pub(crate) async fn admin_update_info(State(state): State<AppState>) -> Response {
    let status = state.update_status.status.read().await.clone();
    let config = state.config.read().await;
    json_response(&UpdateInfoResponse {
        current: env!("CARGO_PKG_VERSION").to_string(),
        deploy_id: crate::self_update::deploy_id(),
        enabled: config.update.enabled,
        status,
    })
}

/// POST /admin/api/update/check — 只查询 GitHub Releases，不改动任何东西
pub(crate) async fn admin_update_check(State(state): State<AppState>) -> Response {
    let update_cfg = state.config.read().await.update.clone();
    let current = env!("CARGO_PKG_VERSION");
    match crate::self_update::check(state.adapter.ds_core(), &update_cfg, current).await {
        Ok(view) => {
            *state.update_status.status.write().await = view.status.clone();
            json_response(&view)
        }
        Err(e) => {
            let status = crate::self_update::UpdateStatus::Failed {
                latest: None,
                error: e.clone(),
                at: crate::self_update::now_secs(),
            };
            *state.update_status.status.write().await = status.clone();
            error_response(StatusCode::BAD_GATEWAY, &format!("{e} ({status:?})"))
        }
    }
}

/// POST /admin/api/update/apply — 下载 + 校验 + 替换二进制，需要重启生效
pub(crate) async fn admin_update_apply(State(state): State<AppState>) -> Response {
    use std::sync::atomic::Ordering;

    if state.update_status.busy.swap(true, Ordering::AcqRel) {
        return error_response(StatusCode::CONFLICT, "已有更新任务在进行中");
    }
    let result = run_apply(state.clone()).await;
    state.update_status.busy.store(false, Ordering::Release);

    match result {
        Ok(status) => {
            *state.update_status.status.write().await = status.clone();
            json_response(&serde_json::json!({
                "status": status,
                "restart_required": true,
            }))
        }
        Err(e) => {
            *state.update_status.status.write().await = crate::self_update::UpdateStatus::Failed {
                latest: None,
                error: e.clone(),
                at: crate::self_update::now_secs(),
            };
            error_response(StatusCode::BAD_GATEWAY, &e)
        }
    }
}

/// 实际执行：检查 → 下载 → 校验 → 替换，并逐步写入状态供 UI 轮询
async fn run_apply(state: AppState) -> Result<crate::self_update::UpdateStatus, String> {
    let update_cfg = {
        let config = state.config.read().await;
        if !config.update.enabled {
            return Err("[update] enabled = false，管理面板未开启自更新".to_string());
        }
        if !config.update.allow_apply {
            return Err("[update] allow_apply = false，只允许检查不允许应用".to_string());
        }
        config.update.clone()
    };
    let current = env!("CARGO_PKG_VERSION");
    let view = crate::self_update::check(state.adapter.ds_core(), &update_cfg, current).await?;
    if !view.update_available {
        return Err(format!("当前版本 {current} 已是最新（{}）", view.latest));
    }
    let (Some(latest), Some(asset)) = (Some(view.latest.clone()), view.asset.clone()) else {
        return Err("最新版本缺少本平台产物".to_string());
    };
    *state.update_status.status.write().await = crate::self_update::UpdateStatus::Downloading {
        latest: latest.clone(),
        asset,
    };

    let bin = crate::self_update::current_exe()?;
    crate::self_update::download_and_apply(state.adapter.ds_core(), &update_cfg, &view, &bin).await
}
