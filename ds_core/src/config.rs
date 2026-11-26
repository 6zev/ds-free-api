//! DeepSeek 核心配置 —— 独立于根 crate 的 Config
//!
//! 由根 crate 的 `Config` 构造转换而来。

/// ds_core 所需的配置（从根 crate Config 的子集构造）
#[derive(Debug, Clone)]
pub struct DsCoreConfig {
    pub api_base: String,
    pub wasm_url: String,
    pub user_agent: String,
    pub client_version: String,
    pub client_platform: String,
    pub client_locale: String,
    /// X-Client-Bundle-Id 请求头（真实客户端固定为 com.deepseek.chat）
    pub client_bundle_id: String,
    /// X-Device-Id 请求头（设备级 UUID，空 = 按 api_base 确定性派生兜底）
    pub client_device_id: String,
    /// X-Device-Model 请求头（真实 Web 客户端发空串）
    pub client_device_model: String,
    /// X-Client-Timezone-Offset 请求头（分钟或秒，真实 Web 客户端 UTC+8 发 28800）
    pub client_timezone_offset: String,
    /// 登录 payload 的 os 字段（真实 Web 客户端为 "web"，App 为 "android"）
    pub client_os: String,
    /// 传输层拟态档位（`okhttp4_12` = 原生 App，`chrome136` = 桌面 Chrome）
    pub emulation: String,
    /// 是否在 completion 请求上回传 `x-hif-leim` 风控令牌
    ///
    /// 真实客户端会轮询 `hif-leim.deepseek.com` / `hif-dliq.deepseek.com` 取令牌
    /// 并在 SSE 请求上带回；关闭仅用于对照实验（正常使用务必保持开启）。
    pub hif_enabled: bool,
    /// 启动时是否用一条 completion 做健康检查（默认 `true`）
    ///
    /// 真实客户端启动只做「登录 → check_device → 拉会话列表」，**不发消息**；
    /// 置 `false` 可对齐该序列（账号可用性仍由登录 + 禁言早检保证），
    /// 用于风控对照实验。
    pub startup_health_check: bool,
    /// 会话策略：`per_request`（默认，每轮新建、结束时删除）或 `reuse`
    ///
    /// 真实客户端一个会话长期复用、几乎不删。`reuse` 会保留会话并在后续请求中复用，
    /// 空闲超过 `session_idle_secs` 才删除。**复用语义尚未用真实账号验证**，
    /// 仅用于对照实验；未知取值按 `per_request` 处理。
    pub session_policy: String,
    /// 复用模式下会话的空闲回收秒数（0 = 只在进程退出时删除）
    pub session_idle_secs: u64,
    pub proxy_url: Option<String>,
    pub model_types: Vec<String>,
    pub input_character_limits: Vec<u32>,
    /// 每账号每小时请求上限（0 = 不限制）
    pub hourly_request_quota: u64,
    /// 同一账号两次请求之间的最小间隔秒数（0 = 不限制）
    ///
    /// 真实用户在一台设备上发消息的间隔远大于代理池的瞬时间隔；恒定速率的
    /// 「指令 → 回答」循环是明显的机器特征。启用后账号调度会跳过尚未到间隔的
    /// 账号，并在间隔上叠加随机抖动（见 `pool` 模块的 `INTERVAL_JITTER_PERMILLE`）。
    pub request_min_interval_secs: u64,
    /// 长思考保护：心跳 + 截断续写
    pub auto_resume: AutoResumeConfig,
    /// 登录请求体是否做客户端加密（默认 true）
    ///
    /// 上游 2026-10 起 `POST /api/v0/users/login` 不再接受 JSON 明文，必须发
    /// AES-128-CBC 密文 + `x-request-encryption-key` 头，否则 422（issue #118）。
    /// 仅在调试明文 body 时关闭。
    pub login_encryption: bool,
}

/// 长思考保护配置（心跳 + 截断续写）
#[derive(Debug, Clone, Default)]
pub struct AutoResumeConfig {
    /// 纯 thinking 静默期下发 SSE 注释心跳的间隔秒数（0 = 关闭，默认 0）
    ///
    /// 上游长思考时可能长时间不吐帧，客户端与代理之间的 nginx/CDN 会因「无字节」
    /// 先断开。启用后仅在上游 `Poll::Pending` 且处于思考阶段时，每 N 秒下发一条
    /// `: ping` 注释行（OpenAI/Anthropic 客户端都会忽略），不改变任何语义。
    pub thinking_heartbeat_secs: u64,
    /// 检测到「疑似被上游截断」时是否自动续写（默认 false）
    ///
    /// 判定条件见 `chat::response` 模块：已有思考内容但没有正文，且上游状态不是
    /// FINISHED（INCOMPLETE / WIP / 缺失），或 `event: close` 携带 `auto_resume=true`。
    /// 续写 = 同一 `chat_session_id` + `parent_message_id` 指向被截断的响应。
    /// **空 prompt 续写尚未用真实账号验证**，默认关闭以免污染对话。
    pub on_truncation: bool,
    /// 自动续写的最大次数（默认 2；0 = 不续写）
    pub max_attempts: u32,
    /// `true` = 续写用空 prompt（让服务端接着生成）；`false` = 发一条 `"continue"`
    pub empty_prompt: bool,
}

/// 单个账号配置
#[derive(Debug, Clone)]
pub struct AccountConfig {
    pub email: String,
    pub mobile: String,
    pub area_code: String,
    pub password: String,
    /// 浏览器设备指纹 ID。
    ///
    /// **实测为必填**：缺失会被登录风控直接拒绝（`RISK_DEVICE_DETECTED`，biz_code 11）。
    ///
    /// 建议**每个账号使用独立的 device_id**：设备级指纹被上游用于关联与画像，
    /// 同一指纹下挂多个账号、累计数百次请求后，账号会被禁言（`biz_code=5`）。
    pub device_id: String,
}
