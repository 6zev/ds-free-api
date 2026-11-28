//! DeepSeek HTTP 客户端 —— 原始 API 调用层
//!
//! 无状态管理：无缓存、无重试、无会话状态（唯一的状态是 HIF 风控令牌句柄，
//! 缓存逻辑在 `hif.rs`，本模块只负责把它拼进 SSE 请求头）。
//! 每个方法对应一个 REST 端点（详见 docs/ds-api-reference.md）。
//! 流方法（completion/edit_message）返回原始字节流，由上层解析 SSE。
//!
//! 仅包含最小业务逻辑：HTTP 错误码和业务错误码解析（into_result）。

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use log::warn;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use thiserror::Error;
use wreq::multipart::{Form, Part};
use wreq_util::Emulation;

// API 端点常量
const ENDPOINT_USERS_LOGIN: &str = "/users/login";
#[allow(dead_code)]
const ENDPOINT_USERS_CURRENT: &str = "/users/current";
const ENDPOINT_USERS_AUTH_TOKEN_CHECK_DEVICE: &str = "/users/auth_token/check_device";
const ENDPOINT_CHAT_SESSION_CREATE: &str = "/chat_session/create";
const ENDPOINT_CHAT_SESSION_DELETE: &str = "/chat_session/delete";
const ENDPOINT_CHAT_SESSION_FETCH_PAGE: &str = "/chat_session/fetch_page";
#[allow(dead_code)]
const ENDPOINT_CHAT_SESSION_UPDATE_TITLE: &str = "/chat_session/update_title";
const ENDPOINT_CHAT_CREATE_POW_CHALLENGE: &str = "/chat/create_pow_challenge";
const ENDPOINT_CHAT_COMPLETION: &str = "/chat/completion";
#[allow(dead_code)]
const ENDPOINT_CHAT_EDIT_MESSAGE: &str = "/chat/edit_message";
const ENDPOINT_CHAT_STOP_STREAM: &str = "/chat/stop_stream";
const ENDPOINT_FILE_UPLOAD: &str = "/file/upload_file";
const ENDPOINT_FILE_FETCH: &str = "/file/fetch_files";

#[derive(Debug, Error)]
pub enum ClientError {
    /// HTTP 层错误（网络、超时、DNS 等）
    #[error("HTTP error: {0}")]
    Http(#[from] wreq::Error),

    /// HTTP 状态码非 2xx
    #[error("HTTP status {status}: {body}")]
    Status { status: u16, body: String },

    /// 业务错误：API 返回 HTTP 200 但 biz_code 非 0
    #[error("Business error: code={code}, msg={msg}")]
    Business { code: i64, msg: String },

    /// JSON 解析失败
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    /// Header 值包含非法字符
    #[error("Invalid header value: {0}")]
    InvalidHeader(String),
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    code: i64,
    msg: String,
    data: Option<EnvelopeData<T>>,
}

#[derive(Debug, Deserialize)]
struct EnvelopeData<T> {
    biz_code: i64,
    biz_msg: String,
    /// 先当 `Value` 读，再转成 `T`：上游「无返回值」接口会给 `{}` 而不是 `null`
    biz_data: Option<serde_json::Value>,
    #[serde(skip)]
    _marker: std::marker::PhantomData<T>,
}

impl<T: serde::de::DeserializeOwned> Envelope<T> {
    fn into_result(self) -> Result<T, ClientError> {
        if self.code != 0 {
            return Err(ClientError::Business {
                code: self.code,
                msg: self.msg,
            });
        }
        let data = self.data.ok_or_else(|| ClientError::Business {
            code: -1,
            msg: "missing data".into(),
        })?;
        if data.biz_code != 0 {
            return Err(ClientError::Business {
                code: data.biz_code,
                msg: data.biz_msg,
            });
        }
        // 上游的「无返回值」接口两种形态都出现过：`biz_data: null`，以及
        // `biz_data: {}`（`stop_stream` / `delete_session` 实测返回后者）。
        // 旧实现把「有没有数据」直接绑到 `T` 上，`{}` 于是撞上
        // `invalid type: map, expected unit`，正常停止被记成失败。
        // 现在统一走「Value → T」的反序列化：`Option<()>` 接受 null 与 map，
        // 其它类型照旧由 serde 报错，语义不变。
        let value = match data.biz_data.unwrap_or(serde_json::Value::Null) {
            // 空对象是「无返回值」的另一种写法：`Option<()>` 会把它按
            // `Some(())` 解析，`Option<Option<()>>` 才能整体落成 `None`。
            serde_json::Value::Object(map) if map.is_empty() => serde_json::Value::Null,
            other => other,
        };
        serde_json::from_value(value).map_err(ClientError::Json)
    }
}

/// 登录 payload
///
/// 字段与真实客户端一一对应：`email` / `mobile` 二选一，另一个传空串
/// （**不是省略也不是 null** —— 真实客户端固定发 6 个 key）。
#[derive(Debug, Serialize)]
pub struct LoginPayload {
    pub email: String,
    pub mobile: String,
    pub password: String,
    pub area_code: String,
    pub device_id: String,
    pub os: String,
}

#[derive(Debug, Deserialize)]
pub struct LoginData {
    pub code: i64,
    pub msg: String,
    pub user: UserInfo,
}

/// 账号禁言状态（登录响应与 /users/current 均返回）
#[derive(Debug, Clone, Deserialize)]
pub struct ChatStatus {
    /// 1 = 被禁言；接受 int / bool 两种形态
    #[serde(default, deserialize_with = "de_muted_flag")]
    pub is_muted: i64,
    /// 解封时间戳（Unix 秒，浮点）；None = 未设置
    #[serde(default)]
    pub mute_until: Option<f64>,
}

/// 兼容 int(0/1) 与 bool 两种 is_muted 形态
fn de_muted_flag<'de, D>(d: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(d)?;
    Ok(match v {
        serde_json::Value::Bool(b) => i64::from(b),
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        _ => 0,
    })
}

#[derive(Debug, Deserialize)]
pub struct UserInfo {
    pub id: String,
    pub token: String,
    pub email: Option<String>,
    pub mobile_number: Option<String>,
    /// 禁言状态（缺失视为未禁言）
    #[serde(default)]
    pub chat: Option<ChatStatus>,
}

/// POST /users/auth_token/check_device 响应
#[derive(Debug, Deserialize)]
pub struct CheckDeviceData {
    /// 服务端下发的令牌轮换指令（通常为 null）
    pub rotate: Option<serde_json::Value>,
}

/// GET /chat_session/fetch_page 响应中的单个会话
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatSessionInfo {
    pub id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub title_type: Option<String>,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub model_type: Option<String>,
    /// 更新时间戳（Unix 秒，浮点）—— 分页游标
    #[serde(default)]
    pub updated_at: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct FetchSessionsData {
    #[serde(default)]
    pub chat_sessions: Vec<ChatSessionInfo>,
    #[serde(default)]
    pub has_more: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct CreateSessionData {
    pub id: String,
}

// 包装类型：biz_data 里面嵌套了 chat_session 对象
#[derive(Debug, Deserialize)]
struct CreateSessionWrapper {
    chat_session: CreateSessionData,
}

#[derive(Debug, Deserialize)]
pub struct UploadFileData {
    pub id: String,
    #[allow(dead_code)]
    pub status: String,
    #[allow(dead_code)]
    pub file_name: String,
    #[allow(dead_code)]
    pub file_size: i64,
}

#[derive(Debug, Deserialize)]
pub struct FetchFilesData {
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Deserialize)]
pub struct FileInfo {
    #[allow(dead_code)]
    pub id: String,
    pub status: String,
    pub file_name: String,
    #[allow(dead_code)]
    pub file_size: i64,
    #[serde(default)]
    pub token_usage: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ChallengeData {
    pub algorithm: String,
    pub challenge: String,
    pub salt: String,
    pub signature: String,
    pub difficulty: i64,
    #[allow(dead_code)]
    pub expire_after: i64,
    pub expire_at: i64,
    pub target_path: String,
}

// 包装类型：biz_data 里面嵌套了 challenge 对象
#[derive(Debug, Deserialize)]
struct ChallengeWrapper {
    challenge: ChallengeData,
}

#[derive(Debug, Serialize)]
pub struct CompletionPayload {
    pub chat_session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_message_id: Option<i64>,
    pub model_type: String,
    pub prompt: String,
    pub ref_file_ids: Vec<String>,
    pub thinking_enabled: bool,
    pub search_enabled: bool,
    /// 真实客户端固定发送该字段（会话首条为 null）
    pub action: Option<serde_json::Value>,
    /// 消息来源（官方客户端的渠道标记）
    ///
    /// 2026-10-07 从前端 bundle 提取：真实客户端的 completion payload 一定带
    /// `source`。早期按「输入框回车」推断为 `input`，但 2026-11-26 实测上游
    /// 已不接受该取值 —— completion 直接 422
    /// `unknown variant \`input\`, expected \`default\` or \`landing\``
    /// 并连带把账号标记为 Error。现按上游实际枚举发 `default`（App / 会话内消息
    /// 的主路径），见 `docs/development.md` 的 422 记录。
    pub source: &'static str,
    pub preempt: bool,
}

/// 代理侧统一使用的 `source` 取值
///
/// 上游只接受 `default` / `landing`；旧值 `input` 会让 `/chat/completion` 返回
/// HTTP 422（`unknown variant \`input\`），因此对齐为 `default`。
pub const COMPLETION_SOURCE_DEFAULT: &str = "default";

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct EditMessagePayload {
    pub chat_session_id: String,
    pub message_id: i64,
    pub prompt: String,
    pub search_enabled: bool,
    pub thinking_enabled: bool,
    pub model_type: String,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct UpdateTitlePayload {
    pub chat_session_id: String,
    pub title: String,
}

#[derive(Debug, Serialize)]
pub struct StopStreamPayload {
    pub chat_session_id: String,
    pub message_id: i64,
}

/// Check if a response is an AWS WAF Challenge (US IP restriction)
fn is_waf_challenge(resp: &wreq::Response) -> bool {
    resp.status().as_u16() == 202 && resp.headers().get("x-amzn-waf-action").is_some()
}

/// Print a hint when WAF challenge is detected
fn print_waf_hint() {
    warn!(target: "ds_core::client", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    warn!(target: "ds_core::client", "  AWS WAF Challenge detected.");
    warn!(target: "ds_core::client", "  DeepSeek CloudFront WAF blocks US-based IPs.");
    warn!(target: "ds_core::client", "  Rust HTTP clients can't execute the JS challenge.");
    warn!(target: "ds_core::client", "");
    warn!(target: "ds_core::client", "  To fix this, configure a non-US proxy in config.toml:");
    warn!(target: "ds_core::client", "    [proxy]");
    warn!(target: "ds_core::client", "    url = \"http://127.0.0.1:7890\"");
    warn!(target: "ds_core::client", "");
    warn!(target: "ds_core::client", "  https://github.com/niyue/ds-free-api");
    warn!(target: "ds_core::client", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}

/// 默认 HIF（风控令牌）取值端点
const DEFAULT_HIF_LEIM_URL: &str = "https://hif-leim.deepseek.com/query";
/// 第二个风控令牌端点（前端 `dliqPoller`；本网络常为 NXDOMAIN，取不到就跳过）
const DEFAULT_HIF_DLIQ_URL: &str = "https://hif-dliq.deepseek.com/query";

/// 传输层拟态档位（TLS/HTTP2 指纹 + 该档位自带的默认请求头）
///
/// 关键在于**自洽**：安卓 App UA 配 Chrome/macOS 的 client hints 与
/// 「地址栏导航」头（`sec-fetch-dest: document`、`accept: text/html,…`）是
/// 任何真实客户端都不会有的组合（2026-10-07 本地 echo server 取证）。
///
/// **默认档位是安卓 App**（`okhttp4_12`）：AWS WAF 对桌面浏览器身份下发 JS challenge
/// （HTTP 202），本 HTTP 客户端无法执行 JS，因此 web 身份在 datacenter/直连 IP 上必被拦；
/// 安卓 App 身份 login 为明文、无需过 JS challenge，是与服务端「App 客户端」对齐的现实选择
/// （见 `docs/login-encryption-audit.md` 方案 B、`ds_core/raw-api-reference.md`「WAF」节）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EmulationProfile {
    /// 原生安卓 App（OkHttp）：默认，与 `user_agent = "DeepSeek/… Android/…"`
    /// + `client_platform = android` 自洽
    #[default]
    OkHttp4_12,
    /// 桌面 Chrome：带 `sec-ch-ua*` / `sec-fetch-*` / 文档 accept，
    /// 需要同时把 UA / `client_platform` / `client_os` 切成 web 才自洽。
    /// 注意：AWS WAF 对该身份会下发 JS challenge（202），本客户端无法通过。
    Chrome136,
}

impl EmulationProfile {
    /// 解析配置值（未知值返回 None，由调用方告警并回退默认档位）
    #[must_use]
    pub fn from_config(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "okhttp4_12" | "okhttp_4_12" | "okhttp" | "app" => Some(Self::OkHttp4_12),
            "chrome136" | "chrome_136" | "chrome" | "web" => Some(Self::Chrome136),
            _ => None,
        }
    }

    const fn to_wreq(self) -> Emulation {
        match self {
            Self::OkHttp4_12 => Emulation::OkHttp4_12,
            Self::Chrome136 => Emulation::Chrome136,
        }
    }
}

/// 请求的发起语境（决定 `sec-fetch-site`）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchSite {
    /// 同源 XHR（`chat.deepseek.com` 的业务 API）
    SameOrigin,
    /// 跨源 XHR（`hif-leim.deepseek.com` 之类的风控令牌端点）
    CrossSite,
}

impl FetchSite {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SameOrigin => "same-origin",
            Self::CrossSite => "cross-site",
        }
    }
}

/// 客户端拟态身份（UA + x-client-* 请求头 + 登录 payload 的 os）
#[derive(Debug, Clone, Default)]
pub struct ClientIdentity {
    pub user_agent: String,
    pub client_version: String,
    pub client_platform: String,
    pub client_locale: String,
    pub client_bundle_id: String,
    /// X-Device-Id（设备级 UUID）；空 = 按 api_base 确定性派生（兜底，
    /// 根 crate 会在首启生成随机 UUID 并写入配置）
    pub device_id: String,
    pub device_model: String,
    pub timezone_offset: String,
    /// 登录 payload 的 os 字段
    pub client_os: String,
}

impl ClientIdentity {
    /// 客户端通用请求头（登录、鉴权请求与 HIF 取令牌共用）
    ///
    /// 对齐真实客户端（2026-10-07 真实浏览器抓包）：除 UA 外还带 7 个 x-* 头，
    /// 缺这些头会明显拉低「像真实客户端」的拟态保真度。
    ///
    /// **同时显式覆盖传输层档位自带的「地址栏导航」头**：wreq 的 `chrome136`
    /// 档默认发 `sec-fetch-dest: document` / `sec-fetch-mode: navigate` /
    /// `sec-fetch-site: none` / `accept: text/html,…` / `priority`，
    /// 且 `sec-ch-ua-platform` 固定 `"macOS"`（与 Windows UA 矛盾）——
    /// 而真实客户端的业务 API 是页面内的 **XHR/fetch**
    /// （抓包：`accept: */*`、`sec-ch-ua-platform: "Windows"`、无 `priority`）。
    /// 本地 echo server 逐头比对见 `docs/development.md`。
    fn headers(&self, site: FetchSite) -> Result<wreq::header::HeaderMap, ClientError> {
        let mut h = wreq::header::HeaderMap::new();
        let mut put = |name: &'static str, value: &str| -> Result<(), ClientError> {
            h.insert(
                name,
                wreq::header::HeaderValue::from_str(value)
                    .map_err(|e| ClientError::InvalidHeader(format!("{name}: {e}")))?,
            );
            Ok(())
        };
        put(wreq::header::USER_AGENT.as_str(), &self.user_agent)?;
        put("X-Client-Version", &self.client_version)?;
        put("X-Client-Platform", &self.client_platform)?;
        put("X-Client-Locale", &self.client_locale)?;
        put("X-Client-Bundle-Id", &self.client_bundle_id)?;
        put("X-Device-Id", self.device_id.trim())?;
        put("X-Device-Model", &self.device_model)?;
        put("X-Client-Timezone-Offset", &self.timezone_offset)?;
        // 默认头里的 accept-language 固定 en-US，与 client_locale（默认 zh_CN）不符
        if !self.client_locale.is_empty() {
            put(
                wreq::header::ACCEPT_LANGUAGE.as_str(),
                &self.client_locale.replace('_', "-"),
            )?;
        }
        // 官方 Web 客户端对每个 API 请求都带 Referer（抓包：https://chat.deepseek.com/）
        put(wreq::header::REFERER.as_str(), "https://chat.deepseek.com/")?;
        // —— 覆盖档位默认的「导航头」，改成页面内 fetch/XHR 的真实形态 ——
        put(wreq::header::ACCEPT.as_str(), "*/*")?;
        put("Sec-Fetch-Dest", "empty")?;
        put("Sec-Fetch-Mode", "cors")?;
        put("Sec-Fetch-Site", site.as_str())?;
        // client hints 必须与 UA 自洽（UA 是 Windows Chrome/140）
        put(
            "Sec-Ch-Ua",
            "\"Chromium\";v=\"140\", \"Not_A Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
        )?;
        put("Sec-Ch-Ua-Mobile", "?0")?;
        put("Sec-Ch-Ua-Platform", "\"Windows\"")?;
        Ok(h)
    }
}

/// HIF 风控令牌配置
#[derive(Debug, Clone)]
pub struct HifConfig {
    /// 是否在 completion 请求上回传 `x-hif-leim` / `x-hif-dliq`（关闭仅用于对照实验）
    pub enabled: bool,
    /// `x-hif-leim` 取令牌端点
    pub leim_url: String,
    /// `x-hif-dliq` 取令牌端点（真实客户端的第二个轮询器）
    pub dliq_url: String,
}

impl Default for HifConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            leim_url: DEFAULT_HIF_LEIM_URL.to_string(),
            dliq_url: DEFAULT_HIF_DLIQ_URL.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct DsClient {
    http: wreq::Client,
    api_base: String,
    wasm_url: String,
    identity: ClientIdentity,
    /// HIF 风控令牌注册表（`x-hif-leim` + `x-hif-dliq`，按设备身份分桶）；None = 已关闭
    hif: Option<std::sync::Arc<super::hif::HifRegistry>>,
}

impl DsClient {
    #[must_use]
    pub fn new(
        api_base: String,
        wasm_url: String,
        mut identity: ClientIdentity,
        hif: &HifConfig,
        emulation: EmulationProfile,
        proxy_url: Option<&str>,
        proxy_bypass_hosts: &[String],
    ) -> Self {
        let mut builder = wreq::Client::builder().emulation(emulation.to_wreq());
        if let Some(url) = proxy_url.and_then(|u| wreq::Proxy::all(u).ok()) {
            // `no_proxy`：SOCKS5 按域名转发时由代理端解析，本地 hosts 不生效，
            // 所以本地 mock / 自建中转必须在这里显式跳过。
            let proxy = if proxy_bypass_hosts.is_empty() {
                url
            } else {
                url.no_proxy(wreq::NoProxy::from_string(&proxy_bypass_hosts.join(",")))
            };
            builder = builder.proxy(proxy);
        }
        // 设备身份必须由调用方显式提供：真实 Web 客户端的 device_id 是浏览器登录时
        // 由数美 SDK 生成的，拿不到就登录不了（biz_code 11）。这里再「按 api_base 派生
        // 一个」只是把同一个假身份发给所有账号 —— 假指纹本身就是风险信号，而且会让
        // 多个账号在上游侧被聚成同一台设备。真正生效的是 per-account 派生
        // （pool::account_x_device_id）或显式配置的 client_device_id；两者都没配时
        // 这里退化成空占位，由账号初始化阶段给出告警。
        identity.device_id = identity.device_id.trim().to_string();
        if identity.device_id.is_empty() {
            warn!(
                target: "ds_core::client",
                "未配置 client_device_id，且该客户端视图未绑定账号设备身份；\
                 请在 config.toml / 管理面板为每个账号填写真实浏览器的 device_id，\
                 否则登录将被上游以 RISK_DEVICE_DETECTED(biz_code=11) 拒绝"
            );
        }

        let http = builder.build().expect("构建 HTTP 客户端失败");
        let hif = hif.enabled.then(|| {
            let headers = identity.headers(FetchSite::CrossSite).unwrap_or_else(|e| {
                warn!(target: "ds_core::client", "客户端头构造失败，HIF 取令牌将不带拟态头: {e}");
                wreq::header::HeaderMap::new()
            });
            super::hif::HifRegistry::new(
                http.clone(),
                hif.leim_url.clone(),
                hif.dliq_url.clone(),
                headers,
            )
        });

        Self {
            http,
            api_base,
            wasm_url,
            identity,
            hif,
        }
    }

    /// 测试用：构造一个不联网、不带 HIF 的客户端（仅流层单测需要占位）。
    #[cfg(test)]
    pub(crate) fn offline_for_test() -> Self {
        Self {
            http: wreq::Client::new(),
            api_base: "http://127.0.0.1:1".to_owned(),
            wasm_url: String::new(),
            identity: ClientIdentity::default(),
            hif: None,
        }
    }

    /// 登录 payload 使用的 os 值
    pub(crate) fn client_os(&self) -> &str {
        &self.identity.client_os
    }

    /// 返回绑定到指定设备身份（`X-Device-Id`）的客户端视图
    ///
    /// 真实 Web 客户端是「一个浏览器 profile = 一个数美 `device_id` + 一个
    /// `X-Device-Id`」，HIF 风控令牌也按设备下发。因此代理侧同样**按账号**
    /// 派生设备身份：多账号共用同一个 `X-Device-Id` / 同一个令牌，等于告诉
    /// 上游「这些账号来自同一台设备」。
    ///
    /// 返回的客户端与原客户端共享 HTTP 连接池与令牌注册表，仅设备身份不同。
    #[must_use]
    pub fn scoped_to(&self, x_device_id: &str) -> Self {
        let mut scoped = self.clone();
        if !x_device_id.trim().is_empty() {
            scoped.identity.device_id = x_device_id.trim().to_string();
        }
        scoped
    }

    /// 当前视图的设备身份（X-Device-Id）
    #[must_use]
    pub fn device_id(&self) -> &str {
        &self.identity.device_id
    }

    /// 预热 HIF 风控令牌（真实客户端在应用启动时即开始轮询两个端点）
    pub async fn warm_up_hif(&self) {
        if let Some(hif) = &self.hif {
            hif.warm_up(&self.identity.device_id).await;
        }
    }

    /// 读取当前设备身份的 `x-hif-leim`（诊断用；缺失时触发一次取令牌）
    pub async fn hif_token(&self) -> Option<String> {
        let hif = self.hif.as_ref()?;
        let tokens = hif.tokens_for(&self.identity.device_id);
        tokens
            .values()
            .await
            .into_iter()
            .find(|(kind, _)| *kind == super::hif::HifKind::Leim)
            .map(|(_, value)| value)
    }

    /// 客户端通用请求头（登录与鉴权请求共用）
    fn client_headers(&self) -> Result<wreq::header::HeaderMap, ClientError> {
        self.identity.headers(FetchSite::SameOrigin)
    }

    /// 在 completion（SSE）请求上追加风控令牌头（`x-hif-leim` + `x-hif-dliq`）
    ///
    /// 真实客户端只在 SSE 请求上带这两个头（前端 `addSSEHeader`），
    /// 且两个令牌由同一个头提供者一起附加；缺失时上游可直接判定为非官方客户端。
    async fn attach_hif(&self, headers: &mut wreq::header::HeaderMap) -> Result<(), ClientError> {
        if let Some(hif) = &self.hif {
            let values = hif.tokens_for(&self.identity.device_id).values().await;
            super::hif::insert_tokens(headers, &values)?;
        }
        Ok(())
    }

    fn auth_headers(&self, token: &str) -> Result<wreq::header::HeaderMap, ClientError> {
        let mut h = self.client_headers()?;
        h.insert(
            wreq::header::AUTHORIZATION,
            wreq::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| ClientError::InvalidHeader(format!("Authorization: {e}")))?,
        );
        Ok(h)
    }

    fn auth_headers_with_pow(
        &self,
        token: &str,
        pow_response: &str,
    ) -> Result<wreq::header::HeaderMap, ClientError> {
        let mut h = self.auth_headers(token)?;
        h.insert(
            "X-Ds-Pow-Response",
            wreq::header::HeaderValue::from_str(pow_response)
                .map_err(|e| ClientError::InvalidHeader(format!("X-Ds-Pow-Response: {e}")))?,
        );
        Ok(h)
    }

    async fn parse_envelope<T: serde::de::DeserializeOwned>(
        resp: wreq::Response,
    ) -> Result<T, ClientError> {
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let envelope: Envelope<T> = resp.json().await?;
        envelope.into_result()
    }

    /// 登录
    ///
    /// 2026-10 起上游要求请求体加密：body = AES-128-CBC(PKCS#7) 密文，
    /// 包裹 AES key/IV 的 RSA-OAEP-SHA256 结果放进 `x-request-encryption-key`。
    /// 明文 JSON 会被 422 拒绝（issue #118）。加密后仍保留原有请求头集合。
    pub async fn login(
        &self,
        payload: &LoginPayload,
        encryption_enabled: bool,
        pow_header: Option<&str>,
    ) -> Result<LoginData, ClientError> {
        let mut h = self.client_headers()?;
        h.insert(
            wreq::header::REFERER,
            wreq::header::HeaderValue::from_static("https://chat.deepseek.com/sign_in"),
        );
        if let Some(pow) = pow_header {
            h.insert(
                "x-ds-challenge-response",
                wreq::header::HeaderValue::from_str(pow).map_err(|e| {
                    ClientError::InvalidHeader(format!("x-ds-challenge-response: {e}"))
                })?,
            );
        }

        let plaintext = serde_json::to_vec(payload)?;
        let request = self
            .http
            .post(format!("{}{}", self.api_base, ENDPOINT_USERS_LOGIN))
            .headers(h);

        let request = if encryption_enabled {
            let encrypted =
                super::crypto::encrypt_body(&plaintext, super::crypto::PROD_PUBLIC_KEY_B64)
                    .map_err(|e| ClientError::InvalidHeader(format!("login encryption: {e}")))?;
            request
                .header(
                    "x-request-encryption-key",
                    wreq::header::HeaderValue::from_str(&encrypted.key_header).map_err(|e| {
                        ClientError::InvalidHeader(format!("x-request-encryption-key: {e}"))
                    })?,
                )
                .header(wreq::header::CONTENT_TYPE, "application/json")
                .body(encrypted.body)
        } else {
            request.json(payload)
        };

        let resp = request.send().await?;

        if is_waf_challenge(&resp) {
            print_waf_hint();
            return Err(ClientError::Status {
                status: 202,
                body: "WAF Challenge: use a non-US proxy".into(),
            });
        }

        Self::parse_envelope::<LoginData>(resp).await
    }

    /// 当前登录用户信息（含禁言状态）
    ///
    /// 与登录响应的 user 字段同构；探测禁言优先用登录响应（零额外请求），
    /// 此方法保留作原始客户端 API 对称面（同 `update_title` / `edit_message`）。
    #[allow(dead_code)]
    pub async fn current_user(&self, token: &str) -> Result<UserInfo, ClientError> {
        let resp = self
            .http
            .get(format!("{}{}", self.api_base, ENDPOINT_USERS_CURRENT))
            .headers(self.authed_headers(token).await?)
            .send()
            .await?;
        Self::parse_envelope::<UserInfo>(resp).await
    }

    /// 设备校验 / 令牌轮换检查
    ///
    /// 真实客户端登录成功后立即调用；`rotate` 非 null 时服务端要求轮换令牌
    /// （可能是字符串或含 token 字段的对象，由调用方解析）。
    pub async fn check_device(&self, token: &str) -> Result<CheckDeviceData, ClientError> {
        let resp = self
            .http
            .post(format!(
                "{}{}",
                self.api_base, ENDPOINT_USERS_AUTH_TOKEN_CHECK_DEVICE
            ))
            .headers(self.authed_headers(token).await?)
            .json(&serde_json::json!({
                "device_id": self.identity.device_id,
                "device_model": self.identity.device_model,
            }))
            .send()
            .await?;
        Self::parse_envelope::<CheckDeviceData>(resp).await
    }

    /// 拉取账号的会话列表（分页）
    ///
    /// `updated_at` 为上一页末尾会话的更新时间戳（Unix 秒），None = 第一页。
    /// 真实客户端游标序列化在 `lte_cursor` 对象下（`lte_cursor.pinned=false`）。
    pub async fn fetch_sessions(
        &self,
        token: &str,
        updated_at: Option<f64>,
    ) -> Result<FetchSessionsData, ClientError> {
        let mut query: Vec<(&str, String)> = vec![("lte_cursor.pinned", "false".to_string())];
        if let Some(ts) = updated_at {
            query.push(("lte_cursor.updated_at", ts.to_string()));
        }
        let resp = self
            .http
            .get(format!(
                "{}{}",
                self.api_base, ENDPOINT_CHAT_SESSION_FETCH_PAGE
            ))
            .headers(self.authed_headers(token).await?)
            .query(&query)
            .send()
            .await?;
        Self::parse_envelope::<FetchSessionsData>(resp).await
    }

    pub async fn create_session(&self, token: &str) -> Result<String, ClientError> {
        let resp = self
            .http
            .post(format!("{}{}", self.api_base, ENDPOINT_CHAT_SESSION_CREATE))
            .headers(self.authed_headers(token).await?)
            .json(&serde_json::json!({}))
            .send()
            .await?;
        let wrapper: CreateSessionWrapper = Self::parse_envelope(resp).await?;
        let data = wrapper.chat_session;
        Ok(data.id)
    }

    pub async fn delete_session(&self, token: &str, session_id: &str) -> Result<(), ClientError> {
        let resp = self
            .http
            .post(format!("{}{}", self.api_base, ENDPOINT_CHAT_SESSION_DELETE))
            .headers(self.authed_headers(token).await?)
            .json(&serde_json::json!({ "chat_session_id": session_id }))
            .send()
            .await?;
        Self::parse_envelope::<Option<()>>(resp).await?;
        Ok(())
    }

    pub async fn create_pow_challenge(
        &self,
        token: &str,
        target_path: &str,
    ) -> Result<ChallengeData, ClientError> {
        let headers = self.pow_challenge_headers(Some(token)).await?;
        let resp = self
            .http
            .post(format!(
                "{}{}",
                self.api_base, ENDPOINT_CHAT_CREATE_POW_CHALLENGE
            ))
            .headers(headers)
            .json(&serde_json::json!({ "target_path": target_path }))
            .send()
            .await?;
        let wrapper: ChallengeWrapper = Self::parse_envelope(resp).await?;
        let challenge = wrapper.challenge;
        Ok(challenge)
    }

    /// 带风控令牌的业务请求头：鉴权 + `x-hif-leim` / `x-hif-dliq`
    ///
    /// 真实客户端把风险令牌附加到**每个**业务请求上，不只是 SSE。统一的入口
    /// 让所有带 Bearer 的调用点都不必各自记得补 `attach_hif`。
    async fn authed_headers(&self, token: &str) -> Result<wreq::header::HeaderMap, ClientError> {
        let mut headers = self.auth_headers(token)?;
        self.attach_hif(&mut headers).await?;
        Ok(headers)
    }

    /// 带风控令牌 + PoW 与鉴权的业务请求头
    ///
    /// 上传等需要 PoW 的非流式请求走这里：先拿鉴权与风控令牌，再追加
    /// `X-Ds-Pow-Response`。缺风控令牌的请求会被上游/代理静默丢弃。
    async fn authed_headers_with_pow(
        &self,
        token: &str,
        pow_response: &str,
    ) -> Result<wreq::header::HeaderMap, ClientError> {
        let mut headers = self.authed_headers(token).await?;
        headers.insert(
            "X-Ds-Pow-Response",
            wreq::header::HeaderValue::from_str(pow_response)
                .map_err(|e| ClientError::InvalidHeader(format!("X-Ds-Pow-Response: {e}")))?,
        );
        Ok(headers)
    }

    /// PoW challenge 请求头：鉴权（可选）+ 风控令牌（`x-hif-leim` / `x-hif-dliq`）
    ///
    /// 真实客户端把风险令牌附加到**每个**业务请求上，challenge 也不例外；
    /// 漏掉它时该请求可能被上游/代理静默丢弃，表现为「续写一直 pending」。
    async fn pow_challenge_headers(
        &self,
        token: Option<&str>,
    ) -> Result<wreq::header::HeaderMap, ClientError> {
        if let Some(token) = token {
            return self.authed_headers(token).await;
        }
        let mut headers = self.client_headers()?;
        self.attach_hif(&mut headers).await?;
        Ok(headers)
    }

    /// guest PoW challenge（登录前匿名获取，不带 Authorization）
    ///
    /// 真实 Web 客户端对白名单路径（含 `users/login`）先解一次 guest challenge，
    /// 结果写入 `x-ds-challenge-response`。与 `create_pow_challenge` 走同一端点，
    /// 只是缺少 Bearer token。
    pub async fn create_guest_pow_challenge(
        &self,
        target_path: &str,
    ) -> Result<ChallengeData, ClientError> {
        let resp = self
            .http
            .post(format!(
                "{}{}",
                self.api_base, ENDPOINT_CHAT_CREATE_POW_CHALLENGE
            ))
            .headers(self.pow_challenge_headers(None).await?)
            .json(&serde_json::json!({ "target_path": target_path }))
            .send()
            .await?;
        let wrapper: ChallengeWrapper = Self::parse_envelope(resp).await?;
        Ok(wrapper.challenge)
    }

    pub async fn completion(
        &self,
        token: &str,
        pow_response: &str,
        payload: &CompletionPayload,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>, ClientError> {
        let mut headers = self.auth_headers_with_pow(token, pow_response)?;
        self.attach_hif(&mut headers).await?;
        let resp = self
            .http
            .post(format!("{}{}", self.api_base, ENDPOINT_CHAT_COMPLETION))
            .headers(headers)
            .json(payload)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::Status {
                status: status.as_u16(),
                body,
            });
        }

        Ok(Box::pin(resp.bytes_stream().map_err(ClientError::Http)))
    }

    /// 用本客户端（同一拟态身份 / 代理）取任意 URL 的文本
    ///
    /// 只用于非 DeepSeek 域的辅助请求（GitHub Releases、镜像）。不复用
    /// `parse_envelope`：这些域返回的是裸 JSON / 文本，没有上游信封。
    pub async fn get_text(&self, url: &str) -> Result<(u16, String), ClientError> {
        let resp = self
            .http
            .get(url)
            .headers(self.identity.headers(FetchSite::CrossSite)?)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Ok((status, body))
    }

    /// 用本客户端取任意 URL 的原始字节（发布产物下载）
    pub async fn get_bytes(&self, url: &str) -> Result<Vec<u8>, ClientError> {
        let resp = self
            .http
            .get(url)
            .headers(self.identity.headers(FetchSite::CrossSite)?)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(ClientError::Status {
                status: status.as_u16(),
                body: format!("HTTP {status}"),
            });
        }
        let bytes = resp.bytes().await?;
        Ok(bytes.to_vec())
    }

    #[allow(dead_code)]
    pub async fn edit_message(
        &self,
        token: &str,
        pow_response: &str,
        payload: &EditMessagePayload,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>, ClientError> {
        let mut headers = self.auth_headers_with_pow(token, pow_response)?;
        self.attach_hif(&mut headers).await?;
        let resp = self
            .http
            .post(format!("{}{}", self.api_base, ENDPOINT_CHAT_EDIT_MESSAGE))
            .headers(headers)
            .json(payload)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::Status {
                status: status.as_u16(),
                body,
            });
        }

        Ok(Box::pin(resp.bytes_stream().map_err(ClientError::Http)))
    }

    #[allow(dead_code)]
    pub async fn update_title(
        &self,
        token: &str,
        payload: &UpdateTitlePayload,
    ) -> Result<(), ClientError> {
        let resp = self
            .http
            .post(format!(
                "{}{}",
                self.api_base, ENDPOINT_CHAT_SESSION_UPDATE_TITLE
            ))
            .headers(self.authed_headers(token).await?)
            .json(payload)
            .send()
            .await?;
        Self::parse_envelope::<serde::de::IgnoredAny>(resp).await?;
        Ok(())
    }

    /// 取消正在进行的流式输出，不需要 PoW
    pub async fn stop_stream(
        &self,
        token: &str,
        payload: &StopStreamPayload,
    ) -> Result<(), ClientError> {
        let resp = self
            .http
            .post(format!("{}{}", self.api_base, ENDPOINT_CHAT_STOP_STREAM))
            .headers(self.authed_headers(token).await?)
            .json(payload)
            .send()
            .await?;
        Self::parse_envelope::<Option<()>>(resp).await?;
        Ok(())
    }

    /// 上传文件，返回文件元数据（id, status 等）
    pub async fn upload_file(
        &self,
        token: &str,
        pow_response: &str,
        filename: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> Result<UploadFileData, ClientError> {
        let part = Part::bytes(bytes)
            .file_name(filename.to_string())
            .mime_str(content_type)?;
        let form = Form::new().part("file", part);

        let resp = self
            .http
            .post(format!("{}{}", self.api_base, ENDPOINT_FILE_UPLOAD))
            .headers(self.authed_headers_with_pow(token, pow_response).await?)
            .multipart(form)
            .send()
            .await?;
        Self::parse_envelope::<UploadFileData>(resp).await
    }

    /// 查询文件状态，返回文件列表（含 status: PENDING/SUCCESS/FAILED）
    pub async fn fetch_files(
        &self,
        token: &str,
        file_ids: &[String],
    ) -> Result<FetchFilesData, ClientError> {
        let ids = file_ids.join(",");
        let resp = self
            .http
            .get(format!("{}{}", self.api_base, ENDPOINT_FILE_FETCH))
            .headers(self.authed_headers(token).await?)
            .query(&[("file_ids", &ids)])
            .send()
            .await?;
        Self::parse_envelope::<FetchFilesData>(resp).await
    }

    pub async fn get_wasm(&self) -> Result<Bytes, ClientError> {
        // 本地样本/离线回归：允许直接用磁盘上的 wasm，避免启动强依赖外网
        if let Some(path) = self.wasm_url.strip_prefix("file://") {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|e| ClientError::Status {
                    status: 0,
                    body: format!("读取本地 wasm 失败 ({path}): {e}"),
                })?;
            return Ok(Bytes::from(bytes));
        }
        let resp = self.http.get(&self.wasm_url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::Status {
                status: status.as_u16(),
                body,
            });
        }
        Ok(resp.bytes().await?)
    }
}

/// 由种子字节确定性派生 UUID（RFC 4122 v4 格式）
///
/// 用途：**按账号派生 X-Device-Id**（`ds-free-api:x-device-id:{数美 device_id}`）——
/// 稳定且每账号唯一，对齐真实客户端「一个浏览器 profile 一个设备身份」。
/// 注意：**不**用于「账号没配 device_id 时兜底造一个」——假指纹对上游是风险信号，
/// 且会让多个账号被聚成同一台设备；缺失时宁可报错要求补齐。
pub(crate) fn derive_device_uuid(seed: &[u8]) -> String {
    fn fnv1a(data: &[u8], offset: u64) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325u64 ^ offset;
        for &b in data {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }
    let hi = fnv1a(seed, 0);
    let lo = fnv1a(seed, 0x9e37_79b9_7f4a_7c15);
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&hi.to_be_bytes());
    b[8..].copy_from_slice(&lo.to_be_bytes());
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant 10xx
    let hex: Vec<String> = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}{}{}{}-{}{}-{}{}-{}{}-{}{}{}{}{}{}",
        hex[0],
        hex[1],
        hex[2],
        hex[3],
        hex[4],
        hex[5],
        hex[6],
        hex[7],
        hex[8],
        hex[9],
        hex[10],
        hex[11],
        hex[12],
        hex[13],
        hex[14],
        hex[15]
    )
}

/// 解析 check_device 的 rotate 指令，提取新令牌
///
/// 形态未知（未观测到非 null 值）：兼容字符串与 `{"token": "..."}` 对象，
/// 其余形态返回 None（记录日志由调用方处理）。
pub(crate) fn extract_rotate_token(rotate: &serde_json::Value) -> Option<String> {
    match rotate {
        serde_json::Value::String(s) if !s.is_empty() => Some(s.clone()),
        serde_json::Value::Object(map) => map
            .get("token")
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::accounts::hif::HifKind;

    fn test_identity() -> ClientIdentity {
        ClientIdentity {
            user_agent: "DeepSeek/2.5.0 Android/35".to_string(),
            client_version: "2.5.0".to_string(),
            client_platform: "android".to_string(),
            client_locale: "zh_CN".to_string(),
            client_bundle_id: "com.deepseek.chat".to_string(),
            device_id: String::new(),
            device_model: String::new(),
            timezone_offset: "28800".to_string(),
            client_os: "android".to_string(),
        }
    }

    fn test_client() -> DsClient {
        DsClient::new(
            "https://chat.deepseek.com/api/v0".to_string(),
            "https://example.com/x.wasm".to_string(),
            test_identity(),
            &HifConfig::default(),
            EmulationProfile::default(),
            None,
            &[],
        )
    }

    #[test]
    fn device_uuid_is_deterministic_and_well_formed() {
        let a = derive_device_uuid(b"https://chat.deepseek.com/api/v0");
        let b = derive_device_uuid(b"https://chat.deepseek.com/api/v0");
        assert_eq!(a, b, "同一 seed 必须派生同一 UUID");
        assert_eq!(a.len(), 36);
        assert_eq!(a.matches('-').count(), 4);
        assert_eq!(&a[14..15], "4", "version 4");
        assert!(
            matches!(a.as_bytes()[19], b'8' | b'9' | b'a' | b'b'),
            "variant"
        );
        assert_ne!(
            derive_device_uuid(b"https://other.example/api"),
            a,
            "不同 seed 应派生不同 UUID"
        );
    }

    #[test]
    fn client_headers_match_real_client_shape() {
        // 未绑定设备身份时不再兜底派生：先按真实用法绑定一个，再验证头形态
        let c = test_client().scoped_to("11111111-2222-4333-8444-555555555555");
        let h = c.client_headers().expect("headers");
        for name in [
            "user-agent",
            "x-client-version",
            "x-client-platform",
            "x-client-locale",
            "x-client-bundle-id",
            "x-device-id",
            "x-device-model",
            "x-client-timezone-offset",
        ] {
            assert!(h.contains_key(name), "缺少请求头 {name}");
        }
        assert_eq!(h["x-client-version"], "2.5.0");
        assert_eq!(h["x-device-model"], "");
        assert_eq!(h["x-client-timezone-offset"], "28800");
        // X-Device-Id 必须是调用方绑定的 UUID 形态（账号派生 / 显式配置），
        // 未绑定时为空，绝不凭空派生共用身份
        assert_eq!(h["x-device-id"], "11111111-2222-4333-8444-555555555555");

        // 业务 API 必须是**页面内 XHR** 形态，而不是地址栏导航：
        // 真实浏览器抓包为 accept: */*、sec-ch-ua-platform 与 UA 一致、
        // 不带 sec-fetch-dest: document / priority（2026-10-07 echo server 比对）
        assert_eq!(h["accept"], "*/*");
        assert_eq!(h["sec-fetch-dest"], "empty");
        assert_eq!(h["sec-fetch-mode"], "cors");
        assert_eq!(h["sec-fetch-site"], "same-origin");
        assert_eq!(h["sec-ch-ua-platform"], "\"Windows\"");
        assert_eq!(h["referer"], "https://chat.deepseek.com/");
        assert!(
            !h.contains_key("priority"),
            "不应带地址栏导航才有的 priority 头"
        );
    }

    #[test]
    fn device_id_config_override_wins() {
        let mut identity = test_identity();
        identity.device_id = "11111111-2222-4333-8444-555555555555".to_string();
        let c = DsClient::new(
            "https://chat.deepseek.com/api/v0".to_string(),
            "https://example.com/x.wasm".to_string(),
            identity,
            &HifConfig::default(),
            EmulationProfile::default(),
            None,
            &[],
        );
        assert_eq!(
            c.client_headers().unwrap()["x-device-id"],
            "11111111-2222-4333-8444-555555555555"
        );
    }

    #[test]
    fn scoped_client_carries_its_own_device_identity() {
        let base = test_client();
        let a = base.scoped_to("11111111-1111-4111-8111-111111111111");
        let b = base.scoped_to("22222222-2222-4222-8222-222222222222");
        assert_eq!(
            a.client_headers().unwrap()["x-device-id"],
            "11111111-1111-4111-8111-111111111111"
        );
        assert_eq!(
            b.client_headers().unwrap()["x-device-id"],
            "22222222-2222-4222-8222-222222222222"
        );
        // 空值不改变原设备身份
        let base_id = base.client_headers().unwrap()["x-device-id"].clone();
        assert_eq!(
            base.scoped_to("").client_headers().unwrap()["x-device-id"],
            base_id
        );
        // 设备视图与原客户端共享同一份 HIF 注册表（同一设备 = 同一份令牌）
        let base_hif = base.hif.as_ref().expect("hif enabled");
        assert!(std::sync::Arc::ptr_eq(
            &base_hif.tokens_for("device-x"),
            &a.hif.as_ref().unwrap().tokens_for("device-x")
        ));
        assert!(!std::sync::Arc::ptr_eq(
            &base_hif.tokens_for("device-x"),
            &base_hif.tokens_for("device-y")
        ));
    }

    #[test]
    fn emulation_profile_parsing() {
        assert_eq!(
            EmulationProfile::from_config("okhttp4_12"),
            Some(EmulationProfile::OkHttp4_12)
        );
        assert_eq!(
            EmulationProfile::from_config(" Chrome136 "),
            Some(EmulationProfile::Chrome136)
        );
        assert_eq!(EmulationProfile::from_config("nope"), None);
        assert_eq!(
            EmulationProfile::default(),
            EmulationProfile::OkHttp4_12,
            "默认档位对齐安卓 App（okhttp4_12），web 身份会被 AWS WAF 202 challenge 拦下"
        );
    }

    /// 回归：上游 `/chat/completion` 的 `source` 只接受 `default` / `landing`。
    /// 发 `input` 会得到 HTTP 422（`unknown variant `input`），账号随即被标记 Error。
    #[test]
    fn completion_source_is_accepted_upstream() {
        assert_eq!(COMPLETION_SOURCE_DEFAULT, "default");
        let payload = CompletionPayload {
            chat_session_id: "s".to_string(),
            parent_message_id: None,
            model_type: "default".to_string(),
            prompt: "hi".to_string(),
            ref_file_ids: Vec::new(),
            thinking_enabled: false,
            search_enabled: false,
            action: None,
            source: COMPLETION_SOURCE_DEFAULT,
            preempt: false,
        };
        let json = serde_json::to_string(&payload).expect("序列化失败");
        assert!(
            json.contains(r#""source":"default""#),
            "completion payload 必须发 source=default：{json}"
        );
    }

    /// 回归：PoW challenge 请求也必须带 `x-hif-leim` / `x-hif-dliq`。
    ///
    /// 真实客户端在**所有**鉴权请求上附加风险令牌（不只是 SSE），而早些版本的
    /// `create_pow_challenge` 用的是裸 `auth_headers`，于是「首发 completion 之前
    /// 先算 PoW」这条路径会静默挂住（无令牌请求被上游/代理当异常流量丢弃）。
    /// 这里用桩 HIF 端点验证头确实被带进请求构造流程。
    #[tokio::test]
    async fn pow_challenge_carries_hif_headers() {
        let mut identity = test_identity();
        identity.device_id = "11111111-2222-4333-8444-555555555555".to_string();
        let client = DsClient::new(
            "http://127.0.0.1:1/api/v0".to_string(),
            "https://example.com/x.wasm".to_string(),
            identity,
            &HifConfig::default(),
            EmulationProfile::default(),
            None,
            &[],
        );
        let tokens = client
            .hif
            .as_ref()
            .expect("HIF 注册表应存在")
            .tokens_for(client.device_id());
        tokens
            .get(HifKind::Leim)
            .seed_for_test("leim-v", std::time::Duration::from_mins(10))
            .await;
        tokens
            .get(HifKind::Dliq)
            .seed_for_test("dliq-v", std::time::Duration::from_mins(10))
            .await;

        let headers = client
            .pow_challenge_headers(Some("MOCK-TOKEN"))
            .await
            .expect("构造 challenge 请求头失败");
        assert_eq!(headers["x-hif-leim"], "leim-v");
        assert_eq!(headers["x-hif-dliq"], "dliq-v");
        assert_eq!(headers["authorization"], "Bearer MOCK-TOKEN");
        assert_eq!(
            headers["x-device-id"],
            "11111111-2222-4333-8444-555555555555"
        );
    }

    /// 回归：普通鉴权业务请求也必须带 `x-hif-leim` / `x-hif-dliq`。
    ///
    /// `create_session` / `stop_stream` / `fetch_files` 等非 SSE 调用曾经只用裸
    /// `auth_headers`。真实客户端的风险令牌覆盖每一个业务请求，漏掉的调用点会被
    /// 上游当成非官方客户端（创建会话失败 → 整条链路不可用）。这里同时覆盖
    /// `authed_headers` 与 `authed_headers_with_pow`（上传文件走后者）。
    #[tokio::test]
    async fn business_requests_carry_hif_headers() {
        let mut identity = test_identity();
        identity.device_id = "22222222-3333-4444-8555-666666666666".to_string();
        let client = DsClient::new(
            "http://127.0.0.1:1/api/v0".to_string(),
            "https://example.com/x.wasm".to_string(),
            identity,
            &HifConfig::default(),
            EmulationProfile::default(),
            None,
            &[],
        );
        let tokens = client
            .hif
            .as_ref()
            .expect("HIF 注册表应存在")
            .tokens_for(client.device_id());
        tokens
            .get(HifKind::Leim)
            .seed_for_test("leim-v", std::time::Duration::from_mins(10))
            .await;
        tokens
            .get(HifKind::Dliq)
            .seed_for_test("dliq-v", std::time::Duration::from_mins(10))
            .await;

        let headers = client
            .authed_headers("MOCK-TOKEN")
            .await
            .expect("构造鉴权请求头失败");
        assert_eq!(headers["x-hif-leim"], "leim-v");
        assert_eq!(headers["x-hif-dliq"], "dliq-v");
        assert_eq!(headers["authorization"], "Bearer MOCK-TOKEN");

        let pow_headers = client
            .authed_headers_with_pow("MOCK-TOKEN", "MOCK-POW")
            .await
            .expect("构造鉴权 + PoW 请求头失败");
        assert_eq!(pow_headers["x-hif-leim"], "leim-v");
        assert_eq!(pow_headers["x-ds-pow-response"], "MOCK-POW");
    }

    #[test]
    fn hif_can_be_disabled() {
        let c = DsClient::new(
            "https://chat.deepseek.com/api/v0".to_string(),
            "https://example.com/x.wasm".to_string(),
            test_identity(),
            &HifConfig {
                enabled: false,
                ..HifConfig::default()
            },
            EmulationProfile::default(),
            None,
            &[],
        );
        assert!(c.hif.is_none(), "关闭后不应构造 HIF 令牌提供者");
    }

    #[test]
    fn chat_status_accepts_int_and_bool_mute_flags() {
        let int_form: ChatStatus =
            serde_json::from_str(r#"{"is_muted":1,"mute_until":1790397239.972}"#).unwrap();
        assert_eq!(int_form.is_muted, 1);
        assert_eq!(int_form.mute_until, Some(1_790_397_239.972));

        let bool_form: ChatStatus = serde_json::from_str(r#"{"is_muted":false}"#).unwrap();
        assert_eq!(bool_form.is_muted, 0);
        assert_eq!(bool_form.mute_until, None);

        // 缺失 chat 字段 = 未禁言
        let user: UserInfo = serde_json::from_str(r#"{"id":"u","token":"t"}"#).unwrap();
        assert!(user.chat.is_none());
    }

    #[test]
    fn check_device_rotate_parsing() {
        let null_rotate: CheckDeviceData = serde_json::from_str(r#"{"rotate":null}"#).unwrap();
        assert!(null_rotate.rotate.is_none());

        let str_rotate: CheckDeviceData =
            serde_json::from_str(r#"{"rotate":"new-token"}"#).unwrap();
        assert_eq!(
            extract_rotate_token(str_rotate.rotate.as_ref().unwrap()).as_deref(),
            Some("new-token")
        );

        let obj_rotate: CheckDeviceData =
            serde_json::from_str(r#"{"rotate":{"token":"obj-token"}}"#).unwrap();
        assert_eq!(
            extract_rotate_token(obj_rotate.rotate.as_ref().unwrap()).as_deref(),
            Some("obj-token")
        );

        let junk: serde_json::Value = serde_json::from_str(r#"{"foo":1}"#).unwrap();
        assert!(extract_rotate_token(&junk).is_none());
    }

    #[test]
    fn fetch_sessions_response_parsing() {
        let data: FetchSessionsData = serde_json::from_str(
            r#"{"chat_sessions":[
                {"id":"97952d9b","title":"命令行命令翻译","title_type":"SYSTEM",
                 "pinned":false,"model_type":"default","updated_at":1778039195.997},
                {"id":"5be62e10","title":null,"pinned":true}
            ],"has_more":true}"#,
        )
        .unwrap();
        assert_eq!(data.chat_sessions.len(), 2);
        assert_eq!(data.has_more, Some(true));
        assert_eq!(
            data.chat_sessions[0].title.as_deref(),
            Some("命令行命令翻译")
        );
        assert_eq!(data.chat_sessions[0].updated_at, Some(1_778_039_195.997));
        assert!(data.chat_sessions[1].title.is_none());
        assert!(data.chat_sessions[1].model_type.is_none());
        assert!(data.chat_sessions[1].updated_at.is_none());

        // 空响应（无会话账号）
        let empty: FetchSessionsData = serde_json::from_str(r#"{"chat_sessions":[]}"#).unwrap();
        assert!(empty.chat_sessions.is_empty());
        assert_eq!(empty.has_more, None);
    }

    fn envelope(biz_data: &serde_json::Value) -> Envelope<Option<()>> {
        serde_json::from_value(serde_json::json!({
            "code": 0,
            "msg": "",
            "data": { "biz_code": 0, "biz_msg": "", "biz_data": biz_data }
        }))
        .unwrap()
    }

    /// `stop_stream` / `delete_session` 的上游返回是 `biz_data: {}`，
    /// 旧实现只放行 `null`，导致正常结束被记成 `invalid type: map, expected unit`。
    #[test]
    fn empty_object_biz_data_counts_as_ok() {
        for payload in [serde_json::json!(null), serde_json::json!({})] {
            let result = envelope(&payload).into_result();
            assert!(result.is_ok(), "biz_data={payload} 应视为成功: {result:?}");
        }
    }

    #[test]
    fn non_empty_biz_data_for_unit_still_fails() {
        assert!(matches!(
            envelope(&serde_json::json!(1)).into_result(),
            Err(ClientError::Json(_))
        ));
    }
}
