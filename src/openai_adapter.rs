//! OpenAI 协议适配层 —— OpenAI JSON 与 ds_core 内部格式的双向转换
//!
//! 本模块负责将 OpenAI 兼容的 HTTP 请求转换为 ds_core 内部格式，
//! 并将 ds_core 的响应转换为 OpenAI 兼容的 JSON 格式。
//!
//! 对外暴露最小接口：OpenAIAdapter, OpenAIAdapterError

use std::pin::Pin;
use std::sync::Arc;

use std::collections::HashMap;

use bytes::Bytes;
use futures::{Stream, StreamExt};

use ds_core::{AccountConfig, CoreError, DsCore, DsCoreConfig};

use self::types::{FunctionCallOption, NamedFunction, NamedToolChoice, Tool, ToolChoice};

mod models;
pub(crate) mod request;
pub(crate) mod response;
pub(crate) mod types;

pub use types::{ChatCompletionsRequest, ChatCompletionsResponse, ChatCompletionsResponseChunk};

/// 流式响应类型（SSE 字节流）
/// 根 crate 的 `[ds_core.auto_resume]` 段 → `ds_core::AutoResumeConfig`
impl From<&crate::config::AutoResumeSection> for ds_core::AutoResumeConfig {
    fn from(section: &crate::config::AutoResumeSection) -> Self {
        Self {
            thinking_heartbeat_secs: section.thinking_heartbeat_secs,
            on_truncation: section.on_truncation,
            max_attempts: section.max_attempts,
            empty_prompt: section.empty_prompt,
        }
    }
}

pub type StreamResponse = Pin<Box<dyn Stream<Item = Result<Bytes, OpenAIAdapterError>> + Send>>;

/// 流式响应结构体流
pub type ChunkStream =
    Pin<Box<dyn Stream<Item = Result<ChatCompletionsResponseChunk, OpenAIAdapterError>> + Send>>;

/// Chat Completions 统一输出
pub enum ChatOutput {
    Stream(ChunkStream),
    Json(ChatCompletionsResponse),
}

/// adapter 层通用结果包装：携带请求结果和账号标识
pub struct ChatResult<T> {
    pub data: T,
    pub account_id: String,
    pub prompt_tokens: u32,
}

/// OpenAI 适配器
pub struct OpenAIAdapter {
    ds_core: Arc<DsCore>,
    model_types: tokio::sync::RwLock<Vec<String>>,
    model_registry: tokio::sync::RwLock<HashMap<String, String>>,
    model_aliases: tokio::sync::RwLock<Vec<String>>,
    /// 元数据取值策略 + `model_types` 对齐的配置数组
    metadata_source: tokio::sync::RwLock<crate::config::DsCoreSection>,
    tag_config: tokio::sync::RwLock<Arc<response::TagConfig>>,
    /// 未显式传 `web_search_options` 时是否默认开启搜索模式
    default_search_enabled: std::sync::atomic::AtomicBool,
    /// 缓存的 tiktoken BPE 编码器（避免每次请求重建）
    bpe: Option<Arc<tiktoken_rs::CoreBPE>>,
    /// 最近一次账号同步结果（供面板在保存后继续展示被拒账号）
    last_sync_report: std::sync::Mutex<Option<AccountSyncReport>>,
}

impl OpenAIAdapter {
    /// 创建适配器实例
    pub async fn new(config: &crate::config::Config) -> Result<Self, OpenAIAdapterError> {
        let core_cfg = DsCoreConfig {
            api_base: config.ds_core.api_base.clone(),
            wasm_url: config.ds_core.wasm_url.clone(),
            user_agent: config.ds_core.user_agent.clone(),
            client_version: config.ds_core.client_version.clone(),
            client_platform: config.ds_core.client_platform.clone(),
            client_locale: config.ds_core.client_locale.clone(),
            client_bundle_id: config.ds_core.client_bundle_id.clone(),
            client_device_id: config.ds_core.client_device_id.clone(),
            client_device_model: config.ds_core.client_device_model.clone(),
            client_timezone_offset: config.ds_core.client_timezone_offset.clone(),
            client_os: config.ds_core.client_os.clone(),
            emulation: config.ds_core.emulation.clone(),
            hif_enabled: config.ds_core.hif_enabled,
            startup_health_check: config.ds_core.startup_health_check,
            session_policy: config.ds_core.session_policy.clone(),
            session_idle_secs: config.ds_core.session_idle_secs,
            proxy_url: config.proxy.url.clone(),
            proxy_bypass_hosts: config.proxy.bypass_hosts.clone(),
            model_types: config.ds_core.model_types.clone(),
            input_character_limits: config.ds_core.input_character_limits.clone(),
            hourly_request_quota: config.ds_core.hourly_request_quota,
            request_min_interval_secs: config.ds_core.request_min_interval_secs,
            auto_resume: (&config.ds_core.auto_resume).into(),
            login_encryption: config.ds_core.login_encryption,
        };
        let accounts: Vec<AccountConfig> = config
            .ds_core
            .accounts
            .iter()
            .map(|a| AccountConfig {
                email: a.email.clone(),
                mobile: a.mobile.clone(),
                area_code: a.area_code.clone(),
                password: a.password.clone(),
                device_id: a.device_id.clone(),
            })
            .collect();
        let ds_core = Arc::new(DsCore::new(&core_cfg, accounts).await?);
        let model_registry = config.ds_core.model_registry();
        // 预初始化 tiktoken BPE（避免每次请求重建词表）
        let bpe = tiktoken_rs::cl100k_base().ok().map(Arc::new);

        Ok(Self {
            ds_core,
            model_types: tokio::sync::RwLock::new(config.ds_core.model_types.clone()),
            model_registry: tokio::sync::RwLock::new(model_registry),
            model_aliases: tokio::sync::RwLock::new(config.ds_core.model_aliases.clone()),
            metadata_source: tokio::sync::RwLock::new(config.ds_core.clone()),
            tag_config: tokio::sync::RwLock::new(Arc::new(response::TagConfig::from_config(
                &config.ds_core.tool_call,
            ))),
            default_search_enabled: std::sync::atomic::AtomicBool::new(
                config.ds_core.default_search_enabled,
            ),
            bpe,
            last_sync_report: std::sync::Mutex::new(None),
        })
    }

    /// POST /v1/chat/completions（统一入口）
    ///
    /// 内部校验参数、构建 ChatML prompt、按 stream 标记分流：
    /// - stream=true  → 返回 SSE 字节流
    /// - stream=false → 将 SSE 流聚合为单个 JSON 对象后返回
    pub async fn chat_completions(
        &self,
        mut req: ChatCompletionsRequest,
        request_id: &str,
    ) -> Result<ChatResult<ChatOutput>, OpenAIAdapterError> {
        log::debug!(target: "adapter", "req={} adapter start: model={}, stream={}", request_id, req.model, req.stream);

        // 兼容旧版 functions / function_call → tools / tool_choice
        if req.tools.as_ref().is_none_or(|t| t.is_empty())
            && let Some(functions) = req.functions.clone()
            && !functions.is_empty()
        {
            req.tools = Some(
                functions
                    .into_iter()
                    .map(|f| Tool {
                        ty: "function".to_string(),
                        function: Some(f),
                        custom: None,
                    })
                    .collect(),
            );
        }
        if req.tool_choice.is_none()
            && let Some(fc) = req.function_call.clone()
        {
            req.tool_choice = Some(match fc {
                FunctionCallOption::Mode(mode) => ToolChoice::Mode(mode),
                FunctionCallOption::Named(named) => ToolChoice::Named(NamedToolChoice {
                    ty: "function".to_string(),
                    function: NamedFunction { name: named.name },
                }),
            });
        }

        let norm = request::normalize::apply(&req).map_err(OpenAIAdapterError::BadRequest)?;
        let tool_ctx = request::tools::extract(&req).map_err(OpenAIAdapterError::BadRequest)?;
        let prompt = request::prompt::build(&req, &tool_ctx);
        // 读锁只覆盖模型解析，避免后续网络等待期间阻塞配置热重载
        let model_res = {
            let registry = self.model_registry.read().await;
            request::resolver::resolve(
                &registry,
                &req.model,
                req.reasoning_effort.as_deref(),
                req.web_search_options.as_ref(),
                self.default_search(),
            )
            .map_err(OpenAIAdapterError::BadRequest)?
        };

        let prompt_tokens = self.bpe.as_ref().map_or(0, |bpe| {
            u32::try_from(bpe.encode_with_special_tokens(&prompt).len())
                .expect("token count exceeds u32::MAX")
        });

        let file_result = request::files::extract(&req);
        // 纯对话形态下提供增量信息：会话复用时只发新增的那条用户消息（对齐官方客户端）
        let delta = request::delta::build(
            &req,
            &tool_ctx,
            !file_result.files.is_empty(),
            file_result.has_http_urls,
        );
        let chat_req = ds_core::ChatRequest {
            prompt,
            thinking_enabled: model_res.thinking_enabled,
            search_enabled: model_res.search_enabled || file_result.has_http_urls,
            model_type: model_res.model_type,
            files: file_result.files,
            delta,
            ..Default::default()
        };

        let chat_resp = self.try_chat(chat_req, request_id).await?;
        let (account_id, event_stream) = Self::take_meta(chat_resp.stream).await.map_err(|e| {
            log::error!(target: "adapter", "req={request_id} failed to read Meta event: {e}");
            OpenAIAdapterError::Internal("取 Meta 事件失败".into())
        })?;

        // 为修复模型准备工具定义信息
        let tool_defs = req.tools.as_ref().map(|tools| {
            tools
                .iter()
                .filter_map(|t| t.function.as_ref())
                .map(|f| {
                    format!(
                        "- {}: {}",
                        f.name,
                        serde_json::to_string(&f.parameters).unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        });

        if req.stream {
            let repair_fn = self.create_repair_fn(request_id, tool_defs.clone()).await;
            let s = response::stream(
                event_stream,
                req.model,
                response::StreamCfg {
                    include_usage: norm.include_usage,
                    include_obfuscation: norm.include_obfuscation,
                    stop: norm.stop,
                    prompt_tokens,
                    repair_fn: Some(repair_fn),
                    tag_config: self.tag_config.read().await.clone(),
                },
            );
            Ok(ChatResult {
                data: ChatOutput::Stream(s),
                account_id,
                prompt_tokens,
            })
        } else {
            let repair_fn = self.create_repair_fn(request_id, tool_defs).await;
            let json = response::aggregate(
                event_stream,
                req.model,
                response::StreamCfg {
                    include_usage: true,
                    include_obfuscation: false,
                    stop: norm.stop,
                    prompt_tokens,
                    repair_fn: Some(repair_fn),
                    tag_config: self.tag_config.read().await.clone(),
                },
            )
            .await?;
            Ok(ChatResult {
                data: ChatOutput::Json(json),
                account_id,
                prompt_tokens,
            })
        }
    }

    /// 内部辅助：对 `Overloaded` 进行退避重试（v0_chat 内部已做换号重试，此处为号池级兜底）
    pub(crate) async fn try_chat(
        &self,
        req: ds_core::ChatRequest,
        request_id: &str,
    ) -> Result<ds_core::ChatResponse, CoreError> {
        const MAX_RETRIES: usize = 2;
        const BASE_DELAY_MS: u64 = 2000;

        for attempt in 0..MAX_RETRIES {
            match self.ds_core.v0_chat(req.clone(), request_id).await {
                Ok(resp) => {
                    if attempt > 0 {
                        log::info!(target: "adapter", "req={request_id} retry #{attempt} succeeded");
                    }
                    return Ok(resp);
                }
                Err(CoreError::Overloaded) if attempt + 1 < MAX_RETRIES => {
                    let delay = BASE_DELAY_MS * (1 << attempt);
                    log::warn!(target: "adapter", "req={} overloaded, retry #{} in {}ms", request_id, attempt + 1, delay);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
                Err(e) => return Err(e),
            }
        }
        log::warn!(target: "adapter", "req={request_id} all {MAX_RETRIES} retries failed, giving up");
        Err(CoreError::Overloaded)
    }

    /// 读取「默认是否开启搜索模式」开关
    fn default_search(&self) -> bool {
        self.default_search_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 对外暴露的模型元数据（token 上限等）
    ///
    /// 只有 `input_character_limit` 是上游真实字段；token 上限是实测常量。
    /// 由 `[ds_core] model_metadata_source` 决定取值策略（见 `resolved_*`）。
    async fn model_metadata(&self) -> (Vec<u32>, Vec<u32>) {
        // 先把需要的字段拷出来，读锁不跨后续计算持有
        let (source, limit, input, output) = {
            let config = self.metadata_source.read().await;
            (
                config.model_metadata_source.clone(),
                config.model_types.len(),
                config.max_input_tokens.clone(),
                config.max_output_tokens.clone(),
            )
        };
        match source.as_str() {
            // 纯配置驱动：完全按配置里的数组，缺项就回落到实测常量
            "config" => {
                let mut input = input;
                let mut output = output;
                input.resize(limit, crate::config::MEASURED_MAX_INPUT_TOKENS);
                output.resize(limit, crate::config::MEASURED_MAX_OUTPUT_TOKENS);
                (input, output)
            }
            // auto（及任何未知值）：实测常量为准，配置值不参与
            _ => (
                vec![crate::config::MEASURED_MAX_INPUT_TOKENS; limit],
                vec![crate::config::MEASURED_MAX_OUTPUT_TOKENS; limit],
            ),
        }
    }

    /// GET /v1/models
    pub async fn list_models(&self) -> types::OpenAIModelList {
        let model_types = self.model_types.read().await;
        let aliases = self.model_aliases.read().await;
        let (max_input, max_output) = self.model_metadata().await;
        models::list(&model_types, &max_input, &max_output, &aliases)
    }

    /// GET /v1/models/{model_id}
    pub async fn get_model(&self, model_id: &str) -> Option<types::OpenAIModel> {
        let model_types = self.model_types.read().await;
        let aliases = self.model_aliases.read().await;
        let (max_input, max_output) = self.model_metadata().await;
        models::get(&model_types, &max_input, &max_output, &aliases, model_id)
    }

    /// 原始 DeepSeek SSE 流（不经 OpenAI 协议转换）
    ///
    /// 用于流分析：对比原始响应与 OpenAI 转换后的差异，定位转换 bug
    pub async fn raw_chat_completions_stream(
        &self,
        body: &[u8],
        request_id: &str,
    ) -> Result<ChatResult<StreamResponse>, OpenAIAdapterError> {
        let chat_req: ChatCompletionsRequest = serde_json::from_slice(body)
            .map_err(|e| OpenAIAdapterError::BadRequest(format!("bad request: {e}")))?;
        let model_res = {
            let registry = self.model_registry.read().await;
            request::resolver::resolve(
                &registry,
                &chat_req.model,
                chat_req.reasoning_effort.as_deref(),
                chat_req.web_search_options.as_ref(),
                self.default_search(),
            )
            .map_err(OpenAIAdapterError::BadRequest)?
        };
        let ds_req = ds_core::ChatRequest {
            prompt: request::prompt::build(
                &chat_req,
                &request::tools::extract(&chat_req).map_err(OpenAIAdapterError::BadRequest)?,
            ),
            thinking_enabled: model_res.thinking_enabled,
            search_enabled: model_res.search_enabled,
            model_type: model_res.model_type,
            files: vec![],
            delta: None,
            ..Default::default()
        };
        let chat_resp = self.try_chat(ds_req, request_id).await?;
        let (account_id, event_stream) = Self::take_meta(chat_resp.stream).await?;

        // 将 StreamEvent 序列化为 JSON 行供调试
        let data: StreamResponse = Box::pin(event_stream.map(|r| {
            r.map(|evt| {
                let line = format!("{evt:?}\n");
                Bytes::from(line.into_bytes())
            })
            .map_err(OpenAIAdapterError::from)
        }));
        Ok(ChatResult {
            data,
            account_id,
            prompt_tokens: 0,
        })
    }

    /// 获取 ds_core 账号池状态
    /// 暴露底层 `DsCore`（OTA 等需要复用拟态客户端 + 代理的辅助请求）
    #[must_use]
    pub const fn ds_core(&self) -> &Arc<DsCore> {
        &self.ds_core
    }

    pub fn account_statuses(&self) -> Vec<ds_core::AccountStatus> {
        self.ds_core.account_statuses()
    }

    /// 获取 ds_core 账号池详细状态（含滑动窗口统计、请求间隔等）
    pub fn account_statuses_detailed(&self) -> Vec<ds_core::AccountStatus> {
        self.ds_core.account_statuses_detailed()
    }

    /// 拉取账号的会话列表（`updated_at` 为分页游标，None = 第一页）
    pub async fn fetch_sessions(
        &self,
        updated_at: Option<f64>,
    ) -> Result<ds_core::FetchSessionsData, OpenAIAdapterError> {
        self.ds_core
            .fetch_sessions(updated_at)
            .await
            .map_err(OpenAIAdapterError::from)
    }

    /// 动态添加账号
    pub async fn add_account(
        &self,
        creds: &crate::config::Account,
    ) -> Result<String, ds_core::PoolError> {
        let ac = AccountConfig {
            email: creds.email.clone(),
            mobile: creds.mobile.clone(),
            area_code: creds.area_code.clone(),
            password: creds.password.clone(),
            device_id: creds.device_id.clone(),
        };
        self.ds_core.add_account(&ac).await
    }

    /// 动态移除账号
    pub fn remove_account(&self, email_or_mobile: &str) -> Result<String, ds_core::PoolError> {
        self.ds_core.remove_account(email_or_mobile)
    }

    /// 标记账号为 Error 状态
    pub fn mark_error(&self, email_or_mobile: &str) {
        self.ds_core.mark_error(email_or_mobile);
    }

    /// 手动重新登录指定账号
    pub async fn re_login_single(&self, email_or_mobile: &str) -> Result<(), String> {
        self.ds_core.re_login_single(email_or_mobile).await
    }
}

/// 单个账号同步失败的原因
///
/// 管理面板新增账号时，池子拒绝一个账号绝不会是「无原因」：登录被拒、
/// 被禁言、缺设备指纹、配额/网络异常都会落到某一种 `PoolError`。这里把
/// 错误翻成用户看得懂的中文，避免面板出现「加了却没反应」的黑箱。
#[derive(Debug, Clone, serde::Serialize)]
pub struct AccountSyncFailure {
    /// 面板里填写的账号标识（邮箱或手机号）
    pub id: String,
    /// 拒绝原因（中文，可直接展示）
    pub reason: String,
}

/// 一次账号同步的结果汇总
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct AccountSyncReport {
    pub added: usize,
    pub removed: usize,
    pub failed: usize,
    pub failures: Vec<AccountSyncFailure>,
    /// 同步是否因为超时被提前中断（账号由后台恢复任务继续处理）
    #[serde(default)]
    pub timed_out: bool,
}

/// 把账号池错误翻成可读的中文原因
fn account_sync_failure_reason(e: &ds_core::PoolError) -> String {
    // PoolError::Client 内部携带的 ClientError 不是公开类型，这里用 Display 文本
    // 判断具体的失败类别：Display 里已经带 biz_code / HTTP 状态，足够分类。
    match e {
        ds_core::PoolError::Validation(msg) => msg.clone(),
        ds_core::PoolError::Pow(err) => format!("PoW 求解失败：{err}"),
        ds_core::PoolError::AlreadyExists(id) => format!("账号已存在于池中：{id}"),
        ds_core::PoolError::NotFound(id) => format!("账号不存在：{id}"),
        ds_core::PoolError::AccountBusy(id) => format!("账号正忙，无法移除：{id}"),
        ds_core::PoolError::AllAccountsFailed => "所有账号初始化失败".to_string(),
        ds_core::PoolError::Client(inner) => {
            let text = inner.to_string();
            let code = text
                .split("code=")
                .nth(1)
                .and_then(|rest| rest.split([' ', ',']).next())
                .and_then(|n| n.parse::<i64>().ok());
            match code {
                Some(2) => "账号或密码错误（biz_code=2）".to_string(),
                Some(5) => format!("账号被禁言/限流，等待 mute_until 过后再试（biz_code=5，{text}）"),
                Some(10) => "账号已被封禁（biz_code=10）".to_string(),
                Some(11) => "设备指纹被上游拒绝 RISK_DEVICE_DETECTED（biz_code=11）：请为该账号填写真实浏览器的 device_id".to_string(),
                Some(c) => format!("上游业务错误 biz_code={c}：{text}"),
                None => format!("登录失败：{text}"),
            }
        }
    }
}

impl OpenAIAdapter {
    /// 批量同步账号：对比当前账号池与目标配置，增删差异账号
    ///
    /// 返回 `AccountSyncReport` 而不是把错误吞进日志：管理面板保存后要把
    /// 「配了但没进池」的账号连同拒绝原因回报给用户。早期实现只 `log::warn!`，
    /// 账号静默缺席（登录失败、被禁言、缺 device_id），面板上什么都看不到。
    pub(crate) async fn sync_accounts(
        &self,
        new_accounts: &[crate::config::Account],
    ) -> AccountSyncReport {
        let old_statuses = self.account_statuses();
        let old_ids: Vec<String> = old_statuses
            .iter()
            .map(|a| {
                if a.email.is_empty() {
                    a.mobile.clone()
                } else {
                    a.email.clone()
                }
            })
            .collect();

        let mut report = AccountSyncReport::default();
        for acct in new_accounts {
            let id = if acct.email.is_empty() {
                &acct.mobile
            } else {
                &acct.email
            };
            if old_ids.contains(id) {
                continue;
            }
            match self.add_account(acct).await {
                Ok(_) => report.added += 1,
                Err(e) => {
                    let reason = account_sync_failure_reason(&e);
                    log::warn!(
                        target: "adapter",
                        "failed to add account during sync {id}: {e}"
                    );
                    report.failed += 1;
                    report.failures.push(AccountSyncFailure {
                        id: id.clone(),
                        reason,
                    });
                }
            }
        }

        let new_ids: Vec<&str> = new_accounts
            .iter()
            .map(|a| {
                if a.email.is_empty() {
                    a.mobile.as_str()
                } else {
                    a.email.as_str()
                }
            })
            .collect();
        for old_id in &old_ids {
            if !new_ids.contains(&old_id.as_str()) && !old_id.is_empty() {
                match self.remove_account(old_id) {
                    Ok(_) => report.removed += 1,
                    Err(e) => {
                        log::warn!(
                            target: "adapter",
                            "failed to remove account during sync {old_id}: {e}"
                        );
                    }
                }
            }
        }

        if report.failed > 0 {
            log::warn!(
                target: "adapter",
                "账号同步完成：新增 {} 个、移除 {} 个、失败 {} 个",
                report.added,
                report.removed,
                report.failed
            );
        }
        report
    }

    /// 从流中提取首个 Meta 事件的 account_id，同时保留 Meta 在流中
    async fn take_meta(
        stream: Pin<Box<dyn Stream<Item = Result<ds_core::StreamEvent, CoreError>> + Send>>,
    ) -> Result<
        (
            String,
            Pin<Box<dyn Stream<Item = Result<ds_core::StreamEvent, CoreError>> + Send>>,
        ),
        CoreError,
    > {
        use futures::StreamExt;
        let mut stream = stream;
        match stream.next().await {
            Some(Ok(ds_core::StreamEvent::Meta { account_id })) => {
                let full =
                    futures::stream::once(futures::future::ready(Ok(ds_core::StreamEvent::Meta {
                        account_id: account_id.clone(),
                    })))
                    .chain(stream);
                Ok((account_id, Box::pin(full)))
            }
            Some(Ok(other)) => {
                log::warn!(target: "adapter", "expected Meta as first event, got: {other:?}");
                let rest = futures::stream::once(futures::future::ready(Ok(other))).chain(stream);
                Ok((String::new(), Box::pin(rest)))
            }
            Some(Err(e)) => Err(e),
            None => Err(CoreError::Stream("empty stream".into())),
        }
    }

    /// 优雅关闭
    pub async fn shutdown(&self) {
        self.ds_core.shutdown().await;
    }

    /// 同步账号池（公开版本，供管理面板在保存时同步调用并回报失败原因）
    ///
    /// 与 `reload_config` 拆开是因为这两步的耗时与失败语义不同：
    /// 账号同步要真的登录上游（每个新账号一次请求），结果必须回给用户；
    /// 客户端/WASM 重建很慢且失败只需重启，改在后台跑。
    pub async fn sync_accounts_now(
        &self,
        accounts: &[crate::config::Account],
    ) -> AccountSyncReport {
        let report = self.sync_accounts(accounts).await;
        if let Ok(mut slot) = self.last_sync_report.lock() {
            *slot = Some(report.clone());
        }
        report
    }

    /// 读取最近一次账号同步结果（面板保存后可继续查看被拒账号）
    pub fn last_sync_report(&self) -> Option<AccountSyncReport> {
        self.last_sync_report
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// 热重载配置：重建模型注册表与底层客户端（**不含**账号同步）
    ///
    /// 账号同步由 `sync_accounts_now` 在保存请求里同步完成并回报原因，这里
    /// 只做不需要用户等待的部分；失败返回 `Err`，调用方提示「重启后生效」。
    pub async fn reload_config(&self, new_config: &crate::config::Config) -> Result<(), CoreError> {
        // Rebuild model registry
        let registry = new_config.ds_core.model_registry();
        *self.model_registry.write().await = registry;
        *self.model_types.write().await = new_config.ds_core.model_types.clone();
        *self.model_aliases.write().await = new_config.ds_core.model_aliases.clone();
        *self.metadata_source.write().await = new_config.ds_core.clone();
        *self.tag_config.write().await = Arc::new(response::TagConfig::from_config(
            &new_config.ds_core.tool_call,
        ));
        self.default_search_enabled.store(
            new_config.ds_core.default_search_enabled,
            std::sync::atomic::Ordering::Relaxed,
        );
        // Rebuild DsClient if needed (deepseek/proxy changes)
        let core_cfg = DsCoreConfig {
            api_base: new_config.ds_core.api_base.clone(),
            wasm_url: new_config.ds_core.wasm_url.clone(),
            user_agent: new_config.ds_core.user_agent.clone(),
            client_version: new_config.ds_core.client_version.clone(),
            client_platform: new_config.ds_core.client_platform.clone(),
            client_locale: new_config.ds_core.client_locale.clone(),
            client_bundle_id: new_config.ds_core.client_bundle_id.clone(),
            client_device_id: new_config.ds_core.client_device_id.clone(),
            client_device_model: new_config.ds_core.client_device_model.clone(),
            client_timezone_offset: new_config.ds_core.client_timezone_offset.clone(),
            client_os: new_config.ds_core.client_os.clone(),
            emulation: new_config.ds_core.emulation.clone(),
            hif_enabled: new_config.ds_core.hif_enabled,
            startup_health_check: new_config.ds_core.startup_health_check,
            session_policy: new_config.ds_core.session_policy.clone(),
            session_idle_secs: new_config.ds_core.session_idle_secs,
            proxy_url: new_config.proxy.url.clone(),
            proxy_bypass_hosts: new_config.proxy.bypass_hosts.clone(),
            model_types: new_config.ds_core.model_types.clone(),
            input_character_limits: new_config.ds_core.input_character_limits.clone(),
            hourly_request_quota: new_config.ds_core.hourly_request_quota,
            request_min_interval_secs: new_config.ds_core.request_min_interval_secs,
            auto_resume: (&new_config.ds_core.auto_resume).into(),
            login_encryption: new_config.ds_core.login_encryption,
        };
        self.ds_core.reload_config(&core_cfg).await
    }

    pub(crate) async fn create_repair_fn(
        &self,
        request_id: &str,
        tool_defs: Option<String>,
    ) -> response::RepairFn {
        use std::sync::atomic::{AtomicU16, Ordering};
        let core = self.ds_core.clone();
        let req_id = request_id.to_string();
        let seq = Arc::new(AtomicU16::new(0));
        let tag_config = self.tag_config.read().await.clone();
        let tools_info = tool_defs.unwrap_or_default();
        Arc::new(move |tool_text: String| {
            let core = core.clone();
            let req_id = req_id.clone();
            let seq = seq.clone();
            let tag_config = tag_config.clone();
            let tools_info = tools_info.clone();
            Box::pin(async move {
                use std::fmt::Write as _;

                use ds_core::ChatRequest;
                let n = seq.fetch_add(1, Ordering::Relaxed);
                let repair_req_id = format!("{req_id}-repair-{n}");
                let mut prompt = String::with_capacity(tools_info.len() + tool_text.len() + 256);
                if !tools_info.is_empty() {
                    let _ = write!(prompt, "可用的工具定义：\n{tools_info}\n\n");
                }
                let _ = write!(
                    prompt,
                    "请将以下代码块中的内容提取并转换为合法的工具调用 JSON 数组。\
                     \n每个元素必须包含 \"name\"（字符串）和 \"arguments\"（对象）字段。\
                     \n只输出 JSON 数组本身，不要加 code fence，不要其他文字解释。\
                     \n注意：字符串值中的引号和换行符必须用反斜杠转义（如 \\\" 和 \\n）。\
                     \n\n需要修复的内容：\n~~~\n{tool_text}\n~~~"
                );
                let req = ChatRequest {
                    prompt,
                    thinking_enabled: false,
                    search_enabled: false,
                    model_type: "default".to_string(),
                    files: vec![],
                    delta: None,
                    ..Default::default()
                };
                log::debug!(
                    target: "adapter",
                    "{} 发起修复请求: len={}", repair_req_id, tool_text.len()
                );
                let resp = core
                    .v0_chat(req, &repair_req_id)
                    .await
                    .map_err(OpenAIAdapterError::from)?;
                response::execute_tool_repair(resp.stream, &tag_config).await
            })
        })
    }
}

/// 适配器错误类型
#[derive(Debug, thiserror::Error)]
pub enum OpenAIAdapterError {
    /// 请求格式错误
    #[error("bad request: {0}")]
    BadRequest(String),

    /// 服务过载，无可用的 ds_core 账号
    #[error("service overloaded")]
    Overloaded,

    /// 上游提供商错误（网络、业务错误等）
    #[error("provider error: {0}")]
    ProviderError(String),

    /// 内部错误（序列化、流转换等）
    #[error("internal error: {0}")]
    Internal(String),

    /// tool_calls 标记解析失败，携带 `{TOOL_CALL_START}...{TOOL_CALL_END}` 内的原始文本
    #[error("tool_calls repair needed: {0}")]
    ToolCallRepairNeeded(String),
}

impl From<CoreError> for OpenAIAdapterError {
    fn from(e: CoreError) -> Self {
        match e {
            CoreError::Overloaded => Self::Overloaded,
            CoreError::ProofOfWorkFailed(err) => {
                Self::Internal(format!("proof of work failed: {err}"))
            }
            CoreError::ProviderError(msg) => Self::ProviderError(msg),
            CoreError::Stream(msg) => Self::Internal(msg),
        }
    }
}

impl From<serde_json::Error> for OpenAIAdapterError {
    fn from(e: serde_json::Error) -> Self {
        Self::Internal(format!("json serialization failed: {e}"))
    }
}

impl OpenAIAdapterError {
    /// 返回对应 HTTP 状态码
    #[must_use]
    pub const fn status_code(&self) -> u16 {
        match self {
            Self::BadRequest(_) => 400,
            Self::Overloaded => 429,
            Self::ProviderError(_) => 502,
            Self::Internal(_) | Self::ToolCallRepairNeeded(_) => 500,
        }
    }
}

#[cfg(test)]
mod account_sync_tests {
    use super::*;

    #[test]
    fn validation_error_reason_is_passthrough() {
        let e = ds_core::PoolError::Validation("账号异常(muted/limited)，mute_until=123".into());
        let r = account_sync_failure_reason(&e);
        assert!(r.contains("muted/limited"));
    }

    #[test]
    fn already_exists_reason_mentions_account() {
        let e = ds_core::PoolError::AlreadyExists("a@b.com".into());
        let r = account_sync_failure_reason(&e);
        assert!(r.contains("a@b.com"));
        assert!(r.contains("已存在"));
    }

    #[test]
    fn report_serializes_failures_and_timeout() {
        let report = AccountSyncReport {
            added: 1,
            removed: 0,
            failed: 2,
            failures: vec![AccountSyncFailure {
                id: "x@y.com".into(),
                reason: "设备指纹被上游拒绝".into(),
            }],
            timed_out: false,
        };
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["added"], 1);
        assert_eq!(json["failed"], 2);
        assert_eq!(json["failures"][0]["id"], "x@y.com");
        assert_eq!(json["timed_out"], false);
    }

    #[test]
    fn default_report_is_empty_ok() {
        let report = AccountSyncReport::default();
        assert_eq!(report.added, 0);
        assert_eq!(report.failed, 0);
        assert!(report.failures.is_empty());
        assert!(!report.timed_out);
    }
}
