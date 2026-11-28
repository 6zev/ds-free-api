//! DeepSeek 核心模块 —— OpenAI API 到 DeepSeek 的适配层
//!
//! 对外暴露最小接口：DsCore, CoreError, ChatRequest, DsCoreConfig, AccountConfig

mod accounts;
mod chat;
mod config;

pub use accounts::PoolError;
pub use accounts::pool::AccountStatus;
pub use accounts::{
    ChatSessionInfo, ClientIdentity, DsClient, EmulationProfile, FetchSessionsData, HifConfig,
    LoginPayload,
};
pub use chat::{ChatRequest, ChatResponse, DeltaPrompt, FilePayload, StreamEvent, StreamOutcome};
pub use config::{AccountConfig, AutoResumeConfig, DsCoreConfig};

use accounts::Accounts;
use chat::Chat;
use std::sync::Arc;

/// 内核层错误类型
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// 服务过载：所有账号都在忙或不健康
    #[error("no available account")]
    Overloaded,

    /// PoW 计算失败
    #[error("proof of work failed: {0}")]
    ProofOfWorkFailed(#[from] accounts::PowError),

    /// 提供商错误：网络、业务错误、Token 失效等
    #[error("provider: {0}")]
    ProviderError(String),

    /// 流处理错误：连接中断等
    #[error("stream error: {0}")]
    Stream(String),
}

impl From<accounts::ClientError> for CoreError {
    fn from(e: accounts::ClientError) -> Self {
        CoreError::ProviderError(e.to_string())
    }
}

pub struct DsCore {
    accounts: Arc<Accounts>,
    chat: Chat,
}

impl DsCore {
    pub async fn new(
        config: &DsCoreConfig,
        account_creds: Vec<AccountConfig>,
    ) -> Result<Self, CoreError> {
        let accounts = Accounts::new(config, account_creds).await?;
        let chat = Chat::new(Arc::clone(&accounts), config);

        Ok(Self { accounts, chat })
    }

    /// 发起对话请求，返回 SSE 字节流 + 账号标识
    ///
    /// 流结束或丢弃时自动释放账号
    pub async fn v0_chat(
        &self,
        req: ChatRequest,
        request_id: &str,
    ) -> Result<ChatResponse, CoreError> {
        self.chat.v0_chat(req, request_id).await
    }

    #[must_use]
    pub fn account_statuses(&self) -> Vec<AccountStatus> {
        self.accounts.account_statuses()
    }

    /// 获取详细账号状态（含滑动窗口统计、请求间隔等）
    #[must_use]
    pub fn account_statuses_detailed(&self) -> Vec<AccountStatus> {
        self.accounts.account_statuses_detailed()
    }

    /// 拉取账号的会话列表（分页；`updated_at` 为游标，None = 第一页）
    pub async fn fetch_sessions(
        &self,
        updated_at: Option<f64>,
    ) -> Result<accounts::FetchSessionsData, CoreError> {
        self.accounts.fetch_sessions(updated_at).await
    }

    /// 动态添加账号
    pub async fn add_account(&self, creds: &AccountConfig) -> Result<String, PoolError> {
        self.accounts.add_account(creds).await
    }

    /// 动态移除账号
    pub fn remove_account(&self, email_or_mobile: &str) -> Result<String, PoolError> {
        self.accounts.remove_account(email_or_mobile)
    }

    /// 标记账号为 Error 状态
    pub fn mark_error(&self, email_or_mobile: &str) {
        self.accounts.mark_error(email_or_mobile);
    }

    /// 手动重新登录指定账号
    pub async fn re_login_single(&self, email_or_mobile: &str) -> Result<(), String> {
        self.accounts.re_login_single(email_or_mobile).await
    }

    /// 优雅关闭：清理所有残留 session（活动中的 + 复用缓存的）
    pub async fn shutdown(&self) {
        self.chat.shutdown().await;
        let reaped = self.accounts.reap_all_sessions().await;
        if reaped > 0 {
            log::info!(target: "ds_core::accounts", "shutdown: 回收 {reaped} 个复用会话");
        }
    }

    /// 用同一个（拟态身份 + 代理）HTTP 客户端取 `GET url` 的文本
    ///
    /// 自更新（OTA）与 `client/settings` 元数据探测都走这里，而不是在根 crate
    /// 里另加一个 HTTP 客户端依赖 —— 这样 GitHub / 上游请求带的是同一套 TLS
    /// 指纹与 `[proxy]` 配置。返回 `(status, body)`；非 2xx 也返回 body，
    /// 由调用方决定是否报错。
    pub async fn fetch_text(&self, url: &str) -> Result<(u16, String), CoreError> {
        self.accounts.fetch_text(url).await
    }

    /// 用同一个 HTTP 客户端取 `GET url` 的原始字节（下载发布产物用）
    pub async fn fetch_bytes(&self, url: &str) -> Result<Vec<u8>, CoreError> {
        self.accounts.fetch_bytes(url).await
    }

    pub async fn reload_config(&self, config: &DsCoreConfig) -> Result<(), CoreError> {
        self.chat.reload_auto_resume(&config.auto_resume);
        self.accounts.reload_config(config).await
    }
}
