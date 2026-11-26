//! 流处理模块 —— 精简响应协议与 DeepSeek SSE 解析
//!
//! 协议定义：将 DeepSeek 原始 SSE 字节流（p/o/v patch 协议）转换为结构化事件序列，
//! 主 crate 只需消费 StreamEvent，不再感知底层格式。

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::{Buf, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::CoreError;
use crate::accounts::ClientError;
use crate::accounts::{AccountGuard, DsClient, StopStreamPayload};

use super::resume::{ResumeReplier, ResumedStream};

// ── 精简响应协议 ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Meta {
        account_id: String,
    },
    ThinkStart,
    ThinkDelta {
        content: String,
    },
    ContentStart,
    ContentDelta {
        content: String,
    },
    Done {
        finish_reason: Option<String>,
        accumulated_token_usage: Option<u32>,
    },
    /// 思考静默期的保活信号（不携带内容；上层序列化为 SSE 注释行 `: ping`）
    Ping,
}

/// 上游流结束时的收尾判定（暴露给上层的 supervisor 用于判断是否自动续写）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamOutcome {
    /// 上游明确 `FINISHED`
    Complete,
    /// 上游明确 `INCOMPLETE`（可能是被暂停/中断）
    Incomplete,
    /// 上游未给出结束状态就断流（`WIP` / 缺 `response/status`）
    Truncated,
}

// ── 内部类型 ──────────────────────────────────────────────────────────

pub(crate) struct ActiveSession {
    /// 该账号设备身份下的客户端视图（shutdown 清理时需要一致的 X-Device-Id）
    pub(crate) client: DsClient,
    pub(crate) token: String,
    pub(crate) session_id: String,
    pub(crate) message_id: i64,
}

pub(crate) struct SessionHandle {
    pub(crate) client: DsClient,
    pub(crate) token: String,
    pub(crate) session_id: String,
    pub(crate) message_id: i64,
    pub(crate) sessions: Arc<Mutex<HashMap<String, ActiveSession>>>,
    /// 复用会话所属账号（归还 / 失效时使用）
    pub(crate) account: Option<Arc<crate::accounts::Account>>,
    /// 本次请求的消息指纹链（归还时写入会话槽，供下一轮做增量匹配）
    pub(crate) chain: Vec<u64>,
    /// 是否按配置把正常结束的会话留在账号上（`session_policy = reuse`）
    pub(crate) keep_on_finish: bool,
}

impl SessionHandle {
    fn cleanup(&self, finished: bool) {
        let start = Instant::now();
        self.sessions.lock().unwrap().remove(&self.session_id);
        let client = self.client.clone();
        let token = self.token.clone();
        let session_id = self.session_id.clone();
        let message_id = self.message_id;
        // 「正常结束 + 会话复用策略开启 + 本次带增量链」才把会话留在账号上：
        // 注意判断依据是**策略**而不是「本轮是否复用了会话」—— 正是本轮新建的会话
        // 才需要被缓存下来供下一轮复用（否则复用永远不会发生）。
        // 客户端中断（!finished）时会话状态不明，按原逻辑删除更安全。
        let keep = self.keep_on_finish && finished && !self.chain.is_empty();
        let account = self.account.clone();
        let chain = self.chain.clone();
        let last_message_id = self.message_id;

        tokio::spawn(async move {
            if !finished {
                let payload = StopStreamPayload {
                    chat_session_id: session_id.clone(),
                    message_id,
                };
                if let Err(e) = client.stop_stream(&token, &payload).await {
                    log::warn!(target: "ds_core::accounts", "stop_stream failed: {e}");
                }
            }
            if keep {
                if let Some(account) = account {
                    account.put_cached_session(&session_id, chain, last_message_id);
                    log::info!(
                        target: "ds_core::accounts",
                        "session_kept: id={session_id}, cleanup_ms={}",
                        start.elapsed().as_millis()
                    );
                    return;
                }
                log::warn!(
                    target: "ds_core::accounts",
                    "复用模式但缺少账号引用，退化为删除: id={session_id}"
                );
            }
            if let Err(e) = client.delete_session(&token, &session_id).await {
                log::warn!(target: "ds_core::accounts", "delete_session failed: {e}");
            }
            log::info!(
                target: "ds_core::accounts",
                "session_deleted: id={}, finished={}, cleanup_ms={}",
                session_id, finished, start.elapsed().as_millis()
            );
        });
    }
}

// ── DeepSeek Patch 状态机 ─────────────────────────────────────────────

const FRAG_THINK: &str = "THINK";
const FRAG_RESPONSE: &str = "RESPONSE";
/// 2026-11 起官方 web 客户端把 `TEMPLATE_RESPONSE` 也当作回答正文
/// （`RESPONSE_TYPES:["RESPONSE","TEMPLATE_RESPONSE"]`），解析时与 `RESPONSE` 等价。
const FRAG_TEMPLATE_RESPONSE: &str = "TEMPLATE_RESPONSE";

/// 该 fragment 类型是否产出回答正文
fn is_response_frag(ty: &str) -> bool {
    ty == FRAG_RESPONSE || ty == FRAG_TEMPLATE_RESPONSE
}

/// Fragment 状态
///
/// 只记录类型：增量内容随事件**转移**给下游（下游是唯一读取方），
/// 不再逐条 `push_str` 累积 —— 长响应下可省下与响应等长的内存与拷贝。
struct Fragment {
    ty: String,
}

/// 维护 DeepSeek 响应的 patch 状态，对齐前端 DeltaParser
///
/// - `p` / `o` 跨事件持久化
/// - `o` 默认 "SET"
struct PatchState {
    current_path: Option<String>,
    current_op: Option<String>,
    fragments: Vec<Fragment>,
    status: Option<String>,
    accumulated_token_usage: Option<u32>,
    /// 是否已产出过非空 RESPONSE 内容（FINISHED 无内容告警用，避免缓存全文）
    response_has_content: bool,
    /// 顶层 `response/has_pending_fragment`：后台仍有片段未完成
    pending_fragment: bool,
    /// 最近一次 `event: close` 的 `auto_resume`：true = 上游建议继续生成
    auto_resume: bool,
}

impl PatchState {
    fn reset_for_resume(&mut self) {
        self.current_path = None;
        self.current_op = None;
        self.fragments = Vec::new();
        self.status = None;
        self.accumulated_token_usage = None;
        self.response_has_content = false;
        self.pending_fragment = false;
        self.auto_resume = false;
    }

    const fn new() -> Self {
        Self {
            current_path: None,
            current_op: None,
            fragments: Vec::new(),
            status: None,
            accumulated_token_usage: None,
            response_has_content: false,
            pending_fragment: false,
            auto_resume: false,
        }
    }

    /// 消费一帧 SSE 文本，返回零个或多个 StreamEvent
    fn apply_frame(&mut self, frame: &str) -> Result<Vec<StreamEvent>, CoreError> {
        if frame.is_empty() {
            return Ok(Vec::new());
        }

        // 提取 event 和 data 行
        let event_type = frame
            .lines()
            .find_map(|l| l.trim().strip_prefix("event:"))
            .map(|v| v.trim());

        let data = frame
            .lines()
            .find_map(|l| l.trim().strip_prefix("data:"))
            .map(|v| v.trim());

        let mut events = Vec::new();

        // event: ready → 不产出事件（在初始化阶段已处理）
        // event: hint → 检查错误
        if event_type == Some("hint")
            && let Some(data) = data
        {
            return Err(hint_to_error(data));
        }

        // event: close → 记录继续生成信号（`auto_resume`）；此前被静默忽略，
        // 导致上游「思考太久先暂停」的提示完全丢失
        if event_type == Some("close") {
            if let Some(data) = data
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(data)
                && let Some(ar) = val.get("auto_resume").and_then(serde_json::Value::as_bool)
            {
                if ar {
                    log::info!(
                        target: "ds_core::accounts",
                        "upstream close: auto_resume=true, status={:?}",
                        self.status
                    );
                }
                self.auto_resume = ar;
            }
            return Ok(Vec::new());
        }

        // 解析 data JSON
        if let Some(data) = data
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(data)
        {
            events.extend(self.apply_patch(val));
        }

        Ok(events)
    }

    /// 应用 p/o/v patch
    ///
    /// `val` 按值传入并原地取用：JSON 里的增量字符串直接**移出**交给下游事件，
    /// 避免「解析出一份 + 再拷贝一份」的双份分配。
    fn apply_patch(&mut self, mut val: serde_json::Value) -> Vec<StreamEvent> {
        // p/o 跨事件持久化
        if let Some(p) = val.get("p").and_then(|v| v.as_str()) {
            self.current_path = Some(p.to_string());
        }
        if let Some(o) = val.get("o").and_then(|v| v.as_str()) {
            self.current_op = Some(o.to_string());
        }

        let op = self.current_op.as_deref().unwrap_or("SET").to_string();
        let path = self.current_path.as_deref().unwrap_or("").to_string();

        let Some(v) = val.get_mut("v") else {
            return Vec::new();
        };

        // 初始快照：无 path 且 v 含 response
        if self.current_path.is_none()
            && let Some(response) = v.get_mut("response")
        {
            return self.apply_initial_snapshot(response);
        }

        // BATCH 递归分解
        if op == "BATCH" && v.is_array() {
            return self.apply_batch(&path, v);
        }

        self.apply_path(&path, &op, v)
    }

    fn apply_batch(&mut self, parent_path: &str, arr: &mut serde_json::Value) -> Vec<StreamEvent> {
        let mut events = Vec::new();
        let Some(arr) = arr.as_array_mut() else {
            return events;
        };

        // 子解析器独立状态
        let (mut sub_path, mut sub_op) = (String::new(), String::from("SET"));

        for item in arr {
            if let Some(p) = item.get("p").and_then(|v| v.as_str()) {
                sub_path = p.to_string();
            }
            if let Some(o) = item.get("o").and_then(|v| v.as_str()) {
                sub_op = o.to_string();
            }

            let Some(v) = item.get_mut("v") else {
                continue;
            };

            let full_path = if parent_path.is_empty() {
                sub_path.clone()
            } else if sub_path.is_empty() {
                parent_path.to_string()
            } else {
                format!("{parent_path}/{sub_path}")
            };

            if sub_op == "BATCH" {
                let mut batch_events = self.apply_batch(&full_path, v);
                events.append(&mut batch_events);
            } else {
                let mut path_events = self.apply_path(&full_path, &sub_op, v);
                events.append(&mut path_events);
            }
        }

        events
    }

    fn apply_initial_snapshot(&mut self, response: &mut serde_json::Value) -> Vec<StreamEvent> {
        let mut events = Vec::new();

        if let Some(s) = response.get("status").and_then(|v| v.as_str()) {
            self.status = Some(s.to_string());
        }

        if let Some(n) = response
            .get("accumulated_token_usage")
            .and_then(|v| v.as_u64())
        {
            self.accumulated_token_usage = Some(u32::try_from(n).unwrap_or(u32::MAX));
        }

        if let Some(arr) = response.get_mut("fragments").and_then(|f| f.as_array_mut()) {
            self.fragments.clear();
            for frag in arr {
                let Some(obj) = frag.as_object_mut() else {
                    continue;
                };
                let Some(ty) = obj.remove("type").and_then(take_string_owned) else {
                    continue;
                };
                let content = obj
                    .get_mut("content")
                    .and_then(take_string)
                    .unwrap_or_default();
                let is_think = ty == FRAG_THINK;
                let is_response = is_response_frag(&ty);
                if is_response && !content.is_empty() {
                    self.response_has_content = true;
                }
                if !content.is_empty() {
                    if is_think {
                        events.push(StreamEvent::ThinkDelta { content });
                    } else if is_response {
                        events.push(StreamEvent::ContentDelta { content });
                    }
                }
                self.fragments.push(Fragment { ty });
            }
        }

        events
    }

    fn apply_path(
        &mut self,
        path: &str,
        op: &str,
        val: &mut serde_json::Value,
    ) -> Vec<StreamEvent> {
        let mut events = Vec::new();

        match path {
            "response/status" | "/response/status" => {
                if let Some(s) = take_string(val) {
                    self.status = Some(s);
                }
            }
            "response/accumulated_token_usage"
            | "accumulated_token_usage"
            | "/response/accumulated_token_usage"
            | "/accumulated_token_usage" => {
                if let Some(n) = val.as_u64() {
                    self.accumulated_token_usage = Some(u32::try_from(n).unwrap_or(u32::MAX));
                }
            }
            "response/has_pending_fragment"
            | "/response/has_pending_fragment"
            | "has_pending_fragment" => {
                if let Some(b) = val.as_bool() {
                    self.pending_fragment = b;
                }
            }
            "response/fragments/-1/content" | "/response/fragments/-1/content" => {
                if let Some(s) = take_string(val)
                    && let Some(frag) = self.fragments.last()
                {
                    match frag.ty.as_str() {
                        FRAG_THINK => events.push(StreamEvent::ThinkDelta { content: s }),
                        _ if is_response_frag(&frag.ty) => {
                            self.response_has_content = true;
                            events.push(StreamEvent::ContentDelta { content: s });
                        }
                        _ => {}
                    }
                }
            }
            // 2026-11 起：新片段开始时上游改为 `o:"SET"` 整体替换 fragment 数组
            // （此前是 `o:"APPEND"` 只追加新片段）。只处理 APPEND 会让新 RESPONSE 片段及其
            // 首个分片被丢弃，后续 `-1/content` 增量落到仍在的 THINK 上，最终正文为空。
            //
            // 处理原则：已存在的片段只更新类型、保留已流出的内容；超出已知数量的新片段才产出
            // 其初始 content，这样对「SET 是完整快照」与「SET 只带新增」两种形态都安全。
            "response/fragments" | "/response/fragments" if op == "SET" && val.is_array() => {
                let known = self.fragments.len();
                if let Some(arr) = val.as_array_mut() {
                    for (i, item) in arr.iter_mut().enumerate() {
                        let Some(obj) = item.as_object_mut() else {
                            continue;
                        };
                        let Some(ty) = obj.remove("type").and_then(take_string_owned) else {
                            continue;
                        };
                        if i < known {
                            self.fragments[i].ty = ty;
                            continue;
                        }
                        let content = obj
                            .get_mut("content")
                            .and_then(take_string)
                            .unwrap_or_default();
                        if !content.is_empty() {
                            if ty == FRAG_THINK {
                                events.push(StreamEvent::ThinkDelta { content });
                            } else if is_response_frag(&ty) {
                                self.response_has_content = true;
                                events.push(StreamEvent::ContentDelta { content });
                            }
                        }
                        self.fragments.push(Fragment { ty });
                    }
                }
            }
            "response/fragments" | "/response/fragments" if op == "APPEND" => {
                // 某些版本 APPEND 单个 fragment 对象而非数组，统一包成数组处理
                if val.is_object() {
                    let one = val.take();
                    *val = serde_json::Value::Array(vec![one]);
                }
                if let Some(arr) = val.as_array_mut() {
                    for item in arr {
                        let Some(obj) = item.as_object_mut() else {
                            continue;
                        };
                        let Some(ty) = obj.remove("type").and_then(take_string_owned) else {
                            continue;
                        };
                        let content = obj
                            .get_mut("content")
                            .and_then(take_string)
                            .unwrap_or_default();
                        let is_think = ty == FRAG_THINK;
                        let is_response = is_response_frag(&ty);
                        if !content.is_empty() {
                            if is_think {
                                events.push(StreamEvent::ThinkDelta { content });
                            } else if is_response {
                                self.response_has_content = true;
                                events.push(StreamEvent::ContentDelta { content });
                            }
                        }
                        self.fragments.push(Fragment { ty });
                    }
                }
            }
            _ => {
                // 上游协议随时可能新增 path；记 debug 便于下一次格式变更被看见而非静默丢弃
                log::debug!(
                    target: "ds_core::accounts",
                    "unhandled patch path={path:?} op={op} value_kind={}",
                    json_kind(val)
                );
            }
        }

        events
    }
}

/// `serde_json::Value` 的种类名（仅用于诊断日志）
const fn json_kind(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::String(_) => "string",
        serde_json::Value::Object(_) => "object",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Null => "null",
    }
}

/// 取出 JSON 字符串的所有权（内容不复制）
fn take_string(val: &mut serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::String(s) => Some(std::mem::take(s)),
        _ => None,
    }
}

/// 取出 `serde_json::Value` 内部字符串的所有权
fn take_string_owned(val: serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::String(s) => Some(s),
        _ => None,
    }
}

/// SSE 帧字节 → 文本
///
/// 帧内是 UTF-8 JSON：合法时**借用**帧缓冲（零拷贝），
/// 仅在非法字节序列时退化为有损解码。
fn frame_str(frame: &[u8]) -> Cow<'_, str> {
    std::str::from_utf8(frame).map_or_else(|_| String::from_utf8_lossy(frame), Cow::Borrowed)
}

// ── 响应阶段跟踪 ─────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
enum Phase {
    Init,
    Thinking,
    Content,
    Done,
}

pin_project! {
    pub(crate) struct ResponseStream {
        #[pin]
        raw: Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>,
        _guard: AccountGuard,
        session: SessionHandle,

        buf: BytesMut,
        patch_state: PatchState,
        phase: Phase,
        pending: VecDeque<StreamEvent>,
        meta_sent: bool,
        account_id: String,
        finished: bool,
        // 单轮流结束（已 cleanup）；续写轮次复用同一空间
        cycle_complete: bool,
        // 是否至少成功续写过一次
        resumed_any: bool,
        // 已下发 INCOMPLETE 结束（续写条件之一）
        resume_incomplete: bool,
        // 纯 thinking 静默期的保活心跳（配置为 0 时不创建）
        heartbeat: Option<tokio::time::Interval>,
        // 流结束后由 supervisor 读取的收尾判定
        outcome: StreamOutcome,
        // 截断续写回调（None = 未启用自动续写）
        replier: Option<ResumeReplier>,
        // 进行中的续写请求
        #[pin]
        resume_fut: Option<Pin<Box<dyn Future<Output = Result<ResumedStream, CoreError>> + Send>>>,
        // 已尝试续写次数
        resume_attempts: u32,
        // 续写次数上限（0 = 不续写）
        resume_cap: u32,
        // 续写时使用空 prompt
        auto_resume_empty_prompt: bool,
    }

    impl PinnedDrop for ResponseStream {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            this.session.cleanup(*this.finished);
        }
    }
}

impl ResponseStream {
    pub(crate) fn new(
        raw: Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>,
        guard: AccountGuard,
        session: SessionHandle,
        account_id: String,
        heartbeat_secs: u64,
    ) -> Self {
        let heartbeat = (heartbeat_secs > 0)
            .then(|| tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs)));
        Self {
            raw,
            _guard: guard,
            session,
            buf: BytesMut::new(),
            patch_state: PatchState::new(),
            phase: Phase::Init,
            pending: VecDeque::new(),
            meta_sent: false,
            account_id,
            finished: false,
            cycle_complete: false,
            resumed_any: false,
            resume_incomplete: false,
            heartbeat,
            outcome: StreamOutcome::Complete,
            replier: None,
            resume_fut: None,
            resume_attempts: 0,
            resume_cap: 0,
            auto_resume_empty_prompt: true,
        }
    }

    /// 安装截断续写回调（由 `Chat::v0_chat` 在配置开启时注入）
    pub(crate) fn install_resume(&mut self, replier: ResumeReplier, cap: u32, empty_prompt: bool) {
        self.replier = Some(replier);
        self.resume_cap = cap;
        self.auto_resume_empty_prompt = empty_prompt;
    }
}

impl Stream for ResponseStream {
    type Item = Result<StreamEvent, CoreError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // 首个事件始终是 Meta（续写轮次由内层流各自重新下发）
        if !*this.meta_sent {
            *this.meta_sent = true;
            return Poll::Ready(Some(Ok(StreamEvent::Meta {
                account_id: this.account_id.clone(),
            })));
        }

        // 先清空 pending 队列
        if let Some(evt) = this.pending.pop_front() {
            return Poll::Ready(Some(Ok(evt)));
        }

        if *this.phase == Phase::Done {
            return Poll::Ready(None);
        }

        loop {
            if let Some(frame) = take_frame(this.buf) {
                let events = match this.patch_state.apply_frame(frame_str(&frame).as_ref()) {
                    Ok(evts) => evts,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };

                if events.is_empty() {
                    continue;
                }

                // 过滤：阶段切换信号插入 + 延后排队
                let mut filtered = VecDeque::new();
                if finalize_events(this.patch_state, this.phase, events, &mut filtered)
                    == Some(true)
                {
                    *this.resume_incomplete = true;
                }

                // 第一个事件立即返回（移动而非克隆），其余入 pending
                if let Some(first) = filtered.pop_front() {
                    this.pending.extend(filtered);
                    return Poll::Ready(Some(Ok(first)));
                }
            }

            match this.raw.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    this.buf.extend_from_slice(&bytes);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(CoreError::Stream(e.to_string()))));
                }
                Poll::Ready(None) => {
                    // 一轮上游流结束：先冲刷残留帧（最后一帧可能不带空行）
                    let mut flushed = cycle_flush(
                        this.buf,
                        this.patch_state,
                        this.phase,
                        this.resume_incomplete,
                        this.outcome,
                    );
                    if let Some(first) = flushed.pop_front() {
                        this.pending.extend(flushed);
                        return Poll::Ready(Some(Ok(first)));
                    }
                    // 判定是否续写
                    let plan = super::resume::plan_resume(
                        *this.resume_cap,
                        *this.resume_attempts,
                        *this.outcome,
                    );
                    if !plan.resume {
                        *this.phase = Phase::Done;
                        *this.finished = true;
                        this.session.cleanup(true);
                        let finish = super::resume::finish_reason_after_resume(
                            *this.outcome,
                            *this.resumed_any,
                        )
                        .to_owned();
                        return Poll::Ready(Some(Ok(StreamEvent::Done {
                            finish_reason: Some(finish),
                            accumulated_token_usage: None,
                        })));
                    }

                    // 起一次续写（上限计数在流侧，单次 replier 用完即弃）
                    let Some(replier) = this.replier.as_mut() else {
                        *this.phase = Phase::Done;
                        *this.finished = true;
                        this.session.cleanup(true);
                        return Poll::Ready(None);
                    };
                    *this.resume_attempts += 1;
                    let ticket = super::resume::ResumeTicket {
                        session_id: this.session.session_id.clone(),
                        parent_message_id: this.session.message_id,
                        prompt: super::resume::resume_prompt(*this.auto_resume_empty_prompt),
                        request_id: String::new(),
                    };
                    log::info!(
                        target: "ds_core::accounts",
                        "auto_resume: attempt={}/{}, session={}",
                        *this.resume_attempts,
                        *this.resume_cap,
                        ticket.session_id
                    );
                    *this.resume_fut = Some(replier(ticket));

                    // 同步推进一次续写请求，可能立即 Ready
                    let ready = this
                        .resume_fut
                        .as_mut()
                        .as_pin_mut()
                        .map(|fut| fut.poll(cx));
                    match ready {
                        None | Some(Poll::Pending) => return Poll::Pending,
                        Some(Poll::Ready(Ok(resumed))) => {
                            this.resume_fut.set(None);
                            *this.raw = resumed.raw;
                            this.buf.clear();
                            this.patch_state.reset_for_resume();
                            this.session.message_id = resumed.message_id;
                            *this.phase = Phase::Init;
                            *this.finished = false;
                            *this.cycle_complete = false;
                            *this.resume_incomplete = false;
                            if let Some(iv) = this.heartbeat.as_mut() {
                                iv.reset();
                            }
                            *this.resumed_any = true;
                            continue;
                        }
                        Some(Poll::Ready(Err(e))) => {
                            log::warn!(
                                target: "ds_core::accounts",
                                "auto_resume failed: {e}"
                            );
                            *this.phase = Phase::Done;
                            *this.finished = true;
                            this.session.cleanup(true);
                            let finish = super::resume::finish_reason_after_resume(
                                *this.outcome,
                                *this.resumed_any,
                            )
                            .to_owned();
                            return Poll::Ready(Some(Ok(StreamEvent::Done {
                                finish_reason: Some(finish),
                                accumulated_token_usage: None,
                            })));
                        }
                    }
                }
                Poll::Pending => {
                    // 纯 thinking 静默期：上游暂时没有字节，但思考还没结束。
                    // 按配置周期下发 Ping（上层序列化为 SSE 注释行），保住
                    // 客户端 ↔ 代理之间中间层的空闲连接，不改变任何语义。
                    if *this.phase == Phase::Thinking
                        && let Some(iv) = this.heartbeat.as_mut()
                        && iv.poll_tick(cx).is_ready()
                    {
                        return Poll::Ready(Some(Ok(StreamEvent::Ping)));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

// ── 截断续写调度 ─────────────────────────────────────────────────────

/// 冲刷缓冲区剩余帧并收尾一轮（含收尾判定与告警）
fn cycle_flush(
    buf: &mut BytesMut,
    patch_state: &mut PatchState,
    phase: &mut Phase,
    resume_incomplete: &mut bool,
    outcome: &mut StreamOutcome,
) -> VecDeque<StreamEvent> {
    let mut flushed = VecDeque::new();
    loop {
        let frame = if let Some(f) = take_frame(buf) {
            f
        } else if buf.is_empty() {
            break;
        } else {
            buf.split()
        };
        let Ok(events) = patch_state.apply_frame(frame_str(&frame).as_ref()) else {
            break;
        };
        if finalize_events(patch_state, phase, events, &mut flushed) == Some(true) {
            *resume_incomplete = true;
        }
    }
    if *phase != Phase::Done
        && finalize_events(patch_state, phase, Vec::new(), &mut flushed) == Some(true)
    {
        *resume_incomplete = true;
    }
    *outcome = classify_outcome(patch_state);
    if *outcome == StreamOutcome::Truncated && !patch_state.response_has_content {
        log::warn!(
            target: "ds_core::accounts",
            "upstream 截断: status={:?}, pending_fragment={}, auto_resume={}, thinking_only=true",
            patch_state.status,
            patch_state.pending_fragment,
            patch_state.auto_resume
        );
    }
    flushed
}

// ── SSE 帧提取 ───────────────────────────────────────────────────────

/// 取出一个完整 SSE 帧（不含分隔的 `\n\n`）
///
/// 用 `BytesMut::split_to` 做 O(1) 指针切分：不搬运剩余字节，也不做 UTF-8 拷贝。
fn take_frame(buf: &mut BytesMut) -> Option<BytesMut> {
    let pos = buf.windows(2).position(|w| w == b"\n\n")?;
    let frame = buf.split_to(pos);
    buf.advance(2);
    Some(frame)
}

fn hint_to_error(data: &str) -> CoreError {
    let val: serde_json::Value = serde_json::from_str(data).unwrap_or_default();
    let content = val
        .get("content")
        .or_else(|| val.get("finish_reason"))
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");

    if content.contains("rate_limit") {
        CoreError::Overloaded
    } else if content.contains("input_exceeds_limit") {
        CoreError::ProviderError("输入内容超长，请缩短后重试".into())
    } else {
        CoreError::ProviderError(format!("hint: {content}"))
    }
}

// ── 内部辅助（供 ResponseStream 使用） ─────────────────────────────────

/// 对一帧解析出的事件做后处理：插入阶段切换信号，并在 status 结束时追加 Done
///
/// 正常循环与「流意外结束（EOF）时冲刷缓冲区」两条路径共用，
/// 避免 EOF 路径丢失上游已经下发的 `finish_reason` 与 `accumulated_token_usage`。
fn finalize_events(
    patch_state: &PatchState,
    phase: &mut Phase,
    events: Vec<StreamEvent>,
    out: &mut VecDeque<StreamEvent>,
) -> Option<bool> {
    let mut incomplete = None;
    for evt in events {
        match &evt {
            StreamEvent::ThinkDelta { .. } if *phase == Phase::Init || *phase == Phase::Content => {
                *phase = Phase::Thinking;
                out.push_back(StreamEvent::ThinkStart);
            }
            StreamEvent::ContentDelta { .. }
                if *phase == Phase::Init || *phase == Phase::Thinking =>
            {
                *phase = Phase::Content;
                out.push_back(StreamEvent::ContentStart);
            }
            _ => {}
        }
        out.push_back(evt);
    }

    if let Some(status) = &patch_state.status
        && matches!(status.as_str(), "FINISHED" | "INCOMPLETE" | "WIP")
        && *phase != Phase::Done
    {
        *phase = Phase::Done;
        // INCOMPLETE 是上游的「先这样」结束：给出 length 让上层知道输出不完整
        // （否则会被当成正常结束），是否续写由 supervisor 决定。
        let is_incomplete = status == "INCOMPLETE";
        let finish = (!is_incomplete).then(|| "stop".to_string());
        incomplete = Some(is_incomplete);
        let usage = patch_state.accumulated_token_usage;
        if finish.is_some() && !has_response_content(patch_state) {
            log::warn!(
                target: "ds_core::accounts",
                "状态机 FINISHED 但无 RESPONSE 内容"
            );
        }
        out.push_back(StreamEvent::Done {
            finish_reason: finish,
            accumulated_token_usage: usage,
        });
    }
    incomplete
}

/// 检查是否有非空的 RESPONSE 内容（用于 FINISHED 告警）
const fn has_response_content(state: &PatchState) -> bool {
    state.response_has_content
}

/// 判定上游流结束的收尾类型（供上层 supervisor 决定是否自动续写）
///
/// - `status = FINISHED` → `Complete`
/// - `status = INCOMPLETE` → `Incomplete`（用户中断 / 上游暂停）
/// - 其它（`WIP`、无 status、已断流）→ `Truncated`
fn classify_outcome(state: &PatchState) -> StreamOutcome {
    match state.status.as_deref() {
        Some("FINISHED") => StreamOutcome::Complete,
        Some("INCOMPLETE") => StreamOutcome::Incomplete,
        _ => StreamOutcome::Truncated,
    }
}

// ── 公开辅助（供 request.rs 初始化阶段使用） ──────────────────────────

pub(crate) fn split_two_events(buf: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = buf.splitn(3, "\n\n").collect();
    (parts.len() >= 3).then(|| (parts[0], parts[1]))
}

pub(crate) fn check_hint(event_block: &str) -> Option<CoreError> {
    let is_hint = event_block.lines().any(|l| {
        l.trim()
            .strip_prefix("event:")
            .is_some_and(|v| v.trim() == "hint")
    });
    if !is_hint {
        return None;
    }
    if event_block.contains("rate_limit") {
        return Some(CoreError::Overloaded);
    }
    if event_block.contains("input_exceeds_limit") {
        return Some(CoreError::ProviderError(
            "输入内容超长，请缩短后重试".into(),
        ));
    }
    None
}

pub(crate) fn parse_ready_message_ids(chunk: &[u8]) -> (i64, i64) {
    let text = std::str::from_utf8(chunk).ok();
    if let Some(text) = text {
        for line in text.lines() {
            if let Some(data) = line.strip_prefix("data: ")
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(data)
                && let (Some(r), Some(s)) = (
                    val.get("request_message_id").and_then(|v| v.as_i64()),
                    val.get("response_message_id").and_then(|v| v.as_i64()),
                )
            {
                return (r, s);
            }
        }
    }
    (1, 2)
}

pub(crate) fn parse_json_error(text: &str, request_id: &str) -> CoreError {
    let raw = text.trim();
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(raw)
        && let Some(code) = val.get("code").and_then(|c| c.as_i64())
    {
        let msg = val
            .get("msg")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown")
            .to_string();
        log::error!(
            target: "ds_core::accounts",
            "req={request_id} JSON 错误响应: code={code}, msg={msg}"
        );
        return match code {
            1001 | 1201 => CoreError::Overloaded,
            40301 => CoreError::ProviderError(format!("INVALID_POW_RESPONSE: {msg}")),
            _ => CoreError::ProviderError(format!("API error code={code}: {msg}")),
        };
    }
    log::error!(
        target: "ds_core::accounts",
        "req={} 无法解析的响应: {}", request_id, raw.chars().take(200).collect::<String>()
    );
    CoreError::Stream(format!(
        "无法解析的响应: {}",
        raw.chars().take(200).collect::<String>()
    ))
}

pub(crate) async fn wait_ready_and_update(
    stream: &mut Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>,
    request_id: &str,
    chunk_index: usize,
    total_chunks: usize,
) -> Result<(i64, Vec<u8>), CoreError> {
    let mut buf = Vec::new();
    let mut ready_msg_id: Option<i64> = None;
    loop {
        let chunk = stream
            .next()
            .await
            .ok_or_else(|| {
                let raw = String::from_utf8_lossy(&buf);
                if raw.trim().starts_with('{') {
                    return parse_json_error(&raw, request_id);
                }
                CoreError::Stream(format!(
                    "req={request_id} 分块 {chunk_index}/{total_chunks} 收到空流"
                ))
            })?
            .map_err(|e| CoreError::Stream(e.to_string()))?;
        buf.extend_from_slice(&chunk);
        let text = String::from_utf8_lossy(&buf);

        let events: Vec<&str> = text.split("\n\n").collect();
        let n_complete = if text.ends_with("\n\n") {
            events.len()
        } else {
            events.len().saturating_sub(1)
        };

        for event in &events[..n_complete] {
            if event.is_empty() {
                continue;
            }
            if let Some(err) = check_hint(event) {
                return Err(err);
            }
            if event.lines().any(|l| {
                l.trim()
                    .strip_prefix("event:")
                    .is_some_and(|v| v.trim() == "ready")
            }) {
                ready_msg_id = Some(parse_ready_message_ids(event.as_bytes()).1);
            }
            if let Some(id) = ready_msg_id
                && event.lines().any(|l| {
                    l.trim()
                        .strip_prefix("event:")
                        .is_some_and(|v| v.trim() == "update_session")
                })
            {
                return Ok((id, buf));
            }
        }
    }
}

pub(crate) async fn wait_close(
    stream: &mut Pin<Box<dyn Stream<Item = Result<Bytes, ClientError>> + Send>>,
    buf: &mut Vec<u8>,
    request_id: &str,
    chunk_index: usize,
    total_chunks: usize,
) -> Result<(), CoreError> {
    loop {
        let text = String::from_utf8_lossy(buf);
        let events: Vec<&str> = text.split("\n\n").collect();
        let n_complete = if text.ends_with("\n\n") {
            events.len()
        } else {
            events.len().saturating_sub(1)
        };

        for event in &events[..n_complete] {
            if event.lines().any(|l| {
                l.trim()
                    .strip_prefix("event:")
                    .is_some_and(|v| v.trim() == "close")
            }) {
                return Ok(());
            }
        }

        let chunk = stream
            .next()
            .await
            .ok_or_else(|| {
                CoreError::Stream(format!(
                    "req={request_id} 分块 {chunk_index}/{total_chunks} 流在 close 前结束"
                ))
            })?
            .map_err(|e| CoreError::Stream(e.to_string()))?;
        buf.extend_from_slice(&chunk);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 回归：上游在 status=FINISHED 之后直接断流时，`accumulated_token_usage` 与
    /// `finish_reason` 仍必须出现在 Done 事件里（issue #93：completion_tokens 恒为 0）
    #[test]
    fn finalize_events_keeps_usage_when_status_finished() {
        let mut state = PatchState::new();
        state.status = Some("FINISHED".to_string());
        state.accumulated_token_usage = Some(87);
        let mut phase = Phase::Content;
        let mut out = VecDeque::new();

        finalize_events(&state, &mut phase, Vec::new(), &mut out);

        assert_eq!(phase, Phase::Done);
        match out.make_contiguous() {
            [
                StreamEvent::Done {
                    finish_reason,
                    accumulated_token_usage,
                },
            ] => {
                assert_eq!(finish_reason.as_deref(), Some("stop"));
                assert_eq!(*accumulated_token_usage, Some(87));
            }
            other => panic!("期望单个 Done 事件，实际: {:?}", other.len()),
        }
    }

    /// INCOMPLETE 不应伪装成正常结束，但仍需带上 usage
    #[test]
    fn finalize_events_incomplete_has_no_finish_reason() {
        let mut state = PatchState::new();
        state.status = Some("INCOMPLETE".to_string());
        state.accumulated_token_usage = Some(12);
        let mut phase = Phase::Content;
        let mut out = VecDeque::new();

        finalize_events(&state, &mut phase, Vec::new(), &mut out);

        match out.make_contiguous() {
            [
                StreamEvent::Done {
                    finish_reason,
                    accumulated_token_usage,
                },
            ] => {
                assert_eq!(*finish_reason, None);
                assert_eq!(*accumulated_token_usage, Some(12));
            }
            other => panic!("期望单个 Done 事件，实际: {:?}", other.len()),
        }
    }

    /// 已经进入 Done 阶段后不得重复追加 Done（两条路径共用 finalize 时的去重保证）
    #[test]
    fn finalize_events_does_not_duplicate_done() {
        let mut state = PatchState::new();
        state.status = Some("FINISHED".to_string());
        let mut phase = Phase::Done;
        let mut out = VecDeque::new();

        finalize_events(&state, &mut phase, Vec::new(), &mut out);

        assert!(out.is_empty(), "Done 阶段不应再追加事件: {out:?}");
    }

    /// 阶段切换信号与 Done 的相对顺序：ContentStart 先于 Done，且 Done 只出现一次
    #[test]
    fn finalize_events_inserts_content_start_before_done() {
        let mut state = PatchState::new();
        state.status = Some("FINISHED".to_string());
        state.accumulated_token_usage = Some(5);
        let mut phase = Phase::Init;
        let mut out = VecDeque::new();

        finalize_events(
            &state,
            &mut phase,
            vec![StreamEvent::ContentDelta {
                content: "hi".into(),
            }],
            &mut out,
        );

        assert!(matches!(&out[0], StreamEvent::ContentStart));
        assert!(matches!(&out[1], StreamEvent::ContentDelta { .. }));
        assert!(matches!(&out[2], StreamEvent::Done { .. }));
        assert_eq!(out.len(), 3);
    }

    /// 抽取仅含正文的 ContentDelta 文本，便于断言
    fn content_text(events: &[StreamEvent]) -> String {
        let mut s = String::new();
        for e in events {
            if let StreamEvent::ContentDelta { content } = e {
                s.push_str(content);
            }
        }
        s
    }

    /// issue #125：2026-11 起上游用 `o:"SET"` 整体替换 fragment 数组来开启 RESPONSE 片段。
    /// 只处理 APPEND 会让首个正文分片被丢弃、后续增量落到 THINK 上，最终正文为空。
    #[test]
    fn fragments_set_replaces_and_emits_new_response_fragment() {
        let mut state = PatchState::new();

        // 1) 先开启 THINK 片段（APPEND，旧形态）
        state.apply_frame(
            r#"data: {"p":"response/fragments","o":"APPEND","v":[{"type":"THINK","content":"思考"}]}"#,
        ).unwrap();

        // 2) 回答开始时上游发 SET 整个数组：已知的 THINK + 新的 RESPONSE（首片 "Capital"）
        let events = state.apply_frame(
            r#"data: {"p":"response/fragments","o":"SET","v":[{"type":"THINK"},{"type":"RESPONSE","content":"Capital"}]}"#,
        ).unwrap();
        assert_eq!(content_text(&events), "Capital");
        assert_eq!(state.fragments.len(), 2);
        assert_eq!(state.fragments[1].ty, FRAG_RESPONSE);

        // 3) 后续 -1/content 增量必须落到 RESPONSE，而不是 THINK
        let events = state.apply_frame(
            r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":"a Franței este Paris."}"#,
        ).unwrap();
        assert_eq!(content_text(&events), "a Franței este Paris.");
        assert!(state.response_has_content);
    }

    /// 已知片段在 SET 快照里再次出现时只更新类型、不重复产出内容
    #[test]
    fn fragments_set_does_not_replay_known_content() {
        let mut state = PatchState::new();
        state.apply_frame(
            r#"data: {"p":"response/fragments","o":"APPEND","v":[{"type":"RESPONSE","content":"abc"}]}"#,
        ).unwrap();

        // SET 携带已存在片段的完整内容（快照形态），不得重复下发
        let events = state.apply_frame(
            r#"data: {"p":"response/fragments","o":"SET","v":[{"type":"RESPONSE","content":"abc"}]}"#,
        ).unwrap();
        assert!(events.is_empty(), "已存在的片段不应重复产出: {events:?}");
        assert_eq!(state.fragments.len(), 1);
    }

    /// TEMPLATE_RESPONSE 与 RESPONSE 一样算回答正文（web 客户端 RESPONSE_TYPES）
    #[test]
    fn template_response_is_treated_as_content() {
        let mut state = PatchState::new();
        let events = state.apply_frame(
            r#"data: {"p":"response/fragments","o":"SET","v":[{"type":"TEMPLATE_RESPONSE","content":"hi"}]}"#,
        ).unwrap();
        assert_eq!(content_text(&events), "hi");
        assert!(state.response_has_content);
    }

    /// APPEND 单个 fragment 对象（非数组）也应被接受
    #[test]
    fn fragments_append_accepts_single_object() {
        let mut state = PatchState::new();
        let events = state.apply_frame(
            r#"data: {"p":"response/fragments","o":"APPEND","v":{"type":"RESPONSE","content":"x"}}"#,
        ).unwrap();
        assert_eq!(content_text(&events), "x");
        assert_eq!(state.fragments.len(), 1);
    }
}
