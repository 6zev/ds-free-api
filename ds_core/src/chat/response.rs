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

use super::resume::ResumeReplier;

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
    /// 只把会话从复用缓存里摘掉（不发 stop_stream、不删上游会话）。
    ///
    /// 截断续写前用：会话本身还要继续写，但空回复不能留在缓存里被下一轮复用。
    fn drop_cache_only(&self) {
        self.sessions.lock().unwrap().remove(&self.session_id);
    }

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
    /// 粘滞的 `p`：只在当前帧内有效，裸增量帧用完即清
    sticky_path: Option<String>,
    /// 粘滞的 `o`：与 `sticky_path` 同生命周期
    sticky_op: Option<String>,
    fragments: Vec<Fragment>,
    status: Option<String>,
    accumulated_token_usage: Option<u32>,
    /// 是否已产出过非空 RESPONSE 内容（FINISHED 无内容告警用，避免缓存全文）
    response_has_content: bool,
    /// 顶层 `response/has_pending_fragment`：后台仍有片段未完成
    pending_fragment: bool,
    /// 最近一次 `event: close` 的 `auto_resume`：true = 上游建议继续生成
    auto_resume: bool,
    /// 当前 RESPONSE 片段已发出的字符数。
    ///
    /// `-1/content` 是纯增量，直接拼接并累加；`fragments` SET 携带的是**累积全文**，
    /// 按这个长度取差值尾部。旧实现直接把已存在片段整个跳过 —— 快照里比已发长度更长
    /// 的那段就永久丢失，客户端只收到开头几个字、`finish_reason` 却是 `stop`，
    /// 这正是线上遇到的截断。新片段开始时归零，等于「仍处于首片」，无需额外布尔标记。
    emitted_content_chars: usize,
    /// `fragments` SET 回放 **上一片** 响应正文后，内部数组被截断在这里。
    ///
    /// 上游在正文中途回放的是「已完成的旧片段 + 当前片段」的快照，不会重新追加新的
    /// 正文片段。若照单全收，`fragments` 里会多出一条假片段，随后的 `-1/content`
    /// 增量就落在它身上、被当成新片重发（线上表现为正文回退重复）。所以一旦 SET
    /// 产出了正文尾部，就记下长度，把这一轮多出来的尾随片段丢掉。
    replay_tail_start: Option<usize>,
}

impl PatchState {
    fn reset_for_resume(&mut self) {
        self.sticky_path = None;
        self.sticky_op = None;
        self.fragments = Vec::new();
        self.status = None;
        self.accumulated_token_usage = None;
        self.response_has_content = false;
        self.pending_fragment = false;
        self.auto_resume = false;
        self.emitted_content_chars = 0;
        self.replay_tail_start = None;
    }

    const fn new() -> Self {
        Self {
            sticky_path: None,
            sticky_op: None,
            fragments: Vec::new(),
            status: None,
            accumulated_token_usage: None,
            response_has_content: false,
            pending_fragment: false,
            auto_resume: false,
            emitted_content_chars: 0,
            replay_tail_start: None,
        }
    }

    /// 发一段 RESPONSE 全文（`SET` 快照或新片段首片）：只发超出已发长度的尾部。
    ///
    /// `emitted_content_chars == 0` 即首片，`skip(0)` 天然整段发出。
    fn emit_content(&mut self, events: &mut Vec<StreamEvent>, content: &str) {
        if content.is_empty() {
            return;
        }
        let total = content.chars().count();
        if total <= self.emitted_content_chars {
            return;
        }
        let tail: String = content.chars().skip(self.emitted_content_chars).collect();
        self.emitted_content_chars = total;
        self.response_has_content = true;
        events.push(StreamEvent::ContentDelta { content: tail });
    }

    /// 追加一段纯增量（`-1/content` APPEND）：原样拼接并累加长度。
    ///
    /// 累加这一步是必须的：`-1/content` 之后上游可能回放一次 `fragments` SET 全文，
    /// 那时差值才对得上。
    fn push_content_delta(&mut self, events: &mut Vec<StreamEvent>, content: &str) {
        if content.is_empty() {
            return;
        }
        self.emitted_content_chars += content.chars().count();
        self.response_has_content = true;
        events.push(StreamEvent::ContentDelta {
            content: content.to_owned(),
        });
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
        // p/o 跨事件持久化（本帧显式给出的覆盖粘滞值）
        if let Some(p) = val.get("p").and_then(|v| v.as_str()) {
            self.sticky_path = Some(p.to_string());
        }
        if let Some(o) = val.get("o").and_then(|v| v.as_str()) {
            self.sticky_op = Some(o.to_string());
        }

        let Some(v) = val.get_mut("v") else {
            return Vec::new();
        };

        // 无 path 的裸增量：上游在 `fragments/-1/content` 之后会连续下发
        // `{"v":" xxx"}`（省略 p/o，沿用上一条的路径继续追加）。这一帧解析完
        // 就必须清掉粘滞的 p/o，否则下一个只带 `p` 的帧（例如
        // `{"p":"response/status","v":"FINISHED"}`）会继承 `APPEND`，被
        // `apply_path` 的 SET 分支全部漏掉 —— 状态永远停在 WIP，流只能靠 EOF
        // 判成截断，正文随之丢失（2026-11-26 空回复）。
        let path = self.sticky_path.take().unwrap_or_default();
        let op = match self.sticky_op.take() {
            Some(o) => o,
            // 裸数组沿用 fragments 的追加语义（`{"v":[…fragment…]}` 形态）
            None if !v.is_object() && v.is_array() && path.ends_with("fragments") => {
                String::from("APPEND")
            }
            None => String::from("SET"),
        };

        // 初始快照：无 path 且 v 含 response
        if path.is_empty()
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
            // 初始快照里的正文就是「已发的全文」：首片标记与已发长度必须同步，
            // 否则后续 `fragments` SET 回放快照时会把整段正文重复发一遍。
            self.emitted_content_chars = 0;
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
                if !content.is_empty() {
                    if ty == FRAG_THINK {
                        events.push(StreamEvent::ThinkDelta { content });
                    } else if is_response_frag(&ty) {
                        self.emit_content(&mut events, &content);
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
                // 回放快照留下的尾随假片段到此为止：真正的增量一定属于当前片段，
                // 必须先把数组截回快照产出的位置，否则增量会落在假片段上被重发。
                if let Some(n) = self.replay_tail_start.take() {
                    self.fragments.truncate(n);
                }
                if let Some(s) = take_string(val)
                    && let Some(frag) = self.fragments.last()
                {
                    match frag.ty.as_str() {
                        FRAG_THINK => events.push(StreamEvent::ThinkDelta { content: s }),
                        _ if is_response_frag(&frag.ty) => {
                            self.push_content_delta(&mut events, &s);
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
                let mut replayed = false;
                if let Some(arr) = val.as_array_mut() {
                    let mut emitted_tail = false;
                    for (i, item) in arr.iter_mut().enumerate() {
                        let Some(obj) = item.as_object_mut() else {
                            continue;
                        };
                        let Some(ty) = obj.remove("type").and_then(take_string_owned) else {
                            continue;
                        };
                        if i < known {
                            let is_response = is_response_frag(&ty);
                            self.fragments[i].ty = ty;
                            // 快照里已存在的 RESPONSE 片段携带的是**累积全文**：
                            // 与已发长度相比多出来的尾部必须补发，否则上游在正文
                            // 中途回放快照时，那一段就永久丢失（客户端只看到开头几个字，
                            // finish_reason 却是 stop）。旧实现这里直接 `continue`。
                            if is_response {
                                let content = obj
                                    .get_mut("content")
                                    .and_then(take_string)
                                    .unwrap_or_default();
                                let before_emitted = self.emitted_content_chars;
                                self.emit_content(&mut events, &content);
                                if self.emitted_content_chars != before_emitted {
                                    emitted_tail = true;
                                }
                            }
                            continue;
                        }
                        // 回放快照只携带「旧片段 + 当前片段」，后面多出来的片段是上游把
                        // 快照数组补齐的结果，不是新正文；收下会让下一条 `-1/content`
                        // 落到假片段上导致重复输出。
                        if emitted_tail {
                            replayed = true;
                            break;
                        }
                        let content = obj
                            .get_mut("content")
                            .and_then(take_string)
                            .unwrap_or_default();
                        if ty == FRAG_THINK {
                            if !content.is_empty() {
                                events.push(StreamEvent::ThinkDelta { content });
                            }
                        } else if is_response_frag(&ty) {
                            // 新片段的首片：已发长度归零，整段发出
                            self.emitted_content_chars = 0;
                            self.emit_content(&mut events, &content);
                        }
                        self.fragments.push(Fragment { ty });
                    }
                }
                self.replay_tail_start = replayed.then_some(self.fragments.len());
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
                        if is_think {
                            if !content.is_empty() {
                                events.push(StreamEvent::ThinkDelta { content });
                            }
                        } else if is_response {
                            // 新追加的 RESPONSE 片段，`content` 是它的首片全文
                            self.emitted_content_chars = 0;
                            self.emit_content(&mut events, &content);
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

#[derive(Debug, PartialEq, Clone, Copy)]
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
        // 不 pin：drop 阶段 `PinnedDrop` 仍能拿到 `&mut` 字段，
        // 且账号必须先于 `session` 释放（会话清理要读账号上缓存的会话）。
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
        // 上一轮冲刷出来的 outcome：续写结束后是 `Truncated`，命中时下一轮
        // `cycle_flush` 会用「零分片」把整次回答误判成截断
        last_outcome: StreamOutcome,
        // 纯 thinking 静默期的保活心跳（配置为 0 时不创建）
        heartbeat: Option<tokio::time::Interval>,
        // 流结束后由 supervisor 读取的收尾判定
        outcome: StreamOutcome,
        // 截断续写回调（None = 未启用自动续写）
        replier: Option<ResumeReplier>,
        // 已尝试续写次数
        resume_attempts: u32,
        // 正在等待的续写 future。**必须持久保存**：future 自己在第一次 poll 时
        // 登记 waker，之后只剩这个句柄持有续写请求的状态（TCP 连接、前缀缓冲）。
        // 早先的实现在 `Pending` 分支直接 `drop`，续写请求被立刻中断——翘尾表现为
        // 「续写: 计算 PoW 之后什么都没有、上游一次 attempt 也没收到」。
        resume_fut: Option<
            Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<super::resume::ResumedStream, CoreError>,
                        > + Send,
                >,
            >,
        >,
        // 续写次数上限（0 = 不续写）
        resume_cap: u32,
        // 续写时使用空 prompt
        auto_resume_empty_prompt: bool,
        // 本轮是否观察到上游 `event: close` 的 `auto_resume`（续写后重置）
        close_auto_resume: bool,
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
            last_outcome: StreamOutcome::Complete,
            heartbeat,
            // 尚未结束：初始值必须是 Truncated。用 Complete 作初始值会让
            // 「WIP 之后直接断流」在 `classify_outcome` 之前就被当成正常结束
            // （settle_on_finish 只降级 Complete，无法从 Complete 反映空正文）。
            outcome: StreamOutcome::Truncated,
            replier: None,
            resume_attempts: 0,
            resume_fut: None,
            resume_cap: 0,
            auto_resume_empty_prompt: true,
            close_auto_resume: false,
        }
    }

    /// 安装截断续写回调（由 `Chat::v0_chat` 在配置开启时注入）
    pub(crate) fn install_resume(&mut self, replier: ResumeReplier, cap: u32, empty_prompt: bool) {
        self.replier = Some(replier);
        self.resume_cap = cap;
        self.auto_resume_empty_prompt = empty_prompt;
    }
}

/// 正常结束时按「本轮是否真的产出正文」判定结果
///
/// 上游对空回复（例如判为重复提问）照样下发 `status: FINISHED`，状态机因此是完整的；
/// 但没有任何 RESPONSE 分片就说明这一轮没给出答案，此前会被判成 `Complete`，
/// 会话被留在账号上，下一轮复用同一条空消息的 `parent_message_id` 继续拿到空回复。
/// 空回复按截断处理：归还会话、触发续写（若已配置），并把 `finish_reason` 置为 `length`。
fn settle_on_finish(patch_state: &PatchState, outcome: &mut StreamOutcome) {
    if !patch_state.response_has_content && *outcome == StreamOutcome::Complete {
        log::warn!(
            target: "ds_core::accounts",
            "上游正常结束但无 RESPONSE 分片，按截断处理: status={:?}",
            patch_state.status
        );
        // 必须同时清掉 WIP 状态：此后 `classify_outcome` 若再看到 WIP，
        // 会按「仍在生成」返回 Incomplete，续写判定重新变得不确定。
        *outcome = StreamOutcome::Truncated;
    }
}

impl Stream for ResponseStream {
    type Item = Result<StreamEvent, CoreError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

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

        // 正在等续写 future：继续 poll **同一个** future（绝不重建），
        // 这样 waker 与请求状态都留在原位。
        if this.resume_fut.is_some() {
            let mut fut = this.resume_fut.take().expect("resume future 刚判定存在");
            let polled = fut.as_mut().poll(cx);
            // 投影只在读字段期间存在；上面 `this` 已经用完，可直接再整借。
            return finish_resume_poll(&mut self, cx, polled, fut);
        }

        loop {
            if let Some(frame) = take_frame(this.buf) {
                let events = match this.patch_state.apply_frame(frame_str(&frame).as_ref()) {
                    Ok(evts) => evts,
                    // 帧解析失败时仍必须先落收尾：否则 `outcome` 停在初始值，
                    // 续写判定与 `finish_reason` 全部失真。
                    Err(e) => {
                        cycle_flush(this.buf, this.patch_state, this.phase, this.outcome);
                        return Poll::Ready(Some(Err(e)));
                    }
                };

                if events.is_empty() {
                    continue;
                }

                // 过滤：阶段切换信号插入 + 延后排队
                let mut filtered = VecDeque::new();
                finalize_events(this.patch_state, this.phase, events, &mut filtered);

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
                    // 一轮上游流结束：先冲刷残帧（最后一帧可能不带空行），
                    // 但**不能**在这里收尾 —— `cycle_flush` 会把 `phase` 直接置成
                    // `Done`，`start_resume` 的续写分支就又走不到了（这正是
                    // `on_truncation` 长期失效的原因）。收尾/续写的判定统一交给
                    // `start_resume`：它先看残帧里有没有正文，再按 `plan_resume` 决定。
                    // 同时带走本轮的 `event: close` 信号：`PatchState` 会被
                    // `reset_for_resume` 清空，标记必须提到流自身的字段上。
                    let mut residual = flush_frames_keep_open(
                        this.buf,
                        this.patch_state,
                        this.phase,
                        this.outcome,
                    );
                    if this.patch_state.auto_resume {
                        *this.close_auto_resume = true;
                        this.patch_state.reset_for_resume();
                    }
                    if let Some(first) = this.pending.pop_front() {
                        this.pending.extend(residual);
                        return Poll::Ready(Some(Ok(first)));
                    }
                    if let Some(first) = residual.pop_front() {
                        this.pending.extend(residual);
                        return Poll::Ready(Some(Ok(first)));
                    }
                    return start_resume(&mut self, cx);
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

// ── 收尾与续写判定 ───────────────────────────────────────────────────

/// 冲刷残帧但**不**收尾：把 `cycle_flush` 会下发的 Done 先按正文判定回退，
/// 让调用方（`start_resume`）拿到真实的 `status` / `response_has_content` 再决定
/// 「续写」还是「收尾」。
fn flush_frames_keep_open(
    buf: &mut BytesMut,
    patch_state: &mut PatchState,
    phase: &mut Phase,
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
        finalize_events(patch_state, phase, events, &mut flushed);
    }
    if patch_state.status.is_some() {
        *outcome = classify_outcome(patch_state);
    }
    flushed
}

/// 按 `plan_resume` 决定续写或收尾（就地接受已投影字段）。
///
/// 续写成功后直接替换内层流并从新流继续解析；`Poll::Pending` 原样上传
/// （waker 已注册在续写 future 或新的上游流上）。
fn start_resume(
    this: &mut Pin<&mut ResponseStream>,
    cx: &mut Context<'_>,
) -> Poll<Option<Result<StreamEvent, CoreError>>> {
    let projected = this.as_mut().project();
    let plan = super::resume::plan_resume(
        *projected.resume_cap,
        *projected.resume_attempts,
        *projected.outcome,
    );
    // 上游 `event: close` 带 `auto_resume=true` 时，即使本轮判定是 Complete
    // （长时间思考后上游主动断开，状态仍是 FINISHED）也必须再续一轮 ——
    // 这正是「思考很久然后没有下文」的那条路径。
    // 这里只看不改：`replier` 没装（续写被关闭）时下面会走收尾分支，若先把
    // 信号清掉，下一次 `poll_next` 就再也拿不到它了。
    let upstream_wants_more = *projected.close_auto_resume;
    let plan = if upstream_wants_more && *projected.resume_attempts < *projected.resume_cap {
        log::info!(
            target: "ds_core::accounts",
            "auto_resume: 上游 close 要求继续生成 (outcome={:?}), 追加一次续写",
            *projected.outcome
        );
        super::resume::ResumePlan {
            resume: true,
            attempt: *projected.resume_attempts,
        }
    } else {
        plan
    };
    log::debug!(
        target: "ds_core::accounts",
        "auto_resume 判定: cap={}, attempts={}, outcome={:?}, plan.resume={}",
        *projected.resume_cap,
        *projected.resume_attempts,
        *projected.outcome,
        plan.resume
    );
    // 先把 `replier` 取出来：它是唯一「还能不能续写」的判据
    let replier = projected.replier.take();
    let Some(mut replier) = replier.filter(|_| plan.resume) else {
        // 这一轮没走续写，但残帧里可能还有最后一帧正文：补冲一次，
        // 否则「WIP 断流」那一帧的整段回答会被静默丢掉。
        let _residual = flush_frames_keep_open(
            projected.buf,
            projected.patch_state,
            projected.phase,
            projected.outcome,
        );
        let produced_content =
            projected.patch_state.response_has_content || *projected.phase == Phase::Content;
        settle_on_finish(projected.patch_state, projected.outcome);
        projected
            .session
            .cleanup(!projected.patch_state.response_has_content);
        *projected.phase = Phase::Done;
        // 空正文永远按截断上报（`length`）：`finish_reason_after_resume`
        // 只看 outcome，而 FINISHED+空正文在关闭续写时仍是 Complete。
        let finish = if produced_content {
            super::resume::finish_reason_after_resume(*projected.outcome, *projected.resumed_any)
                .to_owned()
        } else {
            "length".to_owned()
        };
        return Poll::Ready(Some(Ok(StreamEvent::Done {
            finish_reason: Some(finish),
            accumulated_token_usage: None,
        })));
    };
    // 空回复不留会话：复用它只会把同一个 parent 反复续成空。
    // 只摘掉**缓存**，会话本身还要继续写，卸载上游由 idle 回收负责。
    if !projected.patch_state.response_has_content {
        projected.session.drop_cache_only();
    }
    let ticket = super::resume::ResumeTicket {
        session_id: projected.session.session_id.clone(),
        parent_message_id: projected.session.message_id,
        prompt: super::resume::resume_prompt(*projected.auto_resume_empty_prompt),
        request_id: String::new(),
    };
    // 上游可能其实已经把内容吐完（续写前的残帧里有 RESPONSE 分片）：
    // 先冲刷残帧拿到真实判定，有正文就直接收尾，不再多打一次上游。
    let residual = flush_frames_keep_open(
        projected.buf,
        projected.patch_state,
        projected.phase,
        projected.outcome,
    );
    if projected.patch_state.response_has_content {
        log::debug!(
            target: "ds_core::accounts",
            "auto_resume: 残帧中已有 RESPONSE 内容，跳过续写 (status={:?})",
            projected.patch_state.status
        );
        projected.session.cleanup(false);
        *projected.phase = Phase::Done;
        let finish =
            super::resume::finish_reason_after_resume(*projected.outcome, false).to_owned();
        projected.pending.extend(residual);
        return Poll::Ready(Some(Ok(StreamEvent::Done {
            finish_reason: Some(finish),
            accumulated_token_usage: projected.patch_state.accumulated_token_usage,
        })));
    }

    log::info!(
        target: "ds_core::accounts",
        "auto_resume: attempt={}/{}, session={}",
        *projected.resume_attempts,
        *projected.resume_cap,
        ticket.session_id
    );
    *projected.resume_attempts += 1;
    // 信号已消费：续写轮次必须重新观察 `event: close`，不能沿用上一轮。
    *projected.close_auto_resume = false;
    // 投影必须在这里自然失效：`finish_resume_poll` 要重新借用 `self`
    let mut fut = replier(ticket);
    let polled = fut.as_mut().poll(cx);
    finish_resume_poll(this, cx, polled, fut)
}

/// 续写 future 的 poll 结果收尾：成功后替换内层流并递归续读，失败则收尾。
///
/// `Pending` 时 future 必须**存回 `resume_fut`**：它持有续写请求的全部状态
/// （连接、已读前缀、waker）。丢掉它等于中止请求——这正是「续写永远 pending」
/// 的根因。下次 `poll_next` 会取出**同一个** future 继续 poll，不会重建。
fn finish_resume_poll(
    this: &mut Pin<&mut ResponseStream>,
    cx: &mut Context<'_>,
    polled: Poll<Result<super::resume::ResumedStream, CoreError>>,
    fut: Pin<
        Box<
            dyn std::future::Future<Output = Result<super::resume::ResumedStream, CoreError>>
                + Send,
        >,
    >,
) -> Poll<Option<Result<StreamEvent, CoreError>>> {
    let mut projected = this.as_mut().project();
    let step = match polled {
        Poll::Ready(Ok(resumed)) => Ok(resumed),
        Poll::Ready(Err(e)) => Err(e),
        Poll::Pending => {
            log::debug!(target: "ds_core::accounts", "auto_resume: 续写请求 pending");
            *projected.resume_fut = Some(fut);
            return Poll::Pending;
        }
    };
    match step {
        Err(e) => {
            log::warn!(target: "ds_core::accounts", "auto_resume failed: {e:?}");
            *projected.phase = Phase::Done;
            projected.session.cleanup(true);
            // 续写本身失败：即便此前有正文，整次回答也不完整
            let finish = super::resume::finish_reason_after_resume(StreamOutcome::Truncated, false)
                .to_owned();
            Poll::Ready(Some(Ok(StreamEvent::Done {
                finish_reason: Some(finish),
                accumulated_token_usage: None,
            })))
        }
        Ok(resumed) => {
            log::info!(
                target: "ds_core::accounts",
                "auto_resume: 续写流已就绪, message_id={}",
                resumed.message_id
            );
            *projected.raw = resumed.raw;
            projected.buf.clear();
            projected.patch_state.reset_for_resume();
            *projected.last_outcome = *projected.outcome;
            *projected.outcome = StreamOutcome::Complete;
            projected.session.message_id = resumed.message_id;
            *projected.phase = Phase::Init;
            *projected.finished = false;
            *projected.cycle_complete = false;
            if let Some(iv) = projected.heartbeat.as_mut() {
                iv.reset();
            }
            *projected.resumed_any = true;
            // 续写已经完成：等待标记必须复位，否则新流的 `poll_next` 会在
            // 入口处被短路成永久 `Pending`。
            // 新流已就位：从新流继续解析（同一个唤醒内完成，不多等一轮）。
            // `this` 在 `finish_resume_poll` 里只被借给投影，返回时已经释放，
            // 这里可以直接再借。
            this.as_mut().poll_next(cx)
        }
    }
}

// ── 截断续写调度 ─────────────────────────────────────────────────────

/// 冲刷缓冲区剩余帧并收尾一轮（含收尾判定与告警）
fn cycle_flush(
    buf: &mut BytesMut,
    patch_state: &mut PatchState,
    phase: &mut Phase,
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
        finalize_events(patch_state, phase, events, &mut flushed);
    }
    // 收尾判定只看 `status` 与「是否已有 RESPONSE 分片」：`WIP` 断流同样必须
    // 落到 Done，否则最后一个事件永远不发，续写也判不出来。
    // 不能靠 `finalize_events` —— 它只认终态 status，WIP 断流要自己补 Done。
    {
        *phase = Phase::Done;
        let finish = patch_state.response_has_content.then(|| "stop".to_string());
        if !patch_state.response_has_content {
            log::warn!(
                target: "ds_core::accounts",
                "一轮上游流已结束但无 RESPONSE 内容: status={:?}，按截断上报 finish_reason=length",
                patch_state.status
            );
        }
        flushed.push_back(StreamEvent::Done {
            finish_reason: finish,
            accumulated_token_usage: patch_state.accumulated_token_usage,
        });
    }
    // status 为空 = 本轮一帧都没收到（续写刚接上就被 flush 的空轮），
    // 此时沿用上一轮的 outcome，不能把整次回答误判成截断。
    if patch_state.status.is_some() {
        *outcome = classify_outcome(patch_state);
    }
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
    patch_state: &mut PatchState,
    phase: &mut Phase,
    events: Vec<StreamEvent>,
    out: &mut VecDeque<StreamEvent>,
) {
    // 本帧新产生的内容也算「有正文」：`finalize_events` 只看事件流，
    // 不能依赖 `PatchState.response_has_content` 已经在别处被写过。
    let has_content = patch_state.response_has_content
        || events
            .iter()
            .any(|e| matches!(e, StreamEvent::ContentDelta { .. }));
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

    // `WIP` 只是「还在写」：纯 thinking 阶段上游就发 WIP，若把它当收尾，
    // 一轮还在跑就会被提前判成 Done，`auto_resume` 永远不来。
    let terminal_status = patch_state
        .status
        .as_deref()
        .filter(|s| matches!(*s, "FINISHED" | "INCOMPLETE"))
        .filter(|_| *phase != Phase::Done)
        .map(str::to_string);
    if let Some(status) = terminal_status {
        *phase = Phase::Done;
        // 空正文（零 RESPONSE 分片）一律按截断收尾：上游对空回复也照样下发
        // FINISHED，若照 status 判成 Complete，会话会留在账号上被复用，下一轮
        // 继续拿到空回复，客户端也看不到「没说完」。
        // INCOMPLETE 是上游的「先这样」结束：同样上报 length，是否续写交给 supervisor。
        let finish = has_content.then(|| "stop".to_string());
        let usage = patch_state.accumulated_token_usage;
        if !has_content && !patch_state.response_has_content {
            log::warn!(
                target: "ds_core::accounts",
                "状态机已结束但无 RESPONSE 内容: status={status}，按截断上报 finish_reason=length"
            );
        }
        out.push_back(StreamEvent::Done {
            finish_reason: finish,
            accumulated_token_usage: usage,
        });
    }
}

/// 判定上游流结束的收尾类型（供上层 supervisor 决定是否自动续写）
///
/// - `status = FINISHED` → `Complete`
/// - `status = INCOMPLETE` → `Incomplete`（用户中断 / 上游暂停）
/// - 其它（`WIP`、无 status、已断流）→ `Truncated`
fn classify_outcome(state: &PatchState) -> StreamOutcome {
    match state.status.as_deref() {
        // 「FINISHED 但零 RESPONSE 分片」= 上游主动截流，绝不能算 Complete：
        // 否则空回复会被 cleanup(true) 归还复用池，后续每轮都挂在同一个空 parent 上。
        Some("FINISHED") if !state.response_has_content => StreamOutcome::Truncated,
        Some("FINISHED") => StreamOutcome::Complete,
        Some("INCOMPLETE") => StreamOutcome::Incomplete,
        // `WIP` 断流同样是半截回答（上游明确标了「还在写」），
        // 与「一帧都没收到」一样落到 Truncated。
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// 回归：上游只镜像一帧 THINK 就带上 `WIP` 断流（真实上游偶发，见
    /// development.md 的「只有 THINK 的短流」）时，Done 必须报 `length` 而不是 `stop`，
    /// 否则客户端把半句思考当成完整回答。
    #[test]
    fn think_only_wip_stream_reports_length_not_stop() {
        let mut state = PatchState::new();
        let frame = r#"data: {"v":{"response":{"status":"WIP","fragments":[{"id":2,"type":"THINK","content":"We"}]}}}"#;
        let mut phase = Phase::Init;
        let mut out = VecDeque::new();
        let events = state.apply_frame(frame).expect("帧应可解析");
        finalize_events(&mut state, &mut phase, events, &mut out);

        // WIP 表示「还在写」：单帧不得收尾，否则一轮在跑就被判成 Done，
        // `auto_resume` 再也等不到续写判定。
        assert!(
            out.iter().all(|e| !matches!(e, StreamEvent::Done { .. })),
            "WIP 不是终态，不得提前下发 Done"
        );
        assert_eq!(phase, Phase::Thinking);
        assert!(matches!(out.back(), Some(StreamEvent::ThinkDelta { .. })));
    }

    /// 回归：`cycle_flush` 在 WIP 断流后补发收尾（零 RESPONSE ⇒ length），
    /// 且不能因为「有 THINK 内容」就漏掉续写判定。
    #[test]
    fn think_only_wip_stream_is_truncated() {
        let mut state = PatchState::new();
        // `take_frame` 要求 `\n\n` 分隔，帧尾空行由缓冲区补齐。
        let mut buf = BytesMut::from(
            concat!(
                r#"data: {"v":{"response":{"status":"WIP","fragments":[{"id":2,"type":"THINK","content":"We"}]}}}"#,
                "\n\n"
            )
            .as_bytes(),
        );
        let mut phase = Phase::Init;
        let mut outcome = StreamOutcome::Complete;
        let flushed = cycle_flush(&mut buf, &mut state, &mut phase, &mut outcome);
        assert_eq!(outcome, StreamOutcome::Truncated);
        assert!(!state.response_has_content);
        // WIP 断流在 `cycle_flush` 里补发收尾：零 RESPONSE ⇒ 由 `start_resume`
        // 统一改写为 `length`，这里只要求补上了一个 Done 占位。
        assert!(matches!(
            flushed.back(),
            Some(StreamEvent::Done {
                finish_reason: None,
                ..
            })
        ));
    }

    /// 回归：上游在 status=FINISHED 之后直接断流时，`accumulated_token_usage` 与
    /// `finish_reason` 仍必须出现在 Done 事件里（issue #93：completion_tokens 恒为 0）
    #[test]
    fn bare_delta_does_not_leak_op_into_following_status_frame() {
        // 真实上游序列：`fragments/-1/content` APPEND → 裸增量 → 新 fragments(无 o)
        // → 裸增量 → BATCH → 只带 p 的 `response/status`。旧实现把贴式的 APPEND 继承到
        // status 帧，`apply_path` 的 SET 分支全部落空，状态永远停在 WIP、正文丢失。
        let mut state = PatchState::new();
        let frames = [
            r#"data: {"v":{"response":{"status":"WIP","fragments":[{"id":2,"type":"THINK","content":"We"}]}}}"#,
            r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":" need"}"#,
            r#"data: {"v":" answer"}"#,
            r#"data: {"p":"response/fragments","v":[{"id":2,"type":"THINK","content":"We need answer"},{"id":3,"type":"RESPONSE","content":"Hi"}]}"#,
            r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":"!"}"#,
            r#"data: {"p":"response/status","v":"FINISHED"}"#,
        ];
        let mut phase = Phase::Init;
        let mut content = String::new();
        let mut thinks = String::new();
        for frame in frames {
            let events = state.apply_frame(frame).expect("帧应可解析");
            let mut out = VecDeque::new();
            finalize_events(&mut state, &mut phase, events, &mut out);
            for evt in out {
                match evt {
                    StreamEvent::ContentDelta { content: c } => content.push_str(&c),
                    // 首个快照已带 "We"，后续 `fragments` 又是完整快照，思考内容会重复；
                    // 本用例只关心 RESPONSE 片段是否被粘滞的 APPEND 吞掉。
                    StreamEvent::ThinkDelta { content: c } => thinks.push_str(&c),
                    _ => {}
                }
            }
        }
        assert!(thinks.contains("We need answer"));
        // `-1/content` 是纯增量：快照首片 "Hi" 之后追加 "!"，正文必须完整
        assert_eq!(content, "Hi!");
        assert!(state.response_has_content);
        assert_eq!(state.status.as_deref(), Some("FINISHED"));
        assert_eq!(classify_outcome(&state), StreamOutcome::Complete);
    }

    /// 回归：真实上游 2026-11 的思考流 —— 初始快照只有 THINK，正文片段靠
    /// `fragments` SET 追加，`-1/content` 增量接在后面。这条序列与线上抓包逐帧一致，
    /// 正文必须完整。若 SET 分支漏掉「已存在片段被新片段接替」的位置，
    /// 后续 `-1/content` 会落到旧的 THINK 上，正文只剩首个分片。
    #[test]
    fn fragments_set_appends_response_after_think() {
        let mut state = PatchState::new();
        let frames = [
            r#"data: {"v":{"response":{"status":"WIP","accumulated_token_usage":0,"fragments":[{"id":2,"type":"THINK","content":""}]}}}"#,
            r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":"We"}"#,
            r#"data: {"v":" need"}"#,
            r#"data: {"v":" answer"}"#,
            r#"data: {"p":"response/fragments/-1/elapsed_secs","o":"SET","v":0.37}"#,
            r#"data: {"p":"response/fragments","v":[{"id":2,"type":"THINK","content":"We need answer","elapsed_secs":0.37}]}"#,
            r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":" there"}"#,
            r#"data: {"p":"response/fragments","v":[{"id":2,"type":"THINK","content":"We need answer there"},{"id":3,"type":"RESPONSE","content":"Hello My"}]}"#,
            r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":" name deepseek"}"#,
            r#"data: {"p":"response/fragments/-1/elapsed_secs","o":"SET","v":0.41}"#,
            r#"data: {"p":"response/status","v":"FINISHED"}"#,
        ];
        let mut phase = Phase::Init;
        let mut content = String::new();
        for frame in frames {
            let events = state.apply_frame(frame).expect("帧应可解析");
            let mut out = VecDeque::new();
            finalize_events(&mut state, &mut phase, events, &mut out);
            for evt in out {
                if let StreamEvent::ContentDelta { content: c } = evt {
                    content.push_str(&c);
                }
            }
        }
        assert_eq!(content, "Hello My name deepseek");
        assert!(state.response_has_content);
        assert_eq!(classify_outcome(&state), StreamOutcome::Complete);
    }

    #[test]
    fn finalize_events_keeps_usage_when_status_finished() {
        let mut state = PatchState::new();
        state.response_has_content = true;
        state.status = Some("FINISHED".to_string());
        state.accumulated_token_usage = Some(87);
        let mut phase = Phase::Content;
        let mut out = VecDeque::new();

        finalize_events(&mut state, &mut phase, Vec::new(), &mut out);

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

    /// FINISHED 但零 RESPONSE 分片：不能报 stop，否则空正文会被当成正常结束
    #[test]
    fn finalize_events_empty_finished_reports_length() {
        let mut state = PatchState::new();
        state.status = Some("FINISHED".to_string());
        let mut phase = Phase::Thinking;
        let mut out = VecDeque::new();

        finalize_events(&mut state, &mut phase, Vec::new(), &mut out);

        match out.make_contiguous() {
            [StreamEvent::Done { finish_reason, .. }] => {
                assert_eq!(*finish_reason, None, "空正文必须让上层补成 length");
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

        finalize_events(&mut state, &mut phase, Vec::new(), &mut out);

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

        finalize_events(&mut state, &mut phase, Vec::new(), &mut out);

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
            &mut state,
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

    /// 回归：唯一正文片段以外还有别的片段（如上游的思考/模板占位）时，
    /// 已知片段被 SET 回放**增长后的全文**，多出来的片段不能收进数组。
    ///
    /// 线上截断正是这一段：`known = 2`、快照 3 条，第 3 条被 push 成假片段，
    /// 下一条 `-1/content` 就落在假片段上重发；旧实现更糟 —— 已存在片段直接
    /// `continue`，快照里长出来的尾部永远丢失。
    #[test]
    fn fragments_set_growth_with_trailing_fragment_truncates_replay() {
        let mut state = PatchState::new();
        // 1) THINK 片段在前
        state.apply_frame(
            r#"data: {"p":"response/fragments","o":"APPEND","v":[{"type":"THINK","content":"Think"}]}"#,
        ).unwrap();
        // 2) 正文片段首片（SET 新增）
        state.apply_frame(
            r#"data: {"p":"response/fragments","o":"SET","v":[{"type":"THINK"},{"type":"RESPONSE","content":"Hello My"}]}"#,
        ).unwrap();
        // 3) 纯增量补到 "Hello My name"
        state
            .apply_frame(r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":" name"}"#)
            .unwrap();
        assert_eq!(state.emitted_content_chars, 13);

        // 4) 快照回放全文（22 字符）+ 一条尾随占位片段
        let events = state.apply_frame(
            r#"data: {"p":"response/fragments","o":"SET","v":[{"type":"THINK"},{"type":"RESPONSE","content":"Hello My name deepseek"},{"type":"RESPONSE","content":""}]}"#,
        ).unwrap();
        assert_eq!(content_text(&events), " deepseek");
        assert_eq!(state.emitted_content_chars, 22);

        // 5) 后续增量必须落在真正的正文片段上，且不重复已发内容
        let events = state
            .apply_frame(r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":"!"}"#)
            .unwrap();
        assert_eq!(content_text(&events), "!");
        assert_eq!(state.fragments.len(), 2);
    }

    /// 回归：真实截断序列 —— 正文靠 `fragments` SET 快照长出来，
    /// `-1/content` 只补纯增量，末尾再现一次全文快照。完整序列由 e2e 覆盖
    /// （`scripts/risk-experiment/mock_upstream.py` 的 growth 形态）。
    #[test]
    fn growth_snapshot_after_delta_emits_only_new_tail() {
        let mut state = PatchState::new();
        let frames = [
            r#"event: ready data: {"request_message_id":1,"response_message_id":2}"#,
            r#"data: {"p":"response/status","o":"SET","v":"WIP"}"#,
            r#"data: {"p":"response/fragments","o":"SET","v":[{"id":2,"type":"RESPONSE","content":"Hello My "}]}"#,
            r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":"name deepseek"}"#,
            r#"data: {"p":"response/fragments","o":"SET","v":[{"id":2,"type":"RESPONSE","content":"Hello My name deepseek"}]}"#,
            r#"data: {"p":"response/status","o":"SET","v":"FINISHED"}"#,
            "event: close data: {}",
        ];
        let mut content = String::new();
        for frame in frames {
            for evt in state.apply_frame(frame).expect("帧应可解析") {
                if let StreamEvent::ContentDelta { content: c } = evt {
                    content.push_str(&c);
                }
            }
        }
        assert_eq!(content, "Hello My name deepseek");
        assert_eq!(classify_outcome(&state), StreamOutcome::Complete);
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

    /// 回归：SET 快照携带**增长后**的全文时，只能发超出已发长度的尾部。
    ///
    /// 上游在正文流里会周期性回放 `response/fragments` SET，其中已存在 RESPONSE
    /// 片段的 `content` 是「到当前为止的全文」。旧实现直接跳过已存在片段，
    /// 于是快照里比已发长度更长的那一段被静默丢弃 → 客户端只收到开头几个字，
    /// finish_reason 却是 `stop`。
    #[test]
    fn fragments_set_beyond_emitted_emits_only_tail() {
        let mut state = PatchState::new();
        // 首片走 APPEND（真实序列里大概率是 `-1/content`），已发 "Hello My" = 8 字符
        state.apply_frame(
            r#"data: {"p":"response/fragments","o":"APPEND","v":[{"type":"RESPONSE","content":"Hello My"}]}"#,
        ).unwrap();
        state
            .apply_frame(r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":" name"}"#)
            .unwrap();
        assert_eq!(state.emitted_content_chars, 13);

        // 快照把全文回放成 "Hello My name deepseek"（22 字符），只应补 " deepseek"
        let events = state.apply_frame(
            r#"data: {"p":"response/fragments","o":"SET","v":[{"type":"RESPONSE","content":"Hello My name deepseek"}]}"#,
        ).unwrap();
        assert_eq!(content_text(&events), " deepseek");
        assert_eq!(state.emitted_content_chars, 22);
    }

    /// `-1/content` 是**纯增量**（上游从不回放全文），必须原样拼接。
    #[test]
    fn content_append_is_pure_delta() {
        let mut state = PatchState::new();
        state
            .apply_frame(
                r#"data: {"p":"response/fragments","o":"SET","v":[{"type":"RESPONSE","content":"Hello My"}]}"#,
            )
            .unwrap();
        let events = state
            .apply_frame(
                r#"data: {"p":"response/fragments/-1/content","o":"APPEND","v":" name deepseek"}"#,
            )
            .unwrap();
        assert_eq!(content_text(&events), " name deepseek");
        assert_eq!(state.emitted_content_chars, 22);
    }

    /// 回归：续写 future 返回 `Pending` 后必须**被保存并继续 poll**。
    ///
    /// 真正的伤害是丢弃句柄：`Pending` 之后 future 已中止，续写请求再也
    /// 等不到结果（表现为「续写永远 pending、上游一次 attempt 也没收到」）。
    /// 这里用一个「第一次 Pending、被 waker 叫醒后返回 Ready」的 future 复现：
    /// 丢弃句柄的实现在唤醒后再也拿不到结果。
    #[tokio::test]
    async fn resume_future_resumes_after_pending() {
        use std::sync::Mutex;
        use std::task::Waker;

        struct PendingThenReady {
            first: Option<Waker>,
            polled: usize,
            fired: Arc<Mutex<bool>>,
        }
        impl std::future::Future for PendingThenReady {
            type Output = Result<super::super::resume::ResumedStream, CoreError>;
            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Self::Output> {
                self.polled += 1;
                // 第一次登记 waker 后立刻把它叫醒：下一次 poll 就能拿到结果。
                self.first
                    .get_or_insert_with(|| cx.waker().clone())
                    .wake_by_ref();
                let ready = *self.fired.lock().unwrap();
                if ready {
                    Poll::Ready(Err(CoreError::Stream("续写测试结束".into())))
                } else {
                    *self.fired.lock().unwrap() = true;
                    Poll::Pending
                }
            }
        }

        let raw = futures::stream::iter([Ok(bytes::Bytes::from_static(b"data: {}\n\n"))]);
        let mut stream = ResponseStream::new(
            Box::pin(raw),
            AccountGuard::busy_for_test(Arc::new(crate::accounts::Account::offline_for_test())),
            dummy_session(),
            "acc".to_owned(),
            0,
        );
        let fired = Arc::new(Mutex::new(false));
        let seen = fired.clone();
        stream.install_resume(
            Box::new(move |_ticket| {
                Box::pin(PendingThenReady {
                    first: None,
                    polled: 0,
                    fired: seen.clone(),
                })
            }),
            2,
            true,
        );

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        // 第一次拿到帧；第二次 EOF 进入续写判定（future Pending 并自唤醒）；
        // 第三次必须重新 poll 同一个 future 并拿到 Ready(Err) —— 走失败收尾。
        let mut done = None;
        for _ in 0..6 {
            if let Poll::Ready(Some(Ok(StreamEvent::Done { finish_reason, .. }))) =
                stream.poll_next_unpin(&mut cx)
            {
                done = finish_reason;
                break;
            }
        }
        assert!(*fired.lock().unwrap(), "续写 future 没有被重新 poll");
        assert_eq!(done.as_deref(), Some("length"), "续写失败必须按截断收尾");
    }

    /// 回归：`event: close` 带 `{"auto_resume":true}` 时，即便本轮已判
    /// `Complete`（长时间思考后上游主动断流，status 仍是 FINISHED），也必须
    /// 再续一轮 —— 这就是「想了很久然后没有下文」的路径。
    #[tokio::test]
    async fn close_auto_resume_forces_resume_on_complete() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let raw = futures::stream::iter([Ok(bytes::Bytes::from_static(
            b"data: {\"p\":\"response/fragments\",\"o\":\"SET\",\"v\":[{\"type\":\"RESPONSE\",\"content\":\"a\"}]}\n\n              data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n\n              event: close\ndata: {\"auto_resume\":true}\n\n",
        ))]);
        let mut stream = ResponseStream::new(
            Box::pin(raw),
            AccountGuard::busy_for_test(Arc::new(crate::accounts::Account::offline_for_test())),
            dummy_session(),
            "acc".to_owned(),
            0,
        );
        stream.install_resume(
            Box::new(move |_ticket| {
                counter.fetch_add(1, Ordering::SeqCst);
                // 永远 Ready，让续写立刻完成，测试无需额外唤醒
                Box::pin(futures::future::ready(Err(CoreError::Stream(
                    "测试用续写失败".to_owned(),
                ))))
            }),
            2,
            true,
        );

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut events = Vec::new();
        for _ in 0..40 {
            match stream.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(Ok(evt))) => events.push(evt),
                Poll::Ready(Some(Err(e))) => panic!("意外的流错误: {e:?}"),
                Poll::Ready(None) => break,
                Poll::Pending => {}
            }
        }

        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "上游 close 声明 auto_resume=true，但本轮已判 Complete 时没有续写"
        );
        let done = events
            .iter()
            .rev()
            .find_map(|e| match e {
                StreamEvent::Done { finish_reason, .. } => Some(finish_reason.clone()),
                _ => None,
            })
            .expect("流必须有 Done 事件");
        assert_eq!(done.as_deref(), Some("length"), "续写失败应报 length");
    }

    fn dummy_session() -> SessionHandle {
        SessionHandle {
            client: crate::accounts::DsClient::offline_for_test(),
            token: String::new(),
            session_id: "s".to_owned(),
            message_id: 0,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            account: None,
            chain: Vec::new(),
            keep_on_finish: false,
        }
    }
}
