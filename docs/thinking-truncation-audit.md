# Issue #122 审计：思考时间过长导致响应截断

> 范围：只读分析当前代码，未修改任何源文件。
> 结论一句话：**本代理的代码里没有任何空闲/读取超时会主动截断 thinking 流**；`auto_resume`、`INCOMPLETE` 等可能被上游用于「继续生成」的信号目前**全部被丢弃**，所以当上游因为长时间 thinking 主动关闭 SSE 时，我们把它当成正常结束 → 下游拿到一份截断的回答。

---

## 1. 现状：timeout 与流生命周期

### 1.1 `ds_core/src` 中唯一的 timeout/idle 全是**非流式**用途

`grep -rn "timeout\|Duration::from_secs\|IDLE\|idle" ds_core/src` 的结果与流无关，逐条说明：

| 位置 | 用途 | 是否影响 thinking 流 |
|------|------|----------------------|
| `accounts/pool.rs:655` `get_account_with_wait` | 拿账号时最多等 N ms（`request.rs` 里传 `30_000`） | 否，只在**开流之前**阻塞 |
| `accounts/pool.rs:884-886` `reap_sessions(session_idle_secs)` | 后台定期回收**闲置的复用会话**（默认 `session_idle_secs = 900`） | 否，回收的是没在用的会话；`AccountGuard` 持有期间会话仍忙碌 |
| `accounts/hif.rs:40` `FETCH_TIMEOUT = 3s` | 取 `x-hif-leim` 风控令牌 | 否，只在发请求前取 token |
| `accounts/hif.rs` `INITIAL_FAILURE_BACKOFF / REFRESH_MARGIN` | 风控令牌失败退避 | 否 |

**关键：`ds_core/src/chat/` 下没有任何 `tokio::time::timeout` / `Duration` / read-deadline。**
流处理是纯 `poll_next` 透传，上游不吐字节就一直 `Poll::Pending`，**永远不会因为「思考太久」被本地超时掐断**。

```rust
// ds_core/src/chat/response.rs:559 —— 上游沉默时只是挂起，没有 deadline
Poll::Pending => return Poll::Pending,
```

### 1.2 上游 HTTP 客户端没有 read timeout

`ds_core/src/accounts/client.rs:764 completion()` 只用 `resp.bytes_stream()` 包一层 `map_err`，
`wreq::Client` 构造里没有设置 `.timeout()` / `read_timeout`。
即：真正的空闲上限由 **上游（Cloudflare/Tengine/DeepSeek 网关）** 决定，不由我们决定。

### 1.3 `ResponseStream` 的 EOF 处理：把「上游断流」当成「正常结束」

`ds_core/src/chat/response.rs:519-557`（`poll_next` 的 `Poll::Ready(None)` 分支）：

1. `*this.finished = true;` —— 标记「正常结束」
2. 冲刷残余 buffer 里的最后几帧
3. 若 `phase != Done` → 补发一个 **空 `Done { finish_reason: None }`**（549-556 行）
4. 返回 `Poll::Ready(None)` 真正结束

问题点：**EOF 只有一条路径**。无论上游是：
- 正常 `status=FINISHED` 后断流，
- 还是 thinking 太久上游 `close` 了、或 TCP 被中间层断开，

代码都会走同一套「冲刷 + 补 Done + 结束」。
`finished = true` 还会让 `SessionHandle::cleanup(true)` **保留会话**（reuse 模式，第 78 行 `keep = keep_on_finish && finished …`）
并且**不调用 `stop_stream`**，上游侧可能仍在后台继续或已被判定为异常结束。

### 1.4 `finalize_events`：只认 `FINISHED` / `INCOMPLETE`

`ds_core/src/chat/response.rs:623-640`：

```rust
if let Some(status) = &patch_state.status
    && (status == "FINISHED" || status == "INCOMPLETE")
    && *phase != Phase::Done
{
    let finish = (status == "FINISHED").then(|| "stop".to_string());
    out.push_back(StreamEvent::Done { finish_reason: finish, accumulated_token_usage: usage });
}
```

- `FINISHED` → `finish_reason = Some("stop")`，下游看到 `finish_reason: "stop"` → 认为回答**完整**。
- `INCOMPLETE` → `finish_reason = None`，下游只是「没有 finish_reason」，**不会**触发任何 continue。
- 其它任何状态（`WIP` 结束、`status` 未变、没有 `response/status` 帧）→ 直接 EOF 补空 `Done`。

**没有任何「检测到尚未收尾」的分支。**

### 1.5 `apply_frame` / `event: hint` / `event: close` / `wait_close`

- `apply_frame`（160-194 行）：只处理 `event: hint`（转 `CoreError`）和 `data:` 的 JSON patch。
  `event: ready` 静默忽略；**`event: close` 的 data（含 `auto_resume`）完全不解析**。
- `hint` → `hint_to_error`（577-592 行）：只认 `rate_limit` / `input_exceeds_limit`，其它一律 `ProviderError`。
  **思考超时/继续生成的提示类 hint 会被当成 ProviderError 抛出或忽略。**
- `wait_close`（782-819 行）：**只用于分块 expert 路径**（`v0_chat_oversized_chunk`）。
  正常路径 `v0_chat_once` 根本不读 `event: close`，EOF 即结束。
  它自己的语义也是「等 `event: close`，拿不到就报错」，不区分 close 的 `auto_resume`。

### 1.6 Adapter / HTTP 层：有 keep-alive，没有截断性 timeout

- `src/server.rs:97`：`axum::serve(listener, router).with_graceful_shutdown(...)`，
  **没有** `.layer(TimeoutLayer)`、没有 hyper header read timeout、没有 tower timeout。
- `src/server/stream.rs`：`SseBody` 只设 `Content-Type: text/event-stream` / `Cache-Control` / `Connection: keep-alive` 头，
  把 stream 直接桥接成 `Body::from_stream`，**不注入任何超时**。
- Adapter 侧只有一个 **keep-alive 心跳**（防下游/代理因「长时间无字节」断开）：
  - `src/openai_adapter/response.rs:51` `KEEPALIVE_INTERVAL = 1s`（`RepairStream` 修复期间）
  - `src/openai_adapter/response/tool_parser.rs:32` `KEEPALIVE_INTERVAL = 1s`（工具检测阶段）
  - 这些心跳**只增不减**，不会结束流。
- 注意：**纯 thinking 阶段（THINK fragment 持续 APPEND、尚未进入工具检测/修复）没有心跳**。
  `ThinkDelta` 走 `converter.rs:116` 直接转 `reasoning_content`。只要上游持续吐 THINK 帧，
  每帧都会向下游写字节，链路是活的——**但一旦上游停止吐帧（长思考静默），
  下游（客户端 ↔ 本代理之间的 nginx/CDN/负载均衡）可能因「无字节」先断**，而不是我们主动断。

### 1.7 Config 里没有可调的流 timeout

`grep "timeout\|secs\|keep\|idle" src/config.rs ds_core/src/config.rs config.example.toml` 全部命中与流无关：
`session_idle_secs`（会话回收）、`responses_store_ttl_secs`（Responses 存储）、`hif` 内部常量。
**不存在 `stream_idle_timeout` / `sse_read_timeout` 之类的配置项。**

---

## 2. 上游可能造成截断的机制（结合 raw-api-reference）

`ds_core/raw-api-reference.md` 的 SSE 协议部分给出关键证据：

### 2.1 结束信号只有 `response/status`（第 581 行）

> **最可靠的结束信号是 `response/status` 变为 `FINISHED` 或 `INCOMPLETE`。**

- `INCOMPLETE` 的出处（575-579 行）被标注为「手动中断」示例 —— 即用户/客户端主动 stop。
- 但 `INCOMPLETE` 在语义上**完全可能是**「上游因超时/暂停而收尾」，参考协议里没有把它和「用户中断」区分开。

### 2.2 `event: close` 携带继续生成信号（第 571-572 行）

```
event: close
data: {"click_behavior":"none","auto_resume":false}
```

`auto_resume` 就是一个**明确的「是否自动续写」标志**。网页端在长思考/长输出被暂停时，
会据此决定是否显示「继续生成」按钮。**当前代码完全没读这个字段。**

### 2.3 `fragment.status` 的 `WIP` → `FINISHED`（第 476 行）

每个 fragment 也有 `status: "WIP" | "FINISHED"`。若 `response/status` 最后停在 `WIP`，
或某个 `THINK` fragment 仍为 `WIP` 却断了流，就是「思考没结束」的强信号。
当前 `PatchState` **从不读 fragment 级 status**（`response.rs` 只处理 content 和顶层 status）。

### 2.4 `has_pending_fragment`（第 479 行）

> `response/has_pending_fragment`：后台有 fragment 处理中时为 true。

这也是一个现成的「还有东西没吐完」信号，当前代码**未消费**。

### 2.5 暂停再续写的真实入口

reference 里没有单独列一个 `/continue` 端点，但：
- `parent_message_id` 是官方「串联上一条回答」的机制（第 249-252、376-377 行）；
- 网页端「继续生成」本质上是**发一条新的 `completion`，`parent_message_id = 被截断回答的 response_message_id`**，
  让服务端在会话里接着生成。
- `edit_message`（第 585-604 行）也能对某条消息重新生成，但不是「继续」，会覆盖。

即：**「继续」= 复用同一 `chat_session_id` + `parent_message_id` 指向被截断的响应 + 发空/续写 prompt**。
代码里 `CompletionPayload::parent_message_id` 字段已经存在（`client.rs:255`），
`session_policy = reuse` 路径也已经会带 `parent_message_id`（`request.rs:721`），
**基础设施齐全，只是没有「检测到截断 → 自动续写」的逻辑。**

---

## 3. 代码里的具体薄弱点（按严重度）

| # | 位置 | 问题 |
|---|------|------|
| 1 | `response.rs:519-557` | EOF 一律当正常结束：`finished=true`、补空 `Done`、不 `stop_stream`、保留会话。上游因长思考 close 也走这条路。 |
| 2 | `response.rs:623-640` | `finalize_events` 只认 `FINISHED`/`INCOMPLETE`；`WIP` 结束、`has_pending_fragment`、fragment 级 `WIP` 全部忽略。 |
| 3 | `response.rs:180-184` | `event: hint` 只区分限流/超长，其它 hint（可能是「思考超时已暂停」）被当作 `ProviderError` 或忽略。 |
| 4 | `response.rs` 全文 | `event: close` 的 `auto_resume` / `click_behavior` 从未解析。`wait_close` 仅在分块路径使用。 |
| 5 | `request.rs:579` `v0_chat_once` | 正常路径不等 `close`，EOF 即终。没有任何「思考中且未收尾」的判定。 |
| 6 | 缺监控 | 没有对「`ThinkDelta` 已出现但从未出现 `ContentDelta` 就 `Done`」的检测 —— 这正是「全被思考吃掉、回答为空/极短」的典型特征。 |
| 7 | 缺配置 | 没有 idle 超时配置，也没有「长思考自动续写」开关。 |

> 关于「web UI 弹 continue 按钮」的传闻：本仓库没有前端 chat UI，`web/` 是管理面板。
> 该按钮属于 DeepSeek 官网，说明**上游确实有暂停/续写语义**，而代理没有对应处理。

---

## 4. 缓解方案

> 以下均为**建议**，未改动源文件。三套可叠加。

### 方案 A：为 THINK 阶段放宽 / 取消 idle 约束（治标，最安全）

**核心**：我们没有流级 idle timeout，所以要防的是**下游链路**（客户端 ↔ 代理之间的反向代理/网关）因无字节断开。
在纯 thinking 静默期下发 SSE 注释心跳。

思路（伪代码，插入到 `ResponseStream::poll_next` 的 `Poll::Pending` 分支）：

```rust
// 建议：仅在 Phase::Thinking 下、raw 返回 Pending 时，每 N 秒吐一个注释心跳
// 心跳内容用 SSE 注释行 ": ping\n\n"，OpenAI/Anthropic 客户端都会忽略
Poll::Pending => {
    if *this.phase == Phase::Thinking {
        if this.heartbeat.as_mut().poll(cx).is_ready() {
            this.heartbeat.as_mut().reset(tokio::time::Instant::now() + HEARTBEAT);
            // 新增 StreamEvent::Ping，由 converter 序列化为 ": ping\n\n"（而非 data: 帧）
            return Poll::Ready(Some(Ok(StreamEvent::Ping)));
        }
    }
    Poll::Pending
}
```

- 优点：改动小，纯「保活」，不改变语义；能解决「中间层先断」这一类截断。
- 缺点：治不了「上游自己因超时暂停并 close」的情况；心跳占用连接但不下发内容。
- 配套：确认部署层（nginx `proxy_read_timeout`、CDN idle timeout）足够大，并文档化。

### 方案 B：检测「暂停 / 未收尾」并自动续写（治本，推荐主方向）

**检测点**（在 `ResponseStream` EOF 分支，`response.rs:519` 附近）：

```rust
// 判定「疑似被截断」：
//   - 已有 THINK 内容，但从未出现 RESPONSE 内容（response_has_content == false），且
//   - 顶层 status 不是 FINISHED（是 INCOMPLETE / WIP / 缺失），或
//   - 解析到的 event: close 中 auto_resume == true
let truncated = !patch_state.response_has_content
    && patch_state.status.as_deref() != Some("FINISHED");
```

**自动续写**（新请求，复用会话）：

```rust
// 条件：session_reuse 且 parent_message_id 已知且 truncated
// 上游「继续生成」= 同一 chat_session_id + parent_message_id 指向被截断的响应
let payload = CompletionPayload {
    chat_session_id: session_id.clone(),
    parent_message_id: Some(last_response_message_id), // 被截断回答的 id
    model_type, prompt: String::new(),                 // 空 prompt，让服务端续写
    ref_file_ids: vec![],
    thinking_enabled: false,   // 续写一般不再思考
    search_enabled: false,
    action: None,
    source: COMPLETION_SOURCE_INPUT,
    preempt: false,
};
// 需要重新算 PoW；用一个「续写预算」计数器防死循环（例如最多 2 次）
```

- 优点：真正解决「思考超时 → 截断」；对下游透明；基础字段（`parent_message_id`）已就绪。
- 缺点：`v0_chat_once` 目前是「取账号 → 建/复用会话 → 开流」的线性流程，
  `ResponseStream` 手里没有「重发请求」的能力，需要在 `Chat` 层包一层 supervisor：
  在 `ResponseStream` 之外再包一个 `ResumeStream`，当内层 EOF 且判定 truncated 时，
  重新调用 `self.accounts.completion(...)` 并接着 `chain` 到下游流后面。
- 风险：空 prompt 续写在真实上游**尚未验证**（reference 没明确写「空 prompt = 续写」）。
  需先用真实账号 A/B 验证：复用会话 + `parent_message_id` + 空/极短 prompt 是否能续接。
  若不行，退化为「发一条 `"continue"` 文本」也需验证是否污染对话历史。
- 防呆：必须设续写次数上限 + 总时长上限，避免「一直 pause 一直续」的风控/资源问题。

### 方案 C：把这些行为做成可配置（与 A/B 配套）

在 `[server]` 或新的 `[ds_core]` 下增加（示例字段名）：

```toml
# 纯 thinking 静默期是否下发 SSE 注释心跳
thinking_heartbeat_secs = 15        # 0 = 关闭

# 检测到「只思考没回答」/ auto_resume=true 时是否自动续写
auto_resume_on_truncation = false   # 默认关，验证通过后再开
auto_resume_max_attempts = 2
auto_resume_empty_prompt = true     # true=空 prompt 续写；false=发 "continue"
```

- 优点：默认行为不变（不引入新风险），可按部署逐步启用；便于 A/B 观测上游真实行为。
- 缺点：要同步 `config.example.toml` + `docker/config.example.toml` + 前端 `web/src`
  （有 `check-config-drift.sh` 和 `check-config-parity.mjs` 两道门禁）。
- 注意：`INCOMPLETE` 现在映射成 `finish_reason: None`，若启用自动续写，
  应在续写成功取出内容后，把最终 `finish_reason` 修正为 `Some("stop")`，否则下游会以为没结束。

---

## 5. 结论与建议顺序

1. **先做 A（thinking 心跳）**：零语义风险，覆盖「链路无字节被断」这一类；
   同时把部署层 `proxy_read_timeout` 调大并写进 `docs/development.md`。
2. **再做 B 的「检测」部分**：至少把 `truncated` 判定写日志
   （`THINK 有内容但 RESPONSE 为空 + status != FINISHED`），先只观测、不改行为，
   用真实账号收集证据（是否真的会出现 `auto_resume=true` / `INCOMPLETE`）。
3. **观测确认后**，用 C 开关启用 B 的「自动续写」，并用真实账号验证
   「同一 session + parent_message_id + 空 prompt」确实能续接。
4. 全程：`event: close` 的 `auto_resume`、`has_pending_fragment`、fragment 级 `WIP`
   这三个现成信号都应纳入 `PatchState`，作为截断判定的依据，而不是丢弃。

> 一句话给 issue：**这不是我们设了什么 3 分钟超时，而是上游长思考后会主动/被动结束 SSE，
> 而我们把它当 FINISHED 收尾了 —— `auto_resume` 和 `INCOMPLETE` 都被忽略。**
