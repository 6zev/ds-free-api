//! 截断自动续写 —— 续写决策与票据
//!
//! 上游在长时间思考时可能直接关闭 SSE（`event: close` 带 `auto_resume`），
//! 或下发 `response/status = INCOMPLETE` 后断流；旧实现把它当成正常结束
//! （空 `Done`），用户只看到「思考完没答案」。
//!
//! 续写的实际重发由 [`ResponseStream`](super::response::ResponseStream) 通过
//! 注入的 [`ResumeReplier`] 完成（`Chat` 持有账号池，`ResponseStream` 不持有），
//! 本模块只负责：判定是否续写、收尾类型映射、票据结构。

use std::future::Future;
use std::pin::Pin;

use futures::Stream;

use crate::CoreError;

use super::response::StreamOutcome;

/// 续写票据：沿用哪个会话、接到哪条消息后面
#[derive(Debug, Clone)]
pub(crate) struct ResumeTicket {
    pub session_id: String,
    pub parent_message_id: i64,
    /// 续写时原请求的 prompt（仅用于校验，实际发送走 resume_prompt）
    pub prompt: String,
    /// 请求追踪 id（日志关联用）
    pub request_id: String,
}

/// 一次续写请求的结果：新的原始 SSE 流 + 该流对应的消息 id
pub(crate) struct ResumedStream {
    pub raw: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, crate::accounts::ClientError>> + Send>>,
    pub message_id: i64,
}

/// 续写回调：由 `Chat` 提供，内部重新走一次 completion
pub(crate) type ResumeReplier = Box<
    dyn FnMut(
            ResumeTicket,
        ) -> Pin<Box<dyn Future<Output = Result<ResumedStream, CoreError>> + Send>>
        + Send,
>;

/// 续写决策（纯函数，便于单测）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResumePlan {
    /// 是否再发一次 completion
    pub resume: bool,
    /// 已尝试续写次数
    pub attempt: u32,
}

/// 判断本次流结束后是否应当续写
///
/// - `max_attempts = 0` → 永不续写
/// - `attempts >= max_attempts` → 到达上限，停止（防止无限循环）
/// - `outcome = Complete` → 正常结束，不续写
/// - 其余情况（`Incomplete` / `Truncated`）→ 续写
#[must_use]
pub(crate) const fn plan_resume(
    max_attempts: u32,
    attempts: u32,
    outcome: StreamOutcome,
) -> ResumePlan {
    let resume =
        max_attempts > 0 && attempts < max_attempts && !matches!(outcome, StreamOutcome::Complete);
    ResumePlan {
        resume,
        attempt: attempts,
    }
}

/// 上层在最终 `Done` 事件里使用的结束原因
///
/// 续写成功后整体就是一次完整回答，应修正成 `stop`；达到上限仍未补全时
/// 用 `length`（OpenAI 语义：输出被截断），而不是把截断伪装成正常结束。
#[must_use]
pub(crate) const fn finish_reason_after_resume(
    outcome: StreamOutcome,
    resumed_successfully: bool,
) -> &'static str {
    if resumed_successfully || matches!(outcome, StreamOutcome::Complete) {
        "stop"
    } else {
        "length"
    }
}

/// 从 `StreamOutcome` 生成续写票据的 prompt 部分
#[must_use]
pub(crate) fn resume_prompt(empty_prompt: bool) -> String {
    if empty_prompt {
        String::new()
    } else {
        "continue".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_max_attempts_never_resumes() {
        let plan = plan_resume(0, 0, StreamOutcome::Truncated);
        assert!(!plan.resume);
    }

    #[test]
    fn complete_does_not_resume() {
        let plan = plan_resume(3, 0, StreamOutcome::Complete);
        assert!(!plan.resume);
    }

    #[test]
    fn truncated_and_incomplete_resume_until_cap() {
        assert!(plan_resume(2, 0, StreamOutcome::Truncated).resume);
        assert!(plan_resume(2, 1, StreamOutcome::Incomplete).resume);
        assert!(!plan_resume(2, 2, StreamOutcome::Truncated).resume);
    }

    #[test]
    fn finish_reason_reflects_resume_result() {
        assert_eq!(
            finish_reason_after_resume(StreamOutcome::Truncated, true),
            "stop"
        );
        assert_eq!(
            finish_reason_after_resume(StreamOutcome::Truncated, false),
            "length"
        );
        assert_eq!(
            finish_reason_after_resume(StreamOutcome::Complete, false),
            "stop"
        );
    }

    #[test]
    fn resume_prompt_honours_empty_flag() {
        assert_eq!(resume_prompt(true), "");
        assert_eq!(resume_prompt(false), "continue");
    }
}
