//! OpenAI 模型列表响应生成
//!
//! 基于 DeepSeek `model_types` + `model_aliases` 静态生成 OpenAI `/models` 响应。
//!
//! # 为什么每个条目都带完整元数据
//!
//! 客户端（VS Code 插件、Cherry Studio、Cline 一类）普遍**只按 ID 查自己的内置
//! 模型库**，库里没有 `deepseek-default` 这种代理私有 ID，于是拿到一份「能力全部
//! 未知」的模型：上下文窗口退回 128k/64k 默认、视觉标成不支持、thinking 直接消失，
//! 即便 `/v1/models` 明明写了 `max_input_tokens = 1048576`。
//!
//! 这些客户端不会读 `max_input_tokens`/`context_window` 之类的数字字段 —— 它们
//! 最多读 `name`（显示名）和 `capabilities`（能力开关）。所以这里的策略是
//! **两条同时给**：既输出数字字段（给 OpenAI 官方 SDK 一类会读的客户端），也输出
//! `name` 与 `capabilities`（VS Code `vscode-unify-chat-provider` 一类会读的客户端），
//! 让同一个条目在任何客户端都自带身份，而不依赖对方的内置库。
//!
//! # 单一事实来源
//!
//! `list()` 与 `get()` 必须对外暴露**完全一致**的模型 ID 集合，否则会出现
//! 「列表里有、按 ID 查询却拿不到」或「查询返回的 id 与列表里的拼写不同」。
//!
//! 早期实现把「生成 ID」的逻辑在两处各写一遍，已经产生过真实缺陷：
//! 别名查询返回的是**小写化后的** ID（`make_model(&target, ..)`），
//! 而列表返回的是别名的**原始大小写**，同一个模型因此有两个 ID。
//!
//! 现在由一个函数 [`model_ids`] 统一生成有序 ID 列表，`list()` 与 `get()` 共用它。

use crate::openai_adapter::types::{
    OpenAIModel, OpenAIModelCapabilities, OpenAIModelInputCapabilities, OpenAIModelList,
    OpenAIModelOutputCapabilities,
};

const MODEL_CREATED: u64 = 1_090_108_800;
const MODEL_OWNED_BY: &str = "deepseek-web (proxied by https://github.com/NIyueeE)";

/// 枚举全部可用的模型 ID（按展示顺序），值为 `model_types` 中的下标
///
/// 每个 `model_type` 只产生两种写法 —— 这是**唯一**可对外承诺的模型集合，
/// 不再重复堆叠 `deepseek-deepseek-x` 之类的派生别名：
/// - `deepseek-{ty}`（标准 OpenAI `provider-model` 形式）
/// - `{ty}`（裸 model_type 名；Claude Code / Codex 依赖此写法，见 issue #99）
/// - `model_aliases[i]`（用户自定义别名，可选，管理员显式配置才出现）
///
/// 默认配置只有一个 `model_type`（`default`），于是 `/v1/models` 恰好两条：
/// 生成 `list()` 对外展示的有序 ID 列表。
///
/// 只包含**规范 ID**（`deepseek-{ty}`）与**用户别名**，不再列出裸 `{ty}`：
/// 列表里 `default` 与 `deepseek-default` 同时出现会让用户以为有两个模型。
/// 裸名仍可在请求里使用（Claude Code / Codex 依赖它，issue #99），但那是
/// 解析层（`Config::model_registry`）的职责，不应污染 `/v1/models` 的展示。
///
/// `deepseek-{ty}` 与别名在忽略大小写后可能相同，这里做去重，保证列表内 ID 唯一。
#[must_use]
pub fn model_ids(model_types: &[String], aliases: &[String]) -> Vec<(String, usize)> {
    let mut ids: Vec<(String, usize)> = Vec::with_capacity(model_types.len() + aliases.len());
    let mut seen: Vec<String> = Vec::with_capacity(ids.capacity());

    let push = |id: String, idx: usize, ids: &mut Vec<(String, usize)>, seen: &mut Vec<String>| {
        let key = id.to_lowercase();
        if !seen.contains(&key) {
            seen.push(key);
            ids.push((id, idx));
        }
    };

    // 1) 标准 ID：deepseek-<type>
    for (idx, ty) in model_types.iter().enumerate() {
        push(format!("deepseek-{ty}"), idx, &mut ids, &mut seen);
    }

    // 2) 用户别名（按 index 对齐 model_types）
    for (idx, alias) in aliases.iter().enumerate() {
        if model_types
            .get(idx)
            .is_some_and(|_| !alias.trim().is_empty())
        {
            push(alias.clone(), idx, &mut ids, &mut seen);
        }
    }

    ids
}

/// 根据 `model_types` + `aliases` 生成模型列表
pub fn list(
    model_types: &[String],
    max_input_tokens: &[u32],
    max_output_tokens: &[u32],
    aliases: &[String],
) -> OpenAIModelList {
    let data = model_ids(model_types, aliases)
        .into_iter()
        .map(|(id, idx)| {
            make_model(
                &id,
                model_type_at(model_types, idx),
                max_input_tokens.get(idx).copied(),
                max_output_tokens.get(idx).copied(),
            )
        })
        .collect();

    OpenAIModelList {
        object: "list",
        data,
    }
}

/// 查询单个模型（ID 匹配忽略大小写，返回值使用列表中的规范拼写）
///
/// 除列表里的规范 ID / 别名外，也接受**裸 model_type 名**（`default`）：
/// 它是 issue #99 依赖的兼容写法，但返回值的 `id` 仍是规范拼写
/// （`deepseek-default`），与 `/v1/models` 列表保持一致。
pub fn get(
    model_types: &[String],
    max_input_tokens: &[u32],
    max_output_tokens: &[u32],
    aliases: &[String],
    id: &str,
) -> Option<OpenAIModel> {
    let target = id.trim().to_lowercase();

    // 先按列表（规范 ID + 别名）精确匹配，保证与 list() 不漂移
    if let Some((canonical, idx)) = model_ids(model_types, aliases)
        .into_iter()
        .find(|(candidate, _)| candidate.to_lowercase() == target)
    {
        return Some(make_model(
            &canonical,
            model_type_at(model_types, idx),
            max_input_tokens.get(idx).copied(),
            max_output_tokens.get(idx).copied(),
        ));
    }

    // 回退：裸 model_type 名（`default`）→ 规范拼写 `deepseek-{ty}`
    let idx = model_types
        .iter()
        .position(|ty| ty.to_lowercase() == target)?;
    let ty = &model_types[idx];
    Some(make_model(
        &format!("deepseek-{ty}"),
        ty,
        max_input_tokens.get(idx).copied(),
        max_output_tokens.get(idx).copied(),
    ))
}

/// 取某个下标对应的 `model_type`（下标越界返回空串，只影响显示名与能力推断）
fn model_type_at(model_types: &[String], idx: usize) -> &str {
    model_types.get(idx).map_or("", String::as_str)
}

/// 组装单个模型条目
///
/// `model_type` 决定推导出来的能力（`vision` 类型才给 image/pdf 输入）与显示名，
/// 因此同一个 `deepseek-{ty}` 与它的别名会得到**完全相同**的元数据。
fn make_model(id: &str, model_type: &str, input: Option<u32>, output: Option<u32>) -> OpenAIModel {
    let vision = model_type.eq_ignore_ascii_case("vision");
    OpenAIModel {
        id: id.to_string(),
        object: "model",
        created: MODEL_CREATED,
        owned_by: MODEL_OWNED_BY,
        name: Some(display_name(model_type)),
        capabilities: OpenAIModelCapabilities {
            input: OpenAIModelInputCapabilities {
                image_input: vision,
                pdf_input: vision,
            },
            output: OpenAIModelOutputCapabilities {
                tool_calling: true,
                structured_outputs: true,
                thinking: true,
            },
        },
        vision,
        reasoning: true,
        model_type: model_type.to_string(),
        max_input_tokens: input,
        max_output_tokens: output,
        context_length: input,
        context_window: input,
        max_context_length: input,
        max_tokens: output,
        max_completion_tokens: output,
    }
}

/// `default` → `DeepSeek Default`，`vision` → `DeepSeek Vision`
fn display_name(model_type: &str) -> String {
    if model_type.is_empty() {
        return "DeepSeek".to_string();
    }
    let mut head = String::new();
    let mut chars = model_type.chars();
    if let Some(first) = chars.next() {
        head.extend(first.to_uppercase());
        head.push_str(chars.as_str());
    }
    format!("DeepSeek {head}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn types() -> Vec<String> {
        vec!["default".to_string(), "expert".to_string()]
    }
    fn limits() -> (Vec<u32>, Vec<u32>) {
        (vec![100, 200], vec![10, 20])
    }

    #[test]
    fn list_hides_bare_type_names() {
        let (mi, mo) = limits();
        let list = list(&types(), &mi, &mo, &[]);
        let ids: Vec<&str> = list.data.iter().map(|m| m.id.as_str()).collect();
        assert!(ids.contains(&"deepseek-default"));
        assert!(ids.contains(&"deepseek-expert"));
        assert!(
            !ids.contains(&"default"),
            "裸 model_type 名不应出现在 /v1/models 列表里：{ids:?}"
        );
        assert!(
            !ids.contains(&"expert"),
            "裸 model_type 名不应列出：{ids:?}"
        );
    }

    #[test]
    fn get_accepts_prefixed_and_bare_names() {
        let (mi, mo) = limits();
        for id in ["deepseek-default", "DEFAULT", "default", "deepseek-expert"] {
            assert!(get(&types(), &mi, &mo, &[], id).is_some(), "{id} 应可查询");
        }
        // 裸名查询返回规范拼写
        assert_eq!(
            get(&types(), &mi, &mo, &[], "default").unwrap().id,
            "deepseek-default"
        );
        assert!(get(&types(), &mi, &mo, &[], "nope").is_none());
    }

    #[test]
    fn get_accepts_aliases() {
        let (mi, mo) = limits();
        let aliases = vec!["gpt-4o".to_string()];
        let model = get(&types(), &mi, &mo, &aliases, "gpt-4o").expect("alias must resolve");
        assert_eq!(model.max_input_tokens, Some(100));
    }

    #[test]
    fn list_does_not_duplicate_alias_equal_to_type_name() {
        let (mi, mo) = limits();
        let aliases = vec!["default".to_string()];
        let list = list(&types(), &mi, &mo, &aliases);
        let count = list
            .data
            .iter()
            .filter(|m| m.id.eq_ignore_ascii_case("default"))
            .count();
        // "default"（别名或裸名）与 "deepseek-default" 只应各出现一次
        assert_eq!(count, 1, "别名与 model_type 同名时不应重复列出");
    }

    /// 回归：`get()` 返回的 ID 必须与 `list()` 中的拼写**逐字节相同**
    ///
    /// 旧实现对别名返回小写化后的 ID，导致同一模型在列表与查询结果里有两个 ID。
    #[test]
    fn get_returns_same_spelling_as_list_for_every_listed_id() {
        let (mi, mo) = limits();
        let aliases = vec!["MyModel".to_string(), String::new()];
        let listed = list(&types(), &mi, &mo, &aliases);

        for item in &listed.data {
            let fetched = get(&types(), &mi, &mo, &aliases, &item.id)
                .unwrap_or_else(|| panic!("列表中的 {} 必须可被 get 查询到", item.id));
            assert_eq!(
                fetched.id, item.id,
                "get() 返回的 id 必须与列表一致（大小写敏感）"
            );
            // 忽略大小写的查询也应返回**规范拼写**，而不是查询参数本身
            let lower = get(&types(), &mi, &mo, &aliases, &item.id.to_lowercase())
                .expect("小写查询也必须命中");
            assert_eq!(lower.id, item.id, "小写查询应返回规范拼写");
        }
    }

    #[test]
    fn mixed_case_alias_keeps_original_spelling() {
        let (mi, mo) = limits();
        let aliases = vec!["GPT-4o-Mini".to_string()];
        let fetched = get(&types(), &mi, &mo, &aliases, "gpt-4o-mini").unwrap();
        assert_eq!(
            fetched.id, "GPT-4o-Mini",
            "不应把小写查询参数当作规范 ID 返回"
        );
    }

    #[test]
    fn aliases_shorter_than_model_types_are_tolerated() {
        let (mi, mo) = limits();
        let aliases = vec!["only-first".to_string()];
        let list = list(&types(), &mi, &mo, &aliases);
        assert!(list.data.iter().any(|m| m.id == "only-first"));
        assert!(list.data.iter().any(|m| m.id == "deepseek-expert"));
    }

    #[test]
    fn blank_alias_is_ignored() {
        let aliases = vec!["  ".to_string(), String::new()];
        let ids: Vec<String> = model_ids(&types(), &aliases)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert!(
            !ids.iter().any(|id| id.trim().is_empty()),
            "空白别名不得成为模型 ID：{ids:?}"
        );
    }

    #[test]
    fn no_duplicate_ids_when_type_already_prefixed() {
        // model_type 写成 "deepseek-x" 时，deepseek- 前缀会重复
        let types = vec!["deepseek-x".to_string()];
        let ids = model_ids(&types, &[]);
        let unique: std::collections::HashSet<String> =
            ids.iter().map(|(id, _)| id.to_lowercase()).collect();
        assert_eq!(ids.len(), unique.len(), "ID 列表必须无重复：{ids:?}");
    }

    #[test]
    fn entries_carry_display_name_and_capabilities() {
        let (mi, mo) = limits();
        let list = list(&types(), &mi, &mo, &[]);
        let default = list
            .data
            .iter()
            .find(|m| m.id == "deepseek-default")
            .unwrap();
        assert_eq!(default.name.as_deref(), Some("DeepSeek Default"));
        assert_eq!(default.model_type, "default");
        assert!(default.capabilities.output.tool_calling);
        assert!(default.capabilities.output.thinking);
        assert!(default.capabilities.output.structured_outputs);
        assert!(!default.vision);
        assert!(!default.capabilities.input.image_input);
    }

    #[test]
    fn vision_type_declares_image_input() {
        let types = vec!["vision".to_string()];
        let (mi, mo) = (vec![100u32], vec![10u32]);
        let model = get(&types, &mi, &mo, &[], "deepseek-vision").unwrap();
        assert_eq!(model.name.as_deref(), Some("DeepSeek Vision"));
        assert!(model.vision);
        assert!(model.capabilities.input.image_input);
        assert!(model.capabilities.input.pdf_input);
    }

    #[test]
    fn alias_reuses_base_model_metadata() {
        let (mi, mo) = limits();
        let aliases = vec!["my-model".to_string()];
        // 别名按 index 对齐 model_types，这里别名落在 default 上
        let base = get(&types(), &mi, &mo, &aliases, "deepseek-default").unwrap();
        let alias = get(&types(), &mi, &mo, &aliases, "my-model").unwrap();
        assert_eq!(alias.name, base.name);
        assert_eq!(alias.model_type, base.model_type);
        assert_eq!(
            alias.capabilities.input.image_input,
            base.capabilities.input.image_input
        );
    }

    #[test]
    fn serialized_entry_exposes_name_and_capabilities() {
        let (mi, mo) = limits();
        let model = get(&types(), &mi, &mo, &[], "deepseek-default").unwrap();
        let json = serde_json::to_value(&model).unwrap();
        assert_eq!(json["name"], "DeepSeek Default");
        assert_eq!(json["capabilities"]["tool_calling"], true);
        assert_eq!(json["capabilities"]["image_input"], false);
        assert_eq!(json["vision"], false);
        assert_eq!(json["reasoning"], true);
        assert_eq!(json["model_type"], "default");
    }

    #[test]
    fn canonical_spelling_survives_round_trip_through_list_and_get() {
        let (mi, mo) = limits();
        let aliases = vec!["Alias-Mixed-Case".to_string()];
        // list 中的每一项都能被 get 以同一拼写取回
        for item in list(&types(), &mi, &mo, &aliases).data {
            assert_eq!(
                get(&types(), &mi, &mo, &aliases, &item.id).unwrap().id,
                item.id
            );
        }
    }
}
