//! 二进制自更新（OTA）—— 只用 GitHub Releases 这一条通道
//!
//! # 设计取舍
//!
//! - **只做「检查」与「等价替换」**：下载发布产物、用 `SHA256SUMS` 校验、替换磁盘上
//!   的二进制，再由调用方重启进程。不做差分补丁、不做签名校验（发布产物由 CI 生成，
//!   校验和与产物同源，能挡住传输损坏与镜像劫持，挡不住发布账号被接管 —— 与
//!   `curl | bash` 的风险同级，文档里写清楚即可）。
//! - **平台识别**：沿用 CI 产物的命名（`{tag}-{deploy}.tar.gz` / `.zip`），
//!   `deploy` 由 `std::env::consts::{OS, ARCH}` + 是否静态链接（musl）推断。
//! - **应用方式**：内存里解压 → 写 `<bin>.new` → `chmod 755` → `rename()` 原子替换。
//!   `rename` 对正在运行的可执行文件是安全的（inode 不变），因此替换后必须重启。
//! - **回滚**：替换前若存在原文件，先 `rename` 成 `<bin>.old`；调用方重启后若健康检查
//!   失败，可用 [`rollback`] 换回。
//!
//! 本模块只做纯 I/O 与解析，不做 HTTP —— 请求走 `ds_core::DsCore::fetch_text` /
//! `fetch_bytes`，复用既有的拟态客户端 + 代理配置（根 crate 没有独立的 HTTP 客户端依赖）。

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use serde::Deserialize;

mod zip;

/// 产物文件在 GitHub Release 里的命名后缀（未压缩的二进制名）
pub(crate) const BINARY_NAME: &str = "ds-free-api";

/// 本机部署标识，例如 `linux-x86_64-gnu` / `macos-aarch64` / `windows-x86_64`
///
/// `cfg!(target_env = "musl")` 只有在编译期才有意义；Windows / macOS 的产物
/// 命名不带 libc 段（`ds-free-api-v0.6.5-windows-x86_64.zip`），所以只有
/// Linux 会追加 `-gnu` / `-musl`。
#[must_use]
pub fn deploy_id() -> String {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    if os == "linux" {
        let libc = if cfg!(target_env = "musl") {
            "musl"
        } else {
            "gnu"
        };
        format!("{os}-{arch}-{libc}")
    } else {
        format!("{os}-{arch}")
    }
}

/// GitHub Releases API 的最小响应视图
#[derive(Debug, Clone, Deserialize)]
pub struct Release {
    pub tag_name: String,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub published_at: Option<String>,
    #[serde(default)]
    pub assets: Vec<ReleaseAsset>,
}

/// Release 里的单个产物
#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseAsset {
    pub name: String,
    pub browser_download_url: String,
    #[serde(default)]
    pub size: u64,
}

impl Release {
    /// 按部署标识挑选本平台产物
    #[must_use]
    pub fn asset_for(&self, deploy: &str) -> Option<&ReleaseAsset> {
        let want_tar = format!("{deploy}.tar.gz");
        let want_zip = format!("{deploy}.zip");
        self.assets
            .iter()
            .find(|a| a.name.ends_with(&want_tar) || a.name.ends_with(&want_zip))
    }

    /// 校验和文件（`SHA256SUMS`）
    #[must_use]
    pub fn checksum_asset(&self) -> Option<&ReleaseAsset> {
        self.assets
            .iter()
            .find(|a| a.name.eq_ignore_ascii_case("SHA256SUMS"))
    }
}

/// 把 `v0.6.5` / `0.6.5` 解析成可比较的三段数字
///
/// 只用 patch 版本号比较（本项目的 tag 从不带预发布后缀）；解析失败返回 `None`，
/// 调用方据此把「检查更新」判定为不可用，而不是猜测。
#[must_use]
pub fn parse_version(tag: &str) -> Option<(u64, u64, u64)> {
    let core = tag.trim().trim_start_matches('v');
    let mut it = core.split('.');
    let major = it.next()?.parse().ok()?;
    let minor = it.next()?.parse().ok()?;
    let patch = it.next().unwrap_or("0").parse().ok()?;
    Some((major, minor, patch))
}

/// `latest` 是否严格新于 `current`
#[must_use]
pub fn is_newer(current: &str, latest: &str) -> bool {
    match (parse_version(current), parse_version(latest)) {
        (Some(c), Some(l)) => l > c,
        _ => false,
    }
}

/// `s` 是否是 64 位十六进制（即一个合法 SHA-256 摘要）
#[must_use]
pub fn is_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// 从 `SHA256SUMS` 文本里取出指定文件名的摘要（小写十六进制）
#[must_use]
pub fn find_checksum(sums: &str, file_name: &str) -> Option<String> {
    for line in sums.lines() {
        let mut parts = line.split_whitespace();
        let (Some(hash), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        let name = name.trim_start_matches('*');
        if name == file_name && is_sha256(hash) {
            return Some(hash.to_ascii_lowercase());
        }
    }
    None
}

/// 计算字节串的 SHA-256（小写十六进制）
///
/// 依赖 `sha2`；编译器给出的是稳定实现，无需额外进程。
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut acc, b| {
            use std::fmt::Write as _;
            let _ = write!(acc, "{b:02x}");
            acc
        })
}

/// 从 `.tar.gz` 里取出二进制内容
///
/// 只接受**文件名等于** `ds-free-api`（或带 `.exe`）的那一个条目，避免
/// 归档里混入脚本或路径穿越（`..` / 绝对路径）时被误写。
pub fn extract_binary_tar_gz(archive: &[u8]) -> Result<Vec<u8>, String> {
    let decoder = flate2::read::GzDecoder::new(Cursor::new(archive));
    let mut tar = tar::Archive::new(decoder);
    let entries = tar.entries().map_err(|e| format!("读取 tar 失败: {e}"))?;
    for entry in entries {
        let mut entry = entry.map_err(|e| format!("读取 tar 条目失败: {e}"))?;
        let path = entry.path().map_err(|e| format!("读取条目路径失败: {e}"))?;
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name != BINARY_NAME && name != format!("{BINARY_NAME}.exe").as_str() {
            continue;
        }
        let mut buf = Vec::new();
        entry
            .read_to_end(&mut buf)
            .map_err(|e| format!("解压二进制失败: {e}"))?;
        return Ok(buf);
    }
    Err(format!("归档里没有 {BINARY_NAME}"))
}

/// 按文件名后缀选择解压器
///
/// `.tar.gz` 走 flate2+tar（Linux / macOS gnu 产物），`.zip` 走 `zip` 子模块。
pub fn extract_binary(asset_name: &str, archive: &[u8]) -> Result<Vec<u8>, String> {
    if asset_name.ends_with(".tar.gz") {
        extract_binary_tar_gz(archive)
    } else if asset_name.ends_with(".zip") {
        zip::extract_binary_zip(archive)
    } else {
        Err(format!("不支持的产物格式: {asset_name}"))
    }
}

/// 原子替换磁盘上的二进制
///
/// 步骤：写 `<bin>.new` → 落盘 → `chmod 755` → 若原文件存在则 `rename` 成
/// `<bin>.old` → `rename` 新文件到位。任何一步失败都保留原文件不动。
///
/// 返回备份路径（`<bin>.old`），供 [`rollback`] 使用。
pub fn apply_binary(bin_path: &Path, binary: &[u8]) -> Result<PathBuf, String> {
    let new_path = bin_path.with_extension("new");
    let old_path = bin_path.with_extension("old");

    std::fs::write(&new_path, binary).map_err(|e| format!("写入 {}: {e}", new_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&new_path, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| format!("设置权限失败: {e}"))?;
    }
    // 上一次更新残留的备份先清掉，避免 rename 到已存在的目标（Windows 不允许）
    if old_path.exists() {
        cleanup(&old_path);
    }
    if bin_path.exists() {
        std::fs::rename(bin_path, &old_path).map_err(|e| format!("备份原二进制失败: {e}"))?;
    }
    if let Err(e) = std::fs::rename(&new_path, bin_path) {
        // 替换失败时把原文件换回来，保证服务还能被重启
        if old_path.exists()
            && let Err(restore) = std::fs::rename(&old_path, bin_path)
        {
            log::error!(target: "self_update", "回滚原二进制失败: {restore}");
        }
        cleanup(&new_path);
        return Err(format!("替换二进制失败: {e}"));
    }
    Ok(old_path)
}

/// 用 `<bin>.old` 换回旧二进制（重启后健康检查失败时调用）
pub fn rollback(bin_path: &Path) -> Result<(), String> {
    let old_path = bin_path.with_extension("old");
    if !old_path.exists() {
        return Err("没有可回滚的备份".to_string());
    }
    std::fs::rename(&old_path, bin_path).map_err(|e| format!("回滚失败: {e}"))
}

/// 清理更新残留（`<bin>.old`），在确认新版本可用后调用
pub fn cleanup(path: &Path) {
    // 删除失败不是错误：可能已被其他实例删掉，或文件被占用（Windows）
    if let Err(e) = std::fs::remove_file(path) {
        log::debug!(target: "self_update", "清理 {} 失败（可忽略）: {e}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_error_kinds_drive_status_codes() {
        // 分类是「502 满天飞」这次修复的核心：只有真正的外部依赖故障能算 502，
        // 业务拒绝必须是 4xx，本地写盘失败必须是 5xx 而不是网关错误。
        use crate::server::update_error_status;
        assert_eq!(
            update_error_status(UpdateErrorKind::Rejected),
            axum::http::StatusCode::CONFLICT
        );
        assert_eq!(
            update_error_status(UpdateErrorKind::Upstream),
            axum::http::StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            update_error_status(UpdateErrorKind::Apply),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn already_latest_is_a_rejection_not_an_upstream_failure() {
        let err = UpdateError::rejected("当前版本 0.7.2 已是最新，无需更新");
        assert_eq!(err.kind, UpdateErrorKind::Rejected);
        assert_eq!(err.to_string(), "当前版本 0.7.2 已是最新，无需更新");
    }

    #[test]
    fn view_flags_missing_platform_asset_as_not_available() {
        // release 比当前新，但没有本平台产物 → update_available 必须为 false，
        // 否则前端会放行「下载并替换」，后端再报「没有本平台产物」。
        let rel = Release {
            tag_name: "v9.9.9".into(),
            body: None,
            published_at: None,
            assets: vec![ReleaseAsset {
                name: "ds-free-api-v9.9.9-plan9-sparc.tar.gz".into(),
                browser_download_url: "https://example.invalid/a.tar.gz".into(),
                size: 1,
            }],
        };
        let view = view_from_release(&rel, "0.7.2");
        assert!(is_newer("0.7.2", &rel.tag_name));
        assert!(!view.update_available);
        assert!(view.asset.is_none());
        assert!(view.download_url.is_none());
    }

    #[test]
    fn parses_versions_and_orders_them() {
        assert_eq!(parse_version("v0.6.10"), Some((0, 6, 10)));
        assert_eq!(parse_version("0.7"), Some((0, 7, 0)));
        assert_eq!(parse_version("nope"), None);
        assert!(is_newer("0.6.5", "v0.6.10"));
        assert!(!is_newer("0.6.5", "v0.6.5"));
        assert!(!is_newer("0.6.5", "v0.6.4"));
        assert!(!is_newer("0.6.5", "garbage"));
    }

    #[test]
    fn picks_platform_asset() {
        let rel = Release {
            tag_name: "v0.6.6".into(),
            body: None,
            published_at: None,
            assets: vec![
                ReleaseAsset {
                    name: "ds-free-api-v0.6.6-linux-x86_64-gnu.tar.gz".into(),
                    browser_download_url: "u1".into(),
                    size: 1,
                },
                ReleaseAsset {
                    name: "ds-free-api-v0.6.6-linux-x86_64-musl.tar.gz".into(),
                    browser_download_url: "u2".into(),
                    size: 1,
                },
                ReleaseAsset {
                    name: "SHA256SUMS".into(),
                    browser_download_url: "u3".into(),
                    size: 1,
                },
            ],
        };
        let asset = rel.asset_for("linux-x86_64-gnu").expect("应找到 gnu 产物");
        assert_eq!(asset.browser_download_url, "u1");
        assert!(rel.checksum_asset().is_some());
        assert!(rel.asset_for("windows-x86_64").is_none());
    }

    #[test]
    fn checksum_lookup_ignores_other_files() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let sums = format!(
            "{a}  ds-free-api-v0.6.5-linux-x86_64-gnu.tar.gz\n\
             {b} *ds-free-api-v0.6.6-linux-x86_64-gnu.tar.gz\n"
        );
        let name = "ds-free-api-v0.6.6-linux-x86_64-gnu.tar.gz";
        // `*` 前缀（二进制模式标记）必须被剥掉
        assert_eq!(find_checksum(&sums, name), Some(b));
        assert_eq!(
            find_checksum(&sums, "ds-free-api-v0.6.5-linux-x86_64-gnu.tar.gz"),
            Some(a)
        );
        assert!(find_checksum(&sums, "missing.tar.gz").is_none());
        // 短哈希 / 非 64 位行不参与匹配（防止 `abc  file` 这类垃圾行命中）
        assert!(find_checksum("abc  missing.tar.gz\n", "missing.tar.gz").is_none());
    }

    #[test]
    fn sha256_matches_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn tar_extraction_skips_other_entries() {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_size(3);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "README.md", &b"hi\n"[..])
            .unwrap();
        let mut header = tar::Header::new_gnu();
        header.set_size(4);
        header.set_mode(0o755);
        header.set_cksum();
        builder
            .append_data(&mut header, "ds-free-api", &b"BINX"[..])
            .unwrap();
        let tar_bytes = builder.into_inner().unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(&mut gz, &tar_bytes).unwrap();
        let archive = gz.finish().unwrap();
        assert_eq!(extract_binary_tar_gz(&archive).unwrap(), b"BINX");
    }
}

// ── OTA：检查 / 下载 / 应用 ────────────────────────────────────────────────

/// 更新状态机（只存在进程内存里：重启后重新从「空闲」开始）
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum UpdateStatus {
    /// 尚未检查过
    Idle,
    /// 已检查：`latest` 是仓库最新 tag，`update_available` 表示比当前运行版本新
    Checked {
        latest: String,
        current: String,
        update_available: bool,
        asset: Option<String>,
        notes: Option<String>,
        checked_at: u64,
    },
    /// 正在下载 / 校验
    Downloading { latest: String, asset: String },
    /// 已替换磁盘上的二进制，等待重启
    Ready {
        latest: String,
        asset: String,
        backup: String,
    },
    /// 失败（错误信息是人可读的，不回显 token）
    Failed {
        latest: Option<String>,
        error: String,
        at: u64,
    },
}

/// GitHub Release 检查结果（对外返回的视图）
#[derive(Debug, Clone, serde::Serialize)]
pub struct UpdateCheckView {
    pub current: String,
    pub latest: String,
    pub update_available: bool,
    pub asset: Option<String>,
    pub download_url: Option<String>,
    pub notes: Option<String>,
    pub published_at: Option<String>,
    pub deploy_id: String,
    pub status: UpdateStatus,
}

/// OTA 失败分类 —— 决定管理面板该回什么 HTTP 状态码
///
/// 之前所有失败都被压成 `502 Bad Gateway`，于是「已是最新」「allow_apply=false」
/// 这种纯业务状态也被当成上游故障，前端只能显示一句 `API error: 502`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateErrorKind {
    /// 功能未开启 / 配置不允许 / 已是最新 / 没有本平台产物 —— 4xx，用户能自己改
    Rejected,
    /// 真正的外部依赖故障：GitHub 不可达、非 2xx、JSON 解析失败、下载失败
    Upstream,
    /// 本地失败：解压 / 写盘 / 替换二进制
    Apply,
}

/// OTA 失败：`kind` 决定状态码，`message` 直接给用户看（中文、可执行）
#[derive(Debug, Clone)]
pub struct UpdateError {
    pub kind: UpdateErrorKind,
    pub message: String,
}

impl UpdateError {
    fn rejected(message: impl Into<String>) -> Self {
        Self {
            kind: UpdateErrorKind::Rejected,
            message: message.into(),
        }
    }

    fn upstream(message: impl Into<String>) -> Self {
        Self {
            kind: UpdateErrorKind::Upstream,
            message: message.into(),
        }
    }

    fn apply(message: impl Into<String>) -> Self {
        Self {
            kind: UpdateErrorKind::Apply,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// GitHub API 上的 release 对象 URL
fn release_path(update: &crate::config::UpdateSection) -> String {
    let repo = update.repo.trim().trim_matches('/');
    if update.use_latest || update.target_tag.trim().is_empty() {
        format!(
            "{}/repos/{repo}/releases/latest",
            update.api_base.trim_end_matches('/')
        )
    } else {
        format!(
            "{}/repos/{repo}/releases/tags/{}",
            update.api_base.trim_end_matches('/'),
            update.target_tag.trim()
        )
    }
}

/// 拉一次 release JSON 并解析（`check` 与 `apply` 共用，避免重复请求）
async fn fetch_release(
    core: &ds_core::DsCore,
    update: &crate::config::UpdateSection,
) -> Result<Release, UpdateError> {
    let url = release_path(update);
    let (status, body) = core
        .fetch_text(&url)
        .await
        .map_err(|e| UpdateError::upstream(format!("请求 GitHub 失败（{url}）: {e}")))?;
    if !(200..300).contains(&status) {
        let hint = match status {
            403 | 429 => "（可能是 GitHub API 速率限制或代理被拒）",
            404 => "（仓库或 release 不存在，检查 update.repo / target_tag）",
            _ => "",
        };
        return Err(UpdateError::upstream(format!(
            "GitHub 返回 HTTP {status}{hint}: {}",
            body.chars().take(200).collect::<String>()
        )));
    }
    serde_json::from_str(&body)
        .map_err(|e| UpdateError::upstream(format!("解析 release JSON 失败: {e}")))
}

/// 把一次 release 解析结果包装成对外视图
fn view_from_release(release: &Release, current: &str) -> UpdateCheckView {
    let deploy = deploy_id();
    let asset = release.asset_for(&deploy);
    let latest = release.tag_name.clone();
    let update_available = is_newer(current, &latest) && asset.is_some();
    UpdateCheckView {
        current: current.to_string(),
        update_available,
        latest: latest.clone(),
        asset: asset.map(|a| a.name.clone()),
        download_url: asset.map(|a| a.browser_download_url.clone()),
        notes: release.body.clone(),
        published_at: release.published_at.clone(),
        deploy_id: deploy,
        status: UpdateStatus::Checked {
            latest,
            current: current.to_string(),
            update_available,
            asset: asset.map(|a| a.name.clone()),
            notes: release.body.clone(),
            checked_at: now_secs(),
        },
    }
}

/// 查一次最新 release 并解析出本平台产物
pub async fn check(
    core: &ds_core::DsCore,
    update: &crate::config::UpdateSection,
    current: &str,
) -> Result<UpdateCheckView, UpdateError> {
    let release = fetch_release(core, update).await?;
    Ok(view_from_release(&release, current))
}

/// 下载 + 校验 + 原子替换当前可执行文件
///
/// 返回替换完成后应写回的状态（`Ready`）。**不做重启** —— 重启策略由调用方决定
/// （CLI 进程退出让 systemd / docker 拉起，或由管理面板提示用户）。
///
/// 只发两次 release API 请求中的**一次**：`view` 里的 `download_url` 与资产名已经
/// 足够定位产物，校验和从同一次 release 里取，不再重查 JSON。
pub async fn download_and_apply(
    core: &ds_core::DsCore,
    update: &crate::config::UpdateSection,
    view: &UpdateCheckView,
    bin_path: &Path,
) -> Result<UpdateStatus, UpdateError> {
    let (Some(asset_name), Some(url)) = (view.asset.clone(), view.download_url.clone()) else {
        return Err(UpdateError::rejected(format!(
            "最新版本 {} 没有本平台（{}）的产物",
            view.latest, view.deploy_id
        )));
    };
    if !view.update_available {
        return Err(UpdateError::rejected(format!(
            "当前版本 {} 已是最新，无需更新",
            view.current
        )));
    }

    let archive = core
        .fetch_bytes(&url)
        .await
        .map_err(|e| UpdateError::upstream(format!("下载产物失败: {e}")))?;
    let expected = match update_checksum(core, update).await {
        Ok(sums) => find_checksum(&sums, &asset_name),
        Err(e) => {
            // 没有 SHA256SUMS 时只告警不阻断：老版本发布产物没有该文件
            log::warn!(target: "self_update", "读取 SHA256SUMS 失败，跳过校验: {e}");
            None
        }
    };
    if let Some(expected) = expected {
        let actual = sha256_hex(&archive);
        if actual != expected {
            return Err(UpdateError::upstream(format!(
                "SHA256 校验失败: 期望 {expected}，实际 {actual}"
            )));
        }
    }

    let binary = extract_binary(&asset_name, &archive).map_err(UpdateError::apply)?;
    let backup = apply_binary(bin_path, &binary).map_err(UpdateError::apply)?;
    Ok(UpdateStatus::Ready {
        latest: view.latest.clone(),
        asset: asset_name,
        backup: backup.display().to_string(),
    })
}

/// 取 `SHA256SUMS` 文本（同一次 release）
async fn update_checksum(
    core: &ds_core::DsCore,
    update: &crate::config::UpdateSection,
) -> Result<String, String> {
    let release = fetch_release(core, update).await.map_err(|e| e.message)?;
    let asset = release
        .checksum_asset()
        .ok_or_else(|| "该 release 没有 SHA256SUMS".to_string())?;
    let (status, body) = core
        .fetch_text(&asset.browser_download_url)
        .await
        .map_err(|e| format!("下载 SHA256SUMS 失败: {e}"))?;
    if !(200..300).contains(&status) {
        return Err(format!("下载 SHA256SUMS 返回 HTTP {status}"));
    }
    Ok(body)
}

/// 当前 Unix 秒
#[must_use]
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// 正在运行的可执行文件路径
pub fn current_exe() -> Result<PathBuf, String> {
    std::env::current_exe().map_err(|e| format!("无法定位当前可执行文件: {e}"))
}
