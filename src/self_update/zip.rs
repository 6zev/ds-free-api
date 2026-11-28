//! `.zip` 产物解压（Windows / macOS 发布包）
//!
//! 2026-11 起 CI 对**所有平台**都产出 `tar.gz`，zip 只是历史产物的兼容路径。
//! 这里手写最小化的中央目录解析：只读「local file header + 数据」，不落盘、
//! 不建目录；条目名只用最后一段比较，天然规避路径穿越。
//!
//! 支持的压缩方式：0 = stored、8 = deflate（CI 的 `zip` 产物只用这两种）。

use std::io::Read;

const SIG_LOCAL: u32 = 0x0403_4b50;
const SIG_CENTRAL: u32 = 0x0201_4b50;

fn u16_at(buf: &[u8], at: usize) -> Result<u16, String> {
    buf.get(at..at + 2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]))
        .ok_or_else(|| "zip 结构截断（u16）".to_string())
}

fn u32_at(buf: &[u8], at: usize) -> Result<u32, String> {
    buf.get(at..at + 4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .ok_or_else(|| "zip 结构截断（u32）".to_string())
}

/// 从 `.zip` 里取出二进制内容
pub fn extract_binary_zip(archive: &[u8]) -> Result<Vec<u8>, String> {
    let want = super::BINARY_NAME;
    let want_exe = format!("{want}.exe");
    let mut pos = 0usize;
    while pos + 4 <= archive.len() {
        let sig = u32_at(archive, pos)?;
        if sig == SIG_CENTRAL {
            break;
        }
        if sig != SIG_LOCAL {
            return Err(format!("非法的 zip 条目签名 @{pos}"));
        }
        let method = u16_at(archive, pos + 8)?;
        let compressed = u32_at(archive, pos + 18)? as usize;
        let name_len = u16_at(archive, pos + 26)? as usize;
        let extra_len = u16_at(archive, pos + 28)? as usize;
        let name_start = pos + 30;
        let name_end = name_start + name_len;
        let name = archive
            .get(name_start..name_end)
            .ok_or_else(|| "zip 文件名截断".to_string())?;
        let data_start = name_end + extra_len;
        let data_end = data_start
            .checked_add(compressed)
            .ok_or_else(|| "zip 条目长度溢出".to_string())?;
        let data = archive
            .get(data_start..data_end)
            .ok_or_else(|| "zip 数据截断".to_string())?;
        let base = std::str::from_utf8(name).ok().and_then(|n| {
            std::path::Path::new(n)
                .file_name()
                .and_then(|x| x.to_str())
                .map(str::to_owned)
        });
        if base.as_deref() == Some(want) || base.as_deref() == Some(want_exe.as_str()) {
            return match method {
                0 => Ok(data.to_vec()),
                8 => {
                    let mut out = Vec::new();
                    flate2::read::DeflateDecoder::new(data)
                        .read_to_end(&mut out)
                        .map_err(|e| format!("解压 deflate 失败: {e}"))?;
                    Ok(out)
                }
                other => Err(format!("不支持的 zip 压缩方式: {other}")),
            };
        }
        pos = data_end;
    }
    Err(format!("归档里没有 {want}"))
}
