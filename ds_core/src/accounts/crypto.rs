//! 登录请求体加密 —— 对齐真实 Web 客户端的混合加密
//!
//! 2026-10 起 `POST /api/v0/users/login` 不再接受 JSON 明文：body 必须是
//! AES-128-CBC(PKCS#7) 密文，密钥本身再用服务端 RSA 公钥以 RSA-OAEP-SHA256 包裹，
//! 放在请求头 `x-request-encryption-key` 里。证据见 `docs/login-encryption-audit.md`。
//!
//! 关键参数（与前端 bundle 逐项对齐，不可随意改动）：
//! - AES key / IV 各 16 字节随机（每次请求重新生成）；
//! - 被 RSA 包裹的是 `key ‖ IV` 共 32 字节；
//! - 公钥是 SPKI DER 的 base64（RSA-2048，e=65537），按环境内嵌；
//! - content-type 仍为 `application/json`，但 body 是原始二进制密文。

use aes::Aes128;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use cbc::Encryptor;
use cbc::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
use rsa::PublicKey;
use rsa::pkcs8::DecodePublicKey;
#[cfg(test)]
use rsa::traits::PublicKeyParts;
use sha2::Sha256;

/// 处理后的登录请求体：密文 + 头部密钥
pub struct EncryptedBody {
    /// 原始 AES-CBC 密文，直接作为 HTTP body
    pub body: Vec<u8>,
    /// base64(RSA-OAEP-SHA256(AESKey ‖ IV))，放入 `x-request-encryption-key`
    pub key_header: String,
}

/// 生产环境 RSA 公钥（`eL.production`，SPKI DER → base64）
pub const PROD_PUBLIC_KEY_B64: &str = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0RLAhx1TOJhAQBHS1LHXlnkfOg86O+uhsmKWNNkmugVrRqO6cl13SnjBs2otRYZGJd50xpBG4We2i6/oCJCwxA/49YZfIwaeDmTx+WBIN7qrwhwP7vGCi6Osesmn/Ddgdye6mifYkRpRMonQjGY6VPaUMTw1jjOAIKph5YZn0bDVmGLvs5eH6CoQYHM5/Z1Nfhj36VL28eZi3elJHoz/L6Tzi2wY1rCYqYHfepq3Ysy8y5Y5BeFJXN60RYLlbs+MRC+2kyJqcfApUnn1O/W2om9dhxc6Rc5dqas1YB4X4CWCttrKk/suhpIx9zpQY9kqtotbe5zweYaKv0rCVeqx8wIDAQAB";

/// 加密一段 JSON body
///
/// 返回的 `EncryptedBody` 直接替换原请求：`body` 作 HTTP body，
/// `key_header` 写入 `x-request-encryption-key`。
pub fn encrypt_body(plaintext: &[u8], public_key_b64: &str) -> Result<EncryptedBody, CryptoError> {
    let der = B64
        .decode(public_key_b64)
        .map_err(|e| CryptoError::PublicKey(e.to_string()))?;
    let pubkey =
        PublicKey::from_public_key_der(&der).map_err(|e| CryptoError::PublicKey(e.to_string()))?;

    let mut key = [0u8; 16];
    let mut iv = [0u8; 16];
    getrandom::fill(&mut key).map_err(|e| CryptoError::Encrypt(e.to_string()))?;
    getrandom::fill(&mut iv).map_err(|e| CryptoError::Encrypt(e.to_string()))?;

    let body =
        Encryptor::<Aes128>::new(&key.into(), &iv.into()).encrypt_padded_vec::<Pkcs7>(plaintext);

    let mut blob = [0u8; 32];
    blob[..16].copy_from_slice(&key);
    blob[16..].copy_from_slice(&iv);
    let wrapped = pubkey
        .oaep_encrypt::<Sha256>(&blob)
        .map_err(|e| CryptoError::Encrypt(e.to_string()))?;

    Ok(EncryptedBody {
        body,
        key_header: B64.encode(wrapped),
    })
}

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("公钥解析失败: {0}")]
    PublicKey(String),
    #[error("加密失败: {0}")]
    Encrypt(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_key_parses_as_rsa2048() {
        let der = B64.decode(PROD_PUBLIC_KEY_B64).expect("base64");
        assert_eq!(der.len(), 294);
        let pk = PublicKey::from_public_key_der(&der).expect("spki");
        assert_eq!(pk.n().bit_width(), 2048);
    }

    #[test]
    fn encrypted_body_is_block_aligned_and_key_is_256_bytes() {
        let body = br#"{"email":"a@b.c","mobile":"","password":"x","area_code":"","device_id":"d","os":"web"}"#;
        let out = encrypt_body(body, PROD_PUBLIC_KEY_B64).expect("encrypt");
        assert_eq!(out.body.len() % 16, 0);
        assert!(!out.body.is_empty());
        let wrapped = B64.decode(&out.key_header).expect("b64");
        assert_eq!(wrapped.len(), 256);
    }

    #[test]
    fn padding_pkcs7_adds_full_block_for_aligned_input() {
        let out = encrypt_body(&[0u8; 32], PROD_PUBLIC_KEY_B64).expect("encrypt");
        assert_eq!(out.body.len(), 48);
    }
}
