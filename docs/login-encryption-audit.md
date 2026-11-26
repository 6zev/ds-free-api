# DeepSeek `POST /api/v0/users/login` payload 加密审计

> 审计日期：2026-11-26
> 触发：issue #118 —— 2026-10/11 起向 `users/login` 发送 JSON plaintext 被 HTTP 422。
> 结论先行：**web 客户端的 login 请求体确实在客户端做「AES-CBC + RSA-OAEP 包裹密钥」的混合加密，密钥不再放在 body 里，而是放在请求头 `x-request-encryption-key` 中。** 已在线上主 bundle 里逐行取证（见 §2/§3）。

---

## 1. 当前代码现状

### 1.1 端点常量

`ds_core/src/accounts/client.rs:20`

```rust
const ENDPOINT_USERS_LOGIN: &str = "/users/login";
```

### 1.2 `LoginPayload`（`ds_core/src/accounts/client.rs:105-117`）

```rust
#[derive(Debug, Serialize)]
pub struct LoginPayload {
    pub email: String,
    pub mobile: String,
    pub password: String,
    pub area_code: String,
    pub device_id: String,
    pub os: String,
}
```

固定 6 个 key：`email` / `mobile` 二选一（另一个空串）、`password`、`area_code`、`device_id`、`os`。

### 1.3 `login()` 发送方式（`ds_core/src/accounts/client.rs:632-655`）

```rust
pub async fn login(&self, payload: &LoginPayload) -> Result<LoginData, ClientError> {
    let mut h = self.client_headers()?;
    h.insert(REFERER, "https://chat.deepseek.com/sign_in");
    let resp = self.http
        .post(format!("{}{}", self.api_base, ENDPOINT_USERS_LOGIN))
        .headers(h)
        .json(payload)          // ← JSON plaintext
        .send().await?;
    ...
}
```

- **当前发送的是 6 字段 JSON plaintext**（`wreq` 的 `.json()` 会设 `content-type: application/json`）。
- 带上 `ClientIdentity::headers()` 的完整 `x-*` 头集合（`client.rs:411-451`）：
  `User-Agent`、`X-Client-Version`、`X-Client-Platform`、`X-Client-Locale`、
  `X-Client-Bundle-Id`、`X-Device-Id`、`X-Device-Model`、`X-Client-Timezone-Offset`、
  `Accept-Language`、`Referer`、`Accept: */*`、`sec-fetch-*`、`Sec-Ch-Ua*`。
- `device_id` 来自 `LoginPayload`（每账号独立，`pool.rs:984-991` 用 `creds.device_id` 填充），
  `os` 来自 `client.client_os()`。

### 1.4 `ClientIdentity`（`client.rs:381-396`）

```rust
pub struct ClientIdentity {
    pub user_agent, client_version, client_platform, client_locale,
    pub client_bundle_id, device_id, device_model, timezone_offset,
    pub client_os,   // 登录 payload 的 os 字段
}
```

`EmulationProfile`（`client.rs:333-342`）：`Chrome136`（默认，桌面浏览器身份）/ `OkHttp4_12`（安卓 App 身份）。
`login()` **不区分 emulation**，两种身份都走同一段 `.json(payload)` plaintext 代码。

### 1.5 旧文档 `ds_core/raw-api-reference.md:73-104`

旧文档：`device_id` 必填（省略→422），`device_id` 必须是数美 SDK 生成的真指纹，否则 `biz_code=11`；
`os` 必填。**旧文档完全没有提到客户端加密**——这正是本次要更新的事实基础：

- 旧文档的「422」当时对应的是「缺 `os` / 缺 `device_id`」的 body 校验失败；
- 2026-10/11 之后，同样的 plaintext body 仍 422，但原因变成了**整个 body 不再是服务端期望的
  「加密 envelope」形态**（即：body 必须是一段密文，而不是 6 字段 JSON）。

> 注：`raw-api-reference.md` 本次未改动。建议后续在 §0 增补「web 端 login body 需加密」。
> 本审计只新增本文件。

---

## 2. 上游证据（全部为实际读取的内容）

| 来源 | 实际结果 |
|------|----------|
| `https://chat.deepseek.com/`（直接请求） | **HTTP 403 CloudFront**「Request blocked」。证实 edge 层按请求特征拦截。 |
| `https://chat.deepseek.com/index.html` / `main.d1d163b7e4.js`（浏览器 UA） | **HTTP 202、body 为空**（WAF challenge）。 |
| `https://chat.deepseek.com/static/fp.min.js`（浏览器 UA） | **HTTP 200，348,688 字节**。数美 SMSdk 指纹脚本（见下）。 |
| `https://blog.laozhang.ai/zh/posts/deepseek-device-environment-error` | 2026-11-02 实测记录：curl 即使是桌面 UA 也 403 CloudFront；真实浏览器加载 `static/fp.min.js`，当时 **343,708 字节**混淆脚本；当日线上构建号 `main.d1d163b7e4`。 |
| SPA 壳（`chat.deepseek.com` 任意 `/xxx.js` 返回 app shell，`<meta name="commit-id" content="589c8931">`） | 从壳 HTML 拿到真实静态资源域名 `fe-static.deepseek.com` 与当前主 bundle。 |
| `https://fe-static.deepseek.com/chat/static/main.c918de4e90.js` | **HTTP 200，1,666,571 字节**，当前主 bundle（实际取证对象）。 |
| `https://fe-static.deepseek.com/chat/static/default-vendors.d1ba088e82.js` | HTTP 200，约 1.0 MB，vendor 包。 |

### 2.1 `fp.min.js` 的真实身份（澄清 commenter 的半截信息）

在 `static/fp.min.js`（348,688B）里检索：

```
encrypt:12  publicKey:5  JSEncrypt:3  RSA:7  AES:3  crypto.subtle:0  sm2:0  sm4:0  sendBeacon:0
```

- 全文是数美（Shumei）`SMSdk`：`window['SMSdk']`、`_smConf`、`deviceprofile/v4`、`smid`、
  `[SMSDK] onError` 等。
- 里面的 `JSEncrypt` + `AES` 是给**设备指纹上报**用的：`aesEncrypt(obj, key)` 使用
  **硬编码 IV `'0102030405060708'`**、AES-CBC + ZeroPadding，再用 SDK 的 `publicKey` 做 RSA。
  ——**这是数美自己上报 `deviceprofile` 的加密，不是 login body 加密。**
- 关键：`fp.min.js` 里 `users/login` / `login` / `sign_in` 出现次数均为 **0**。

→ **login 的加密逻辑不在 fp.min.js 里，而在主 bundle `main.*.js` 里。** HuskeyDev 所说的
`let r = JSON.stringify(e.json)` 字符串，在主 bundle 中确实存在（见 §3），对应 login 拦截器。

---

## 3. 加密机制（已从 `main.c918de4e90.js` 逐行取证）

### 3.1 拦截器：只加密特定 POST 路径

主 bundle 里的中间件 `eD(...)`（原文）：

```js
function eD(e){
  let {app:t, origin:n, environment:r, getCrypto:s=()=>globalThis.crypto}=e,
      a=ej({publicKey:eL[r], getCrypto:s});
  return async e=>{
    if((e.method?.toUpperCase())!=="POST") return;
    let i=e.url||"",
        o=new URL(...)  // 解析 baseUrl+url
    if(o.origin!==n || !eN[t].has(o.pathname)) return;   // 只对白名单路径且同源生效
    e.context.sensitive=true;
    ...
    let t=JSON.stringify(e.json);            // ← HuskeyDev 说的 plaintext 就是这里
    let n=new TextEncoder().encode(t),
        {body:r, encryptionKey:i}=await a(n);  // ← 加密
    ...
    let o=Object.fromEntries(Object.entries(e.headers??{})
          .filter(([k])=>!["content-type","content-length","x-request-encryption-key"]
                          .includes(k.toLowerCase())));
    o["content-type"]="application/json";
    o["x-request-encryption-key"]=i;          // ← 包裹后的 AES key 放这里
    e.headers=o; e.body=r; e.json=void 0;     // ← body 换成密文
  }
}
```

**路径白名单 `eN`**（chat app 用 `/api/v0/users/` 前缀）：

```js
let eR = ["create_sms_verification_code","create_email_verification_code",
          "login_by_mobile_sms","login","register_by_mobile","register",
          "check_sms_code","check_email_code","mobile_reset_password",
          "email_reset_password","rebind_mobile/finish",
          "rebind_mobile/verify_disabled_old_mobile","oauth/wechat/mobile_verification"];
eN = {
  chat:     new Set([...eR,"oauth/apple/mobile_verification","miniprogram/mobile_verification"]
                    .map(e=>"/api/v0/users/".concat(e))),
  platform: new Set([...eR,"bind_email"].map(e=>"/auth-api/v0/users/".concat(e))),
};
```

→ **`/api/v0/users/login` 在这个白名单里**，会被加密。同一 login 流程还叠加一层 **guest PoW**
（`x-ds-challenge-response` 头，见 `3.5`）。

### 3.2 加密算法（`ej(...)` 返回的加密器，原文）

```js
function ej(e){
  let t, {publicKey:n, getCrypto:r=()=>globalThis.crypto}=e;
  return async e=>{
    try{
      let s=r();
      if(!s?.subtle) throw Error("Web Crypto unavailable");
      t ??= s.subtle.importKey(
        "spki",
        Uint8Array.from(atob(n), c=>c.charCodeAt(0)),
        {name:"RSA-OAEP", hash:"SHA-256"}, !1, ["encrypt"]
      ).catch(e=>{throw t=void 0,e});
      let a=await t,
          i=s.getRandomValues(new Uint8Array(16)),   // AES-128 key, 16 字节随机
          o=s.getRandomValues(new Uint8Array(16)),   // IV,         16 字节随机
          l=await s.subtle.importKey("raw", i, "AES-CBC", !1, ["encrypt"]),
          d=await s.subtle.encrypt({name:"AES-CBC", iv:o}, l, e),  // body = AES-CBC(key, iv, utf8(JSON))
          c=new Uint8Array(32); c.set(i); c.set(o,16);              // key||iv = 32 字节
      let u=await s.subtle.encrypt({name:"RSA-OAEP"}, a, c);        // RSA-OAEP-SHA256(key||iv)
      return {body:d, encryptionKey:btoa(String.fromCharCode(...new Uint8Array(u))}};
    }catch{ throw Error("Request body encryption failed") }
  }
}
```

**精确参数：**

| 项 | 值 |
|----|----|
| 算法 | **混合加密**：AES-128-CBC 加密 body，RSA-OAEP(SHA-256) 加密 `AES_key ‖ IV` |
| AES key | 16 字节（128-bit），`crypto.getRandomValues` 每次随机 |
| AES IV | 16 字节随机 |
| AES padding | WebCrypto `AES-CBC` 默认 **PKCS#7**（WebCrypto 规范内置，代码没传 padding，故非 ZeroPadding） |
| 被加密明文 | `JSON.stringify(body)` 的 UTF-8 → 即 `{"email":..,"mobile":..,"password":..,"area_code":..,"device_id":..,"os":..}` 那 6 个字段 |
| RSA 公钥格式 | **SPKI DER**，`base64`（无 PEM 头），`importKey("spki", …)` |
| RSA 参数 | **`RSA-OAEP` + `hash: "SHA-256"`**，无 label |
| 被 RSA 包裹内容 | 32 字节 = `AES_key(16) ‖ IV(16)` |
| body 形态 | AES-CBC **原始密文二进制**（不是 base64，不是 hex），直接作为 HTTP request body |
| 密钥传递 | base64 后的 RSA 密文放在**请求头 `x-request-encryption-key`** |
| content-type | `application/json`（即便 body 是二进制密文） |

### 3.3 公钥（内嵌在主 bundle，按环境区分）

```js
eL = {
  development: "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAv0x0JJZVTnvYedB55cyrY6VNUibiCC2TbshHzM8LBhhamaRqJAuGzoXa2cJCyaYWGmD2JxZAJJFjU3PetLvjnO0SHN8rL5hBkmtrS/uYJ55lJcQYUIG95zo78d4GVVasSFhhqmV2PApEaItkYeTmd+/WI1BGj1oEBcR+TFTAYIJn1hYuZ9xylS/6SVL2niSTxXnQV9LJjqdSdBltAkQtrnutSTDaC4cLEKXETXF/Yr2NVOvqxWxLqu/aKW0a/5lH7ertHUniGSLbmBHyc9JasPcrn55RfNd6O1mk3FyWaxsDEDkJ30AW2E8qcxn0A0cxJg9j7UY6NYo5k/JXD7NdRQIDAQAB",
  production:  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0RLAhx1TOJhAQBHS1LHXlnkfOg86O+uhsmKWNNkmugVrRqO6cl13SnjBs2otRYZGJd50xpBG4We2i6/oCJCwxA/49YZfIwaeDmTx+WBIN7qrwhwP7vGCi6Osesmn/Ddgdye6mifYkRpRMonQjGY6VPaUMTw1jjOAIKph5YZn0bDVmGLvs5eH6CoQYHM5/Z1Nfhj36VL28eZi3elJHoz/L6Tzi2wY1rCYqYHfepq3Ysy8y5Y5BeFJXN60RYLlbs+MRC+2kyJqcfApUnn1O/W2om9dhxc6Rc5dqas1YB4X4CWCttrKk/suhpIx9zpQY9kqtotbe5zweYaKv0rCVeqx8wIDAQAB"
};
```

- `base64 → DER` 均为 294 字节，以 `30 82 01 22 30 0d 06 09 2a 86 48 86 f7 0d 01 01 01 …`
  开头（RSA SPKI），末 4 字节 `03 01 00 01`（exponent 65537）。→ **RSA-2048，公钥指数 65537。**
- 生产环境用 `production` 那把。两把都是**公钥**，可安全公开；客户端内嵌即证明这是服务端私钥对应的公开半部。
- 加密器会缓存 `importKey` 结果（`t ??= ...`），即整页生命周期内只导入一次。

### 3.4 最终 envelope（实际发到线上的请求）

```
POST /api/v0/users/login HTTP/1.1
Host: chat.deepseek.com
content-type: application/json
x-request-encryption-key: <base64( RSA-OAEP-SHA256( AESKey16 ‖ IV16 ) )>
x-ds-challenge-response: <guest PoW 解出的 challenge response>   ← 见 3.5
<其余 7 个 x-* 客户端头 + UA + Referer + sec-fetch-* + Sec-Ch-Ua*>
……（可选）……

<32 字节对齐的 AES-128-CBC 二进制密文>     ← 不再是 JSON 字段
```

**注意：**
- 原 `content-length` 头被**显式删除**（filter 列表包含它），再由 HTTP 层按新 body 重算。
- 原 `content-type` 也被删除后设为 `application/json`。
- 也就是说：**不再有 `{"email":...}` 这种可读 body**；body 是一段无法直接识别的二进制 AES 密文，
  解密密钥在 `x-request-encryption-key` 头里，且该头本身是 RSA-OAEP 密文。

### 3.5 叠加：guest PoW（与加密无关，但 login 也会经过）

同一 `ek` 中间件对白名单路径（含 login）先做一次 **guest PoW**：

- 向 `getChallengeUrl` 发 `{target_path: <原 path>}` 拿 `data.biz_data.guest_challenge`；
- 本地解 PoW，`(0,ew.fg)(challengeResponse, encoder)` 返回 `[headerName, headerValue]`，
  写入请求头。
- 另一处命中字符串为 `x-ds-challenge-response`（业务重放 `onBeforeReplay` 时带该头）。

→ login 现在是「**guest PoW 头 + 加密 body + 加密密钥头**」三重机制。现有 `ds_core` login 只发了
plaintext，缺后两者，故 422。

---

## 4. 结论与可行路径

### 4.1 (a) Rust 里实现 crypto 可行吗？需要什么 crate？

**完全可行**，工作量中等。算法是标准 WebCrypto：`RSA-OAEP-SHA256` + `AES-128-CBC-PKCS7`。

| 需求 | 推荐 crate | 说明 |
|------|-----------|------|
| RSA-OAEP(SHA-256) 公钥加密 32 字节 | `rsa`（RustCrypto）+ `sha2` | `rsa::Oaep::new::<Sha256>()`；公钥从 SPKI DER base64 解析：`RsaPublicKey::from_public_key_der()` |
| AES-128-CBC + PKCS7 | `aes` + `cbc`（RustCrypto） | `cbc::Encryptor<aes::Aes128>` + `BlockEncryptMut::encrypt_padded_vec_mut::<Pkcs7>` |
| 随机 key/iv | `rand`（或 `getrandom`） | 16B + 16B |
| base64 | `base64` | 包里已在用？需确认（wreq/serde 生态常见） |
| SPKI 解析 | `rsa::pkcs8::DecodePublicKey` | `from_public_key_der` |

伪代码（不落盘，仅供实现参考）：

```rust
let key: [u8;16] = rand::random();
let iv:  [u8;16] = rand::random();
let plain = serde_json::to_vec(payload)?;                 // 6 字段 JSON
let cipher = cbc::Encryptor::<Aes128>::new(&key.into(), &iv.into())
    .encrypt_padded_vec_mut::<Pkcs7>(&plain);             // 就是 body
let mut blob = [0u8;32]; blob[..16].copy_from_slice(&key); blob[16..].copy_from_slice(&iv);
let pubkey = RsaPublicKey::from_public_key_der(&base64::decode(PROD_KEY_B64)?)?;
let wrapped = pubkey.encrypt(&Oaep::new::<Sha256>(), &blob)?;   // 256 字节
let header = base64::encode(&wrapped);
// POST body = cipher, header x-request-encryption-key = header
```

**风险/不确定点：**
- 仍**必须**过 edge WAF（CloudFront 403/challenge）+ guest PoW。加密只是让 body 通过校验，
  不解决 UA/指纹/出口 IP 的拦截——commenter 1515asd 的 (d)「curl UA → 429、browser UA → 放行」
  与 §2 的 CloudFront 403 一致。
- 公钥常量需要随前端版本跟踪，硬编码在主 bundle（`eL.production`），DeepSeek 换钥需同步更新。
- 加密后仍需合法 `device_id`（数美指纹），否则 `biz_code=11`——这点旧文档已记录。

### 4.2 (b) 绕过加密：改用 Android identity？

commenter 1515asd 的说法：
- (a) web payload 被真加密 → 422；
- (b) **Android 路径仍 plaintext，能走到正常的 password 校验**；
- (c) 业务层未查 device，`users/current` 缺头也能答；
- (d) edge 按 UA 过滤，curl 默认 UA → 429，browser/android UA → 放行。

**机制上与前端代码一致的解释：** §3.1 的加密拦截器 `eD` 是**挂在 web 前端某个 app 实例
（`app: "chat" | "platform"`）上的**。它只处理该 app 自己发出的 fetch 请求。**原生 App 不发这段
JS**，所以 App 的 `/api/v0/users/login` 天然是明文——加密是「web 客户端侧」的要求，不是服务端
强制的 body 形态。这解释了「Android 仍 plaintext」。

对 `ds_core` 的具体含义：
- `EmulationProfile::OkHttp4_12` + UA `DeepSeek/… Android/…` + `client_platform=android` +
  `client_os=android` 已经是「自洽的安卓身份」（`client.rs:333-342`）。
- 当前 `login()` 本就发 plaintext，**如果上游对 Android 身份放行**，则**无需任何改动即可过**——
  只要把整套身份（UA/platform/os/emulation）切到 android 且自洽。
- 但这会**影响全链路**：headers（`Sec-Ch-Ua*` 是 Chrome 桌面专属）、HIF、后续 completion 的风控
  归因都会变成「安卓 App」。项目当前默认是 Chrome web 身份，专门为「对齐官方 Web」而设计
  （`docs/development.md`）。

**兼容 Android 的代价/风险：**
1. **只能改 login 不能只改一处**：`ClientIdentity` 是每 client 一份，若 login 用 android、completion
   用 web，会出现「同账号同 device_id 但 UA 来回变」的矛盾指纹，反而更可疑。
2. **device_id 语义**：web 的 `device_id` 是数美浏览器指纹（89 字符 base64），App 的是另一套设备标识；
   混用可能触发 `biz_code=11`。项目现有 `device_id` 多为浏览器指纹（`extract-device.py` 从浏览器抓）。
3. **Android 明文未必长期有效**：upstream 随时可能对 App 也上加密。属于「押注一条今天能走的路」。
4. **风控/禁言**：`docs/development.md` 反复强调「对齐真实客户端」。把 login 单独切 android 而
   chat 仍 web，属于不对称身份，可能提高风控敏感度。

**评估结论：** 作为**短期快速恢复**手段合理（改动小、纯配置/身份切换），但**不建议长期只改 login**；
若走这条，应把**整个账号生命周期**（login + completion + HIF）统一成 android 身份，并接受「App
侧随时可能也加密」的时效风险。理想是 A（crypto）+ 保持 web 身份。

### 4.3 (c) 浏览器扩展 / 抓 token

用真实浏览器登录，抓 `data.biz_data.user.token`，把 token 直接喂给 `ds_core`（跳过 login）。
- 优点：不碰 crypto、天然是合法 web 身份、token 直接用。
- 缺点：token 有 TTL，过期就要重新人肉登录；无法自动轮换/多账号；不适合无人值守的账号池。
- 适合「少量账号 + 人工维护」，不适合本项目「多账号池自动轮换」的定位。

---

## 5. 三个方案对比

| 维度 | A. 实现 crypto | B. 切 Android identity 跑 login | C. 浏览器扩展/抓 token |
|------|---------------|-------------------------------|----------------------|
| 改动面 | `client.rs` login 增加「加密 body + `x-request-encryption-key` 头」；新增 crypto 模块；常量公钥 | 主要改配置/身份（emulation+UA+platform+os=android），login 逻辑本身不变 | 不改 `ds_core`；外部工具抓 token 注入 |
| 新增依赖 | `rsa` + `sha2` + `aes` + `cbc` + `rand` + `base64`（+`pkcs8`） | 无（现有 `wreq` Emulation::OkHttp4_12 已在） | 无（外部浏览器） |
| 复杂度 | **中**（crypto 标准、可单测；难在跟踪公钥版本 + 仍要过 WAF/PoW） | **低**（若 Android 明文确实放行） | 低-中（工具链），但**运维高** |
| 保持 web 身份 | ✅（符合项目默认设计） | ❌（需全局切 android） | ✅ |
| 自动化/多账号 | ✅ | ✅ | ❌（token 人工过期） |
| 长期稳定性 | 高（跟公钥走） | **低**（App 随时也可能加密） | 低（token TTL） |
| 仍需解决 | CloudFront WAF + guest PoW(`x-ds-challenge-response`) + 合法 `device_id` | 同上 + 身份全局自洽 + `device_id` 语义 | token 注入与续期 |

---

## 6. 推荐

1. **主推 A（Rust 实现混合加密）**：算法已完全确定（AES-128-CBC/PKCS7 + RSA-OAEP-SHA256(key‖iv)，
   密钥进 `x-request-encryption-key` 头），用 `rsa`/`aes`/`cbc` 即可，可对拍 WebCrypto 做单测。
   同时把 login 补上 **guest PoW 头**（`x-ds-challenge-response`）——加密和 PoW 是两道**叠加**门槛，
   只做加密很可能仍被拒。公钥用 `eL.production`，抽成常量并标注来源版本号 `main.c918de4e90.js`。
2. **并行验证 B 作为应急开关**：给 `ds_core` 加一个「login 使用 android 明文身份」的实验开关
   （与现有 `EmulationProfile` 正交），双跑对比 422 率与禁言率。**不要在未全局统一身份前上线 B**，
   避免 web/android 混合指纹。
3. **C 仅作为兜底**：当 A/B 都被 upstream 封死、且账号量小时，用真实浏览器登录抓
   `data.biz_data.user.token` 手动注入，维持服务可用。

> 三个方案都**不能**替代对 edge WAF（CloudFront 403/challenge）与 `device_id` 合法性的处理——
> 这两点是登录能从「网络层」走到「业务层」的前提，与 body 加密是**不同层**的门槛。

---

## 附录 A：取证命令与命中（可复现）

```bash
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
    (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"

# 1) 主站：CloudFront 403 / index 202（WAF）
curl -s -o /dev/null -w "%{http_code}\n" -A "$UA" https://chat.deepseek.com/          # 403
curl -s -o /dev/null -w "%{http_code}\n" -A "$UA" https://chat.deepseek.com/index.html # 202

# 2) SPA 壳给出真实 CDN 与 bundle 名
curl -s -A "$UA" https://chat.deepseek.com/anything.js | grep -oE 'src="[^"]+\.js"'
#   -> fe-static.deepseek.com/chat/static/default-vendors.d1ba088e82.js
#   -> fe-static.deepseek.com/chat/static/main.c918de4e90.js

# 3) 拉主 bundle（1,666,571 B）并检索
curl -s -A "$UA" -o main.js https://fe-static.deepseek.com/chat/static/main.c918de4e90.js
grep -c "users/login" main.js            # 1
grep -c "JSON.stringify(e.json)" main.js # 1
# 关键区：NaiveInclude: "x-request-encryption-key" / "RSA-OAEP" / "AES-CBC" / "importKey(\"spki\"" 
```

## 附录 B：术语对照（代码里的混淆名 → 语义）

| 混淆名 | 语义 |
|--------|------|
| `eD(...)` | login/注册类 POST 的加密中间件（per-app） |
| `ej({publicKey,getCrypto})` | 返回加密器：AES-CBC + RSA-OAEP(key‖iv) |
| `eL`（局部）| `{development, production}` → base64(SPKI RSA-2048 公钥) |
| `eN` | 每个 app 的「需加密路径」白名单（chat=`/api/v0/users/*`） |
| `eR` | 需加密的 login/注册/验证码动作名列表 |
| `(0,ew.fg)` | guest PoW 头生成器（`x-ds-challenge-response`） |
| `x-request-encryption-key` | base64(RSA-OAEP(AES_key‖IV)) |

---

*本文件仅记录审计结论，未修改任何源码。所有引用的 URL 均已实际读取并描述其返回结果。*
