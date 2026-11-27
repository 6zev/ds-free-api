#!/usr/bin/env python3
"""假上游：模拟 DeepSeek 后端的登录 / 建会话 / PoW / completion SSE。

用途：**零账号流量**地验证请求形态（会话复用、增量 prompt、工具注入、身份头等）。
每次 `/api/v0/chat/completion` 的请求体都会追加写入记录文件，供断言或人工比对。

用法：
    python3 mock_upstream.py <port> <记录文件> [--wasm-port-file <path>]

PoW 说明（2026-10-07 实测）：`challenge` 与 `salt`/`expire_at` 之间存在可验证关系，
**伪造的 challenge 永远 no solution**。这里直接复用真实抓包的样例四元组
（`ds_core/raw-api-reference.md` §3），wasm 不校验 `expire_at` 是否过期。
"""
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8098
LOG = sys.argv[2] if len(sys.argv) > 2 else "/tmp/mock_upstream.jsonl"

# 真实下发的样例（salt / expire_at / challenge / signature 必须配套）
REAL_CHALLENGE = {
    "algorithm": "DeepSeekHashV1",
    "challenge": "7ffc9d19b6eed96a6fca68f8ffe30ee61035d4959e4180f187bf85b356016a96",
    "salt": "3bde54628ea8413fee87",
    "signature": "ce4678cf7a1290c2a7ac88c4195a5b8497e5fc4b0e8044e804f5a6f3af6fe462",
    "difficulty": 144000,
    "expire_after": 300000,
    "expire_at": 1775380966945,
}

SESSIONS = {"n": 0}
# 续写轮标记：`MOCK_THINK_ONLY_ONCE` 用它区分「首轮」和「续写轮」
CONTINUED = {"n": False}
RESPONSE_MSG_ID = {"n": 1000}


def envelope(biz_data):
    return {
        "code": 0,
        "msg": "",
        "data": {"biz_code": 0, "biz_msg": "", "biz_data": biz_data},
    }


def envelope_null():
    """biz_data = null（delete_session 等返回 unit 的端点）"""
    return {"code": 0, "msg": "", "data": {"biz_code": 0, "biz_msg": "", "biz_data": None}}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sse_body(self, text="好的"):
        rid = RESPONSE_MSG_ID["n"] = RESPONSE_MSG_ID["n"] + 1
        # `MOCK_THINK_ONLY=1`：复现上游「THINK-only 短流」（3 帧后直接 EOF）。
        # 这是「零正文截断」的回归样本：客户端必须拿到 length / max_tokens /
        # incomplete，而不是 stop / end_turn / completed。
        think_only = os.environ.get("MOCK_THINK_ONLY") == "1"
        if os.environ.get("MOCK_THINK_ONLY_ONCE") == "1" and CONTINUED["n"]:
            think_only = False
        # `MOCK_CLOSE_AUTO_RESUME=1`：只发 THINK + FINISHED，`event: close`
        # 带 `{"auto_resume":true}`。复现「想了很久、上游说还要继续、然后断流」，
        # 客户端必须无视 Complete 判定、再续一轮，并且**真的拿到**续写正文。
        close_auto_resume = (
            os.environ.get("MOCK_CLOSE_AUTO_RESUME") == "1" and not CONTINUED["n"]
        )
        if close_auto_resume:
            frames = [
                "event: ready\ndata: "
                + json.dumps({"request_message_id": rid - 1, "response_message_id": rid})
                + "\n\n",
                'event: update_session\ndata: {"p":"response/status","o":"SET","v":"WIP"}\n\n',
                'data: {"p":"response/fragments","o":"APPEND","v":[{"type":"THINK","content":"We"}]}\n\n',
                'data: {"p":"response/status","o":"SET","v":"FINISHED"}\n\n',
                'event: close\ndata: {"auto_resume":true}\n\n',
            ]
            return rid, "".join(frames).encode()
        if think_only:
            frames = [
                "event: ready\ndata: "
                + json.dumps({"request_message_id": rid - 1, "response_message_id": rid})
                + "\n\n",
                'event: update_session\ndata: {"p":"response/status","o":"SET","v":"WIP"}\n\n',
                'data: {"p":"response/fragments","o":"APPEND","v":[{"type":"THINK","content":"We"}]}\n\n',
            ]
            return rid, "".join(frames).encode()
        frames = [
            "event: ready\ndata: "
            + json.dumps({"request_message_id": rid - 1, "response_message_id": rid})
            + "\n\n",
            'event: update_session\ndata: {"p":"response/status","o":"SET","v":"WIP"}\n\n',
            'data: {"p":"response/fragments","o":"APPEND","v":[{"type":"RESPONSE","content":"'
            + text
            + '"}]}\n\n',
            'data: {"p":"response/status","o":"SET","v":"FINISHED"}\n\n',
            "event: close\ndata: {}\n\n",
        ]
        return rid, "".join(frames).encode()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path.endswith("/client/settings"):
            return self._json(
                envelope(
                    {
                        "version": 88,
                        "settings": {
                            "model_types": [
                                {
                                    "model_type": "default",
                                    "name": "DeepSeek",
                                    "enabled": True,
                                    "switchable": True,
                                    "input_character_limit": 2621440,
                                }
                            ]
                        },
                    }
                )
            )
        return self._json(envelope({}))

    def do_POST(self):
        path = self.path.split("?")[0]
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw) if raw else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            # 上游 2026-10 起 login body 是 AES-128-CBC 密文；mock 不解密，
            # 只要拿得到请求就当作合法登录（`_raw` 仅用于调试）。
            body = {"_raw": raw.hex()[:64]}

        if path.endswith("/users/login"):
            return self._json(
                envelope(
                    {
                        "code": 0,
                        "msg": "",
                        "user": {
                            "id": "mock-user",
                            "token": "MOCK-TOKEN",
                            "email": body.get("email"),
                            "chat": {"is_muted": 0},
                        },
                    }
                )
            )
        if path.endswith("/users/auth_token/check_device"):
            return self._json(envelope({"rotate": None}))
        if path.endswith("/chat_session/create"):
            SESSIONS["n"] += 1
            return self._json(
                envelope({"chat_session": {"id": f"mock-session-{SESSIONS['n']}"}})
            )
        if path.endswith("/chat_session/delete"):
            return self._json(envelope_null())
        if path.endswith("/chat/create_pow_challenge"):
            challenge = dict(REAL_CHALLENGE)
            challenge["target_path"] = body.get("target_path", "/api/v0/chat/completion")
            return self._json(envelope({"challenge": challenge}))
        if path.endswith("/chat/completion"):
            # 续写轮带 `parent_message_id`：`MOCK_THINK_ONLY_ONCE` 靠它区分首轮
            if body.get("parent_message_id"):
                CONTINUED["n"] = True
            rid, sse = self._sse_body()
            with open(LOG, "a") as f:
                f.write(
                    json.dumps(
                        {
                            "t": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "path": path,
                            "headers": {
                                "x-hif-leim": self.headers.get("x-hif-leim"),
                                "x-hif-dliq": self.headers.get("x-hif-dliq"),
                                "x-client-platform": self.headers.get("x-client-platform"),
                                "sec-fetch-dest": self.headers.get("sec-fetch-dest"),
                                "sec-ch-ua-platform": self.headers.get("sec-ch-ua-platform"),
                                "accept": self.headers.get("accept"),
                                "referer": self.headers.get("referer"),
                                "sec-fetch-site": self.headers.get("sec-fetch-site"),
                            },
                            "body": body,
                            "response_message_id": rid,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(sse)))
            self.end_headers()
            self.wfile.write(sse)
            return
        return self._json(envelope({}))


if __name__ == "__main__":
    host = os.environ.get("MOCK_HOST", "127.0.0.1")
    ThreadingHTTPServer((host, PORT), Handler).serve_forever()
