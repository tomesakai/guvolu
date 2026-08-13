"""请求签名（C-06、C-07）。

签名串 = timestamp + method + path + body，path 以 /v1 开头。
例外：PUT 与 DELETE /v1/ws-auth 的签名串不含 body（body 照发）。
该例外为 GMO 的不一致设计，已实测确认，唯一维护点在本模块。
"""
import hashlib
import hmac

# C-07 例外端点集合
_BODY_EXEMPT = frozenset({("PUT", "/v1/ws-auth"), ("DELETE", "/v1/ws-auth")})


def signature_text(timestamp_ms: str, method: str, path: str, body: str) -> str:
    """拼接签名串，应用 C-07 例外。"""
    if (method, path) in _BODY_EXEMPT:
        return timestamp_ms + method + path
    return timestamp_ms + method + path + body


def sign_request(
    secret: str, timestamp_ms: str, method: str, path: str, body: str
) -> str:
    """生成 HMAC-SHA256 十六进制签名。"""
    text = signature_text(timestamp_ms, method, path, body)
    return hmac.new(
        secret.encode("ascii"), text.encode("utf-8"), hashlib.sha256
    ).hexdigest()
