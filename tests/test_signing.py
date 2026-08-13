"""签名逻辑直接单测（C-15）。黄金向量本会话内独立算出。"""
from guvolu.api.signing import sign_request, signature_text

_SECRET = "testsecret"
_TS = "1700000000000"


def test_get_golden_vector() -> None:
    """GET 空体黄金向量。"""
    assert (
        sign_request(_SECRET, _TS, "GET", "/v1/account/assets", "")
        == "092117ad0f2facd0a5766da9f7598e0d2930a2fecc06b4f9b8868a2690941aff"
    )


def test_post_golden_vector() -> None:
    """POST 含体黄金向量。"""
    assert (
        sign_request(_SECRET, _TS, "POST", "/v1/order", '{"symbol":"BTC"}')
        == "04b5106c6f57d5f1b20ff1bd032c39bcb4be254de39ef893bc037b6dd838fb5e"
    )


def test_ws_auth_delete_excludes_body() -> None:
    """C-07 例外：DELETE ws-auth 签名不含体。"""
    with_body = sign_request(_SECRET, _TS, "DELETE", "/v1/ws-auth", '{"token":"x"}')
    without_body = sign_request(_SECRET, _TS, "DELETE", "/v1/ws-auth", "")
    assert with_body == without_body
    assert (
        with_body
        == "9cd2a01df97eb8b398240d3836c2b6703bd6f7e12638f53e690cb78a24ae8aea"
    )


def test_ws_auth_put_excludes_body() -> None:
    """C-07 例外：PUT ws-auth 签名不含体。"""
    assert signature_text(_TS, "PUT", "/v1/ws-auth", '{"token":"x"}') == (
        _TS + "PUT" + "/v1/ws-auth"
    )


def test_ws_auth_post_includes_body() -> None:
    """POST ws-auth 不在例外之列。"""
    assert signature_text(_TS, "POST", "/v1/ws-auth", "{}").endswith("{}")


def test_body_changes_signature() -> None:
    """普通端点体变更则签名变更。"""
    first = sign_request(_SECRET, _TS, "POST", "/v1/order", '{"a":1}')
    second = sign_request(_SECRET, _TS, "POST", "/v1/order", '{"a":2}')
    assert first != second
