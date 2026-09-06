from market_data.config import ClickHouseConfig, PostgresConfig, polygon_rpc_urls, split_urls
from market_data.rpc import RpcClient


def test_split_urls_deduplicates_without_reordering() -> None:
    assert split_urls("http://a, http://b,http://a") == ("http://a", "http://b")


def test_storage_defaults_match_existing_runtime(monkeypatch) -> None:
    for key in (
        "POLYDATA_POSTGRES_HOST",
        "POLYDATA_POSTGRES_PORT",
        "POLYDATA_POSTGRES_USER",
        "POLYDATA_POSTGRES_DATABASE",
        "POLYDATA_ORDERFILLED_CLICKHOUSE_DATABASE",
    ):
        monkeypatch.delenv(key, raising=False)
    assert PostgresConfig.from_env().port == 45432
    assert PostgresConfig.from_env().database == "poly_data_core"
    assert ClickHouseConfig.from_env().database == "poly_orderfilled"


def test_role_rpc_fallback_is_retained_after_primary(monkeypatch) -> None:
    for key in (
        "POLYDATA_ORDERFILLED_RPC_URLS",
        "POLYDATA_POLYGON_RPC_URLS",
        "POLYDATA_POLYGON_FALLBACK_RPC_URLS",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("POLYMARKET_RPC_URL", "http://primary")
    monkeypatch.setenv("POLYDATA_ORDERFILLED_FALLBACK_RPC_URL", "http://fallback")
    assert polygon_rpc_urls("orderfilled") == ("http://primary", "http://fallback")


def test_rpc_batch_restores_requested_order() -> None:
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {"jsonrpc": "2.0", "id": 2, "result": {"number": "0x2"}},
                {"jsonrpc": "2.0", "id": 1, "result": {"number": "0x1"}},
            ]

    class Session:
        def post(self, url, *, json, timeout):
            assert [item["params"][0] for item in json] == ["0x1", "0x2"]
            return Response()

    rpc = RpcClient(("http://rpc",))
    rpc.session = Session()

    assert rpc.blocks([1, 2]) == [{"number": "0x1"}, {"number": "0x2"}]
