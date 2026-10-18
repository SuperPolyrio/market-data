import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location("route_guard", Path(__file__).parents[1] / "tools/rabisu_route_guard.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


@pytest.mark.parametrize("proxy", ["env", "direct", None, "socks5h://127.0.0.1:25172", "http://127.0.0.1:18460", "socks5h://remote:18460"])
def test_rejects_main_proxy_and_implicit_fallback(proxy):
    with pytest.raises(ValueError, match="RABISU_REQUIRED"):
        guard.check_proxy(proxy, "sun")


def test_checks_all_lanes_even_with_safe_default():
    config = {"outbound_proxy": "socks5h://127.0.0.1:18460", "lanes": 2,
              "lane_proxies": ["socks5h://127.0.0.1:18460", "socks5h://127.0.0.1:18461"]}
    guard.check_config(config, "sun")
    config["lane_proxies"][1] = "socks5h://127.0.0.1:25172"
    with pytest.raises(ValueError):
        guard.check_config(config, "sun")


def test_accepts_verified_direct_and_xue_forwarded_rabisu_routes():
    guard.check_config({"outbound_proxy": "socks5h://127.0.0.1:18090", "lanes": 2,
                        "lane_proxies": ["socks5h://127.0.0.1:18090", "socks5h://127.0.0.1:18460"]}, "sun")


def test_rejects_lowercase_environment_override_and_external_bypass():
    env = dict.fromkeys(guard.PROXY_ENV, "socks5://127.0.0.1:18462")
    env.update(NO_PROXY="localhost,127.0.0.1,::1", no_proxy="localhost,127.0.0.1,::1")
    guard.check_environment(env, "sun")
    with pytest.raises(ValueError):
        guard.check_environment(env | {"https_proxy": "http://127.0.0.1:25172"}, "sun")
    with pytest.raises(ValueError):
        guard.check_environment(env | {"NO_PROXY": "*"}, "sun")
