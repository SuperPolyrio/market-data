from collections import Counter

import pytest

from lobv2.routing import plant_network


def settings():
    return {"vps_routes": [{"host": host, "proxies": {"P": f"socks5h://127.0.0.1:{port}",
                                                    "Q": f"socks5h://127.0.0.1:{port+1}"}}
                            for host, port in [("208.92.225.172", 18463),
                                               ("179.254.94.224", 18090),
                                               ("23.230.139.132", 18465)]]}


def test_each_cohort_survives_one_vps_loss_and_load_rotates_evenly():
    counts = Counter()
    for cohort in range(127):
        routes = [plant_network(settings(), cohort, role) for role in ("P", "Q")]
        domains = [route["lane_provider_domains"] for route in routes]
        assert all(len(set(row)) == 2 for row in domains)
        assert domains[0][0] != domains[1][0]
        assert len(set(domains[0] + domains[1])) == 3
        for dead in set(domains[0] + domains[1]):
            assert all(any(host != dead for host in row) for row in domains)
        counts.update(domains[0] + domains[1])
    assert max(counts.values()) - min(counts.values()) == 1


def test_two_logical_labels_cannot_hide_a_duplicate_vps():
    config = settings()
    config["vps_routes"][2]["host"] = config["vps_routes"][0]["host"]
    with pytest.raises(ValueError, match="THREE_DISTINCT_VPS_REQUIRED"):
        plant_network(config, 0, "P")


def test_legacy_routing_keeps_existing_defaults():
    assert plant_network({"proxies": {"P": "p"}, "lane_proxies": {"P": ["a", "b"]}}, 5, "P") == {
        "outbound_proxy": "p", "lane_proxies": ["a", "b"]}
