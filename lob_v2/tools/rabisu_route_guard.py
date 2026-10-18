#!/usr/bin/env python3
"""Fail startup when a production collector could inherit the user's main proxy.

These loopback ports are the verified SSH forwards to the three Rabisu VPS hosts. This
checks configuration, not VPS identity; the deployment receipt verifies the
forwarding processes and actual sockets separately.
"""
import argparse
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lobv2.routing import plant_network

ROUTE_HOSTS = {
    "sun": {**dict.fromkeys((18090, 18091, 18092, 18460, 18461, 18462), "179.254.94.224"),
            **dict.fromkeys((18463, 18464), "208.92.225.172"),
            **dict.fromkeys((18465, 18466), "23.230.139.132")},
    "xue": dict.fromkeys((18090, 18091, 18092), "179.254.94.224"),
}
PORTS = {site: set(routes) for site, routes in ROUTE_HOSTS.items()}
PROXY_ENV = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")


def check_proxy(value, site):
    if not isinstance(value, str):
        raise ValueError("RABISU_REQUIRED: missing explicit proxy")
    parsed = urlsplit(value)
    if (parsed.scheme not in {"socks5", "socks5h"} or parsed.hostname != "127.0.0.1"
            or parsed.port not in PORTS[site] or parsed.username or parsed.password
            or parsed.path or parsed.query or parsed.fragment):
        raise ValueError("RABISU_REQUIRED: non-Rabisu route rejected")


def check_config(config, site):
    check_proxy(config.get("outbound_proxy"), site)
    lanes = config.get("lane_proxies")
    if not isinstance(lanes, list) or len(lanes) != config.get("lanes", 2):
        raise ValueError("RABISU_REQUIRED: explicit route required for every lane")
    for route in lanes:
        check_proxy(route, site)


def check_environment(environment, site):
    # HTTP clients prefer lower-case values in some versions. Check all six.
    for key in PROXY_ENV:
        check_proxy(environment.get(key), site)
    for key in ("NO_PROXY", "no_proxy"):
        bypass = {v.strip() for v in environment.get(key, "").split(",") if v.strip()}
        if not bypass <= {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("RABISU_REQUIRED: external proxy bypass rejected")


def check_pool(settings, site):
    pool = settings.get("vps_routes")
    if not pool:
        return
    if len(pool) != 3 or {row["host"] for row in pool} != {"179.254.94.224", "208.92.225.172", "23.230.139.132"}:
        raise ValueError("THREE_APPROVED_VPS_REQUIRED")
    for row in pool:
        for plant in ("P", "Q"):
            route = row["proxies"][plant]
            check_proxy(route, site)
            if ROUTE_HOSTS[site][urlsplit(route).port] != row["host"]:
                raise ValueError("VPS_HOST_PORT_MISMATCH")


def check_assignment(config, settings, instance, site):
    check_config(config, site)
    check_pool(settings, site)
    if settings.get("vps_routes"):
        role, cohort = instance.split("-", 1)
        expected = plant_network(settings, int(cohort), role)
        if any(config.get(key) != value for key, value in expected.items()):
            raise ValueError("THREE_VPS_ASSIGNMENT_MISMATCH")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", choices=PORTS, default="sun")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--instance")
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()
    check_environment(os.environ, args.site)
    if args.instance:
        instance = json.loads((args.root / "instances" / (args.instance + ".json")).read_text())
        if instance["role"] == "plant":
            settings = json.loads((args.root / "settings.json").read_text())
            check_assignment(instance["config"], settings, args.instance, args.site)
    elif args.config:
        check_config(json.loads(args.config.read_text()), args.site)
    elif args.root:
        settings = json.loads((args.root / "settings.json").read_text())
        check_pool(settings, args.site)
        for plant in ("P", "Q"):
            check_config({"outbound_proxy": settings["proxies"][plant],
                          "lane_proxies": settings["lane_proxies"][plant],
                          "lanes": settings["node_defaults"]["lanes"]}, args.site)
    else:
        parser.error("--root or --config is required")
    print("RABISU_ROUTE_CONFIG_OK")


if __name__ == "__main__":
    main()
