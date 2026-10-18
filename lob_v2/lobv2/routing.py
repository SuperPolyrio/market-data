"""Deterministic plant/lane routes across the operator's three VPS hosts."""


def plant_network(settings, cohort_index, plant):
    pool = settings.get("vps_routes")
    if not pool:
        result = {"outbound_proxy": settings.get("proxies", {}).get(plant, "env")}
        if plant in settings.get("lane_proxies", {}):
            result["lane_proxies"] = settings["lane_proxies"][plant]
        return result
    if plant not in ("P", "Q") or len(pool) != 3 or len({r["host"] for r in pool}) != 3:
        raise ValueError("THREE_DISTINCT_VPS_REQUIRED")
    shifts = (0, 1) if plant == "P" else (2, 0)
    selected = [pool[(cohort_index + offset) % 3] for offset in shifts]
    proxies = [row["proxies"][plant] for row in selected]
    return {"outbound_proxy": proxies[0], "lane_proxies": proxies,
            "lane_provider_domains": ["rabisu-" + row["host"] for row in selected],
            "routing_policy_version": "three_vps_rotating_cohort_v1"}
