"""One CLI for Market, OrderFilled and Oracle acquisition."""

from __future__ import annotations

import argparse
import sys


def _placeholder(_args: argparse.Namespace) -> int:
    return 0


def build_parser(load_domain: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="market-data")
    domains = parser.add_subparsers(dest="domain", required=True)
    market = domains.add_parser("market", help="Gamma canonical markets")
    orderfilled = domains.add_parser("orderfilled", help="Polygon OrderFilled BUY/SELL")
    oracle = domains.add_parser("oracle", help="UMA and UpDown oracle events")
    if load_domain == "market":
        from market_data.market import add_cli

        add_cli(market)
    elif load_domain == "orderfilled":
        from market_data.orderfilled import add_cli

        add_cli(orderfilled)
    elif load_domain == "oracle":
        from market_data.oracle import add_cli

        add_cli(oracle)
    else:
        market.set_defaults(handler=_placeholder)
        orderfilled.set_defaults(handler=_placeholder)
        oracle.set_defaults(handler=_placeholder)
    return parser


def main(argv: list[str] | None = None) -> None:
    actual = list(sys.argv[1:] if argv is None else argv)
    domain = actual[0] if actual else None
    args = build_parser(domain).parse_args(actual)
    raise SystemExit(args.handler(args) or 0)
