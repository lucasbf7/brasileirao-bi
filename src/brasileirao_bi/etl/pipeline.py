from __future__ import annotations

import argparse

from brasileirao_bi.bq.client import get_bq_client
from brasileirao_bi.etl.extract import extract_raw
from brasileirao_bi.etl.load_bq import load_raw_tables
from brasileirao_bi.etl.curated import run_curated_layer
from brasileirao_bi.etl.transform import main as transform_main
from brasileirao_bi.etl.monte_carlo import run_monte_carlo
from brasileirao_bi.etl.views import run_views_layer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extract", action="store_true")
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--curated", action="store_true")
    parser.add_argument("--transform", action="store_true")
    parser.add_argument("--monte_carlo", action="store_true")
    parser.add_argument("--views", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args(argv)

    do_all = args.all or not (
        args.extract
        or args.load
        or args.curated
        or args.transform
        or args.monte_carlo
        or args.views
    )

    if do_all or args.extract:
        extract_raw()

    if do_all or args.load:
        load_raw_tables()

    if do_all or args.curated:
        client = get_bq_client()
        run_curated_layer(client)

    if do_all or args.transform:
        transform_main(["--all"])

    if do_all or args.monte_carlo:
        client = get_bq_client()
        run_monte_carlo(client)

    if do_all or args.views:
        client = get_bq_client()
        run_views_layer(client)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())