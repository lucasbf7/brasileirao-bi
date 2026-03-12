from __future__ import annotations

import argparse
from typing import List, Optional, Dict, Callable

from brasileirao_bi.bq.client import get_bq_client
from brasileirao_bi.etl.transforms.mart import create_matches_enriched, create_fact_matches_running
from brasileirao_bi.etl.transforms.opponent import create_agg_opponent_score_23_25
from brasileirao_bi.etl.transforms.hist import create_agg_hist_local_23_25, create_agg_hist_adv_local_23_25
from brasileirao_bi.etl.transforms.form import create_agg_form_2026
from brasileirao_bi.etl.transforms.future import create_dim_future_matches_enriched
from brasileirao_bi.etl.transforms.projection import create_fact_projection_matches, create_fact_projection_micro


def parse_int_list(csv: Optional[str]) -> List[int]:
    if not csv:
        return []
    parts = [p.strip() for p in csv.split(",") if p.strip()]
    out: List[int] = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError as e:
            raise ValueError(f"Invalid integer in list: '{p}'") from e
    return out


def uniq_ints(values: List[int]) -> List[int]:
    seen = set()
    out: List[int] = []
    for v in values:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out


StepFunc = Callable[..., None]

STEP_FUNCS: Dict[str, StepFunc] = {
    "matches_enriched": lambda c, **kw: create_matches_enriched(c),
    "fact_matches_running": lambda c, **kw: create_fact_matches_running(c),

    "agg_hist_local_23_25": lambda c, **kw: create_agg_hist_local_23_25(c),
    "agg_hist_adv_local_23_25": lambda c, **kw: create_agg_hist_adv_local_23_25(c),
    "agg_form_2026": lambda c, **kw: create_agg_form_2026(c),

    # Score de dificuldade por adversário (23-25) com shrinkage
    "agg_opponent_score_23_25": lambda c, **kw: create_agg_opponent_score_23_25(
        c,
        k_prior=kw.get("k_prior", 3),
    ),

    "dim_future_matches_enriched": lambda c, **kw: create_dim_future_matches_enriched(
        c,
        min_hist_games=kw.get("min_hist_games", 3),
        w_hist_fallback=kw.get("w_hist_fallback", 0.2),
        w_form_fallback=kw.get("w_form_fallback", 0.8),
    ),

    "fact_projection_matches": lambda c, **kw: create_fact_projection_matches(
        c,
        alpha_difficulty=kw.get("alpha_difficulty", 0.35),
    ),
    "fact_projection_micro": lambda c, **kw: create_fact_projection_micro(c),
}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build Brasileirão-BI BigQuery tables. No side effects unless you pass --all or --steps."
    )
    parser.add_argument("--all", action="store_true", help="Run all steps in the correct order.")
    parser.add_argument(
        "--steps",
        type=str,
        default="",
        help=f"Comma-separated list of steps. Options: {', '.join(STEP_FUNCS.keys())}",
    )

    # Params
    parser.add_argument(
        "--min-hist-games",
        type=int,
        default=3,
        help="Min games to treat adv_local head-to-head as STRONG history.",
    )
    parser.add_argument(
        "--w-hist-fallback",
        type=float,
        default=0.2,
        help="Weight for fallback (weak) historical PPJ component.",
    )
    parser.add_argument(
        "--w-form-fallback",
        type=float,
        default=0.8,
        help="Weight for form PPJ component when using fallback history.",
    )
    parser.add_argument("--k-prior", type=int, default=3, help="Shrinkage prior games for opponent score (min sample).")
    parser.add_argument(
        "--alpha-difficulty",
        type=float,
        default=0.35,
        help="Difficulty adjustment intensity for expected points.",
    )

    # Params: head-to-head (Freguês/Carrasco) por time vs adversário
    parser.add_argument(
        "--min-games-hist",
        type=int,
        default=3,
        help="Min games for head-to-head historical (23-25).",
    )
    parser.add_argument(
        "--min-games-2026",
        type=int,
        default=1,
        help="Min games for head-to-head season 2026.",
    )

    args = parser.parse_args(argv)

    # No side effects by default
    if not args.all and not args.steps.strip():
        parser.print_help()
        return 0

    # Weights: normalize
    min_hist_games = int(args.min_hist_games)
    if min_hist_games < 1:
        raise ValueError("--min-hist-games must be >= 1")

    w_hist_fallback = float(args.w_hist_fallback)
    w_form_fallback = float(args.w_form_fallback)
    if w_hist_fallback < 0 or w_form_fallback < 0 or (w_hist_fallback + w_form_fallback) == 0:
        raise ValueError("Invalid fallback weights. Use non-negative --w-hist-fallback/--w-form-fallback and not both zero.")
    s_fb = w_hist_fallback + w_form_fallback
    w_hist_fallback = w_hist_fallback / s_fb
    w_form_fallback = w_form_fallback / s_fb

    k_prior = int(args.k_prior)
    if k_prior < 0:
        raise ValueError("--k-prior must be >= 0")

    alpha_difficulty = float(args.alpha_difficulty)
    if alpha_difficulty < 0:
        raise ValueError("--alpha-difficulty must be >= 0")

    min_games_hist = int(args.min_games_hist)
    if min_games_hist < 1:
        raise ValueError("--min-games-hist must be >= 1")

    min_games_2026 = int(args.min_games_2026)
    if min_games_2026 < 1:
        raise ValueError("--min-games-2026 must be >= 1")
    
    print("\n=== Criando camada TRANSFORM ===\n")

    if args.all:
        ordered = [
            "matches_enriched",
            "fact_matches_running",

            "agg_hist_local_23_25",
            "agg_hist_adv_local_23_25",
            "agg_form_2026",
            "agg_opponent_score_23_25",
            "dim_future_matches_enriched",
            "fact_projection_matches",
            "fact_projection_micro",
        ]
    else:
        requested = [s.strip() for s in args.steps.split(",") if s.strip()]
        unknown = [s for s in requested if s not in STEP_FUNCS]
        if unknown:
            raise ValueError(f"Unknown steps: {unknown}. Valid: {list(STEP_FUNCS.keys())}")
        ordered = requested

    client = get_bq_client()
    for step in ordered:
        STEP_FUNCS[step](
            client,
            min_hist_games=min_hist_games,
            w_hist_fallback=w_hist_fallback,
            w_form_fallback=w_form_fallback,
            k_prior=k_prior,
            alpha_difficulty=alpha_difficulty,
            min_games_hist=min_games_hist,
            min_games_2026=min_games_2026,
        )

    print("\n✅ Camada transform criada com sucesso\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())