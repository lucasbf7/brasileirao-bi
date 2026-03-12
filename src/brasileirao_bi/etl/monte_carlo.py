from __future__ import annotations

import numpy as np
import pandas as pd
from google.cloud import bigquery

from brasileirao_bi.bq.client import table_id


SEASON = 2026
DEFAULT_N_SIMS = 10000
DEFAULT_SEED = 42


def fetch_current_table(client: bigquery.Client, season: int = SEASON) -> pd.DataFrame:
    query = f"""
    SELECT
      season,
      team_id,
      team_name,
      team_short_name,
      team_abbr,
      points,
      wins,
      goal_diff,
      goals_for
    FROM `{table_id(f"agg_team_season_{season}")}`
    WHERE season = {season}
    """
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def fetch_remaining_matches(client: bigquery.Client, season: int = SEASON) -> pd.DataFrame:
    query = f"""
    SELECT
      season,
      match_id,
      matchday,
      match_date,
      home_team_id,
      home_team_name,
      home_team_short_name,
      home_team_abbr,
      away_team_id,
      away_team_name,
      away_team_short_name,
      away_team_abbr,
      home_expected_result,
      away_expected_result,
      home_expected_points,
      away_expected_points
    FROM `{table_id(f"vw_monte_carlo_matches_{season}")}`
    WHERE season = {season}
    """
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def result_to_probabilities(
    home_expected_result: str | None,
    away_expected_result: str | None,
) -> tuple[float, float, float]:
    """
    Retorna probabilidades em ordem:
    (home_win, draw, away_win)
    """
    h = (home_expected_result or "").strip().lower()
    a = (away_expected_result or "").strip().lower()

    if h == "vitória" and a == "derrota":
        return (0.60, 0.23, 0.17)

    if h == "derrota" and a == "vitória":
        return (0.17, 0.23, 0.60)

    if h == "empate" and a == "empate":
        return (0.30, 0.40, 0.30)

    return (0.33, 0.34, 0.33)


def simulate_match_points(
    home_probs: tuple[float, float, float],
    rng: np.random.Generator,
) -> tuple[int, int]:
    """
    Sorteia resultado da partida e retorna pontos:
    (home_points, away_points)
    """
    home_win, draw, away_win = home_probs
    outcome = rng.choice(["H", "D", "A"], p=[home_win, draw, away_win])

    if outcome == "H":
        return 3, 0
    if outcome == "D":
        return 1, 1
    return 0, 3


def _prepare_team_arrays(current_table: pd.DataFrame):
    """
    Prepara arrays base e metadados dos times.
    """
    teams = (
        current_table[
            [
                "team_id",
                "team_name",
                "team_short_name",
                "team_abbr",
                "points",
                "wins",
                "goal_diff",
                "goals_for",
            ]
        ]
        .drop_duplicates(subset=["team_id"])
        .sort_values("team_id")
        .reset_index(drop=True)
    )

    team_ids = teams["team_id"].to_numpy(dtype=np.int64)
    team_names = teams["team_name"].to_numpy(dtype=object)
    team_short_names = teams["team_short_name"].to_numpy(dtype=object)
    team_abbrs = teams["team_abbr"].to_numpy(dtype=object)

    base_points = teams["points"].to_numpy(dtype=np.int64)
    base_wins = teams["wins"].to_numpy(dtype=np.int64)
    base_goal_diff = teams["goal_diff"].to_numpy(dtype=np.int64)
    base_goals_for = teams["goals_for"].to_numpy(dtype=np.int64)

    team_idx = {int(team_id): idx for idx, team_id in enumerate(team_ids)}

    return {
        "team_ids": team_ids,
        "team_names": team_names,
        "team_short_names": team_short_names,
        "team_abbrs": team_abbrs,
        "base_points": base_points,
        "base_wins": base_wins,
        "base_goal_diff": base_goal_diff,
        "base_goals_for": base_goals_for,
        "team_idx": team_idx,
    }


def _prepare_matches(remaining_matches: pd.DataFrame, team_idx: dict[int, int]):
    """
    Pré-processa os jogos restantes em arrays simples para acelerar o loop.
    """
    home_indices: list[int] = []
    away_indices: list[int] = []
    probs_list: list[tuple[float, float, float]] = []

    for match in remaining_matches.itertuples(index=False):
        home_team_id = int(match.home_team_id)
        away_team_id = int(match.away_team_id)

        if home_team_id not in team_idx or away_team_id not in team_idx:
            continue

        home_indices.append(team_idx[home_team_id])
        away_indices.append(team_idx[away_team_id])
        probs_list.append(
            result_to_probabilities(
                match.home_expected_result,
                match.away_expected_result,
            )
        )

    return (
        np.array(home_indices, dtype=np.int64),
        np.array(away_indices, dtype=np.int64),
        probs_list,
    )


def _rank_positions(
    points: np.ndarray,
    wins: np.ndarray,
    goal_diff: np.ndarray,
    goals_for: np.ndarray,
    team_ids: np.ndarray,
) -> np.ndarray:
    """
    Retorna um array de posições finais (1..n) alinhado ao índice original do time.
    """
    # np.lexsort ordena pela última chave como primária.
    # Para ordem DESC nos critérios numéricos, uso valores negativos.
    order = np.lexsort(
        (
            team_ids,          # ASC
            -goals_for,        # DESC
            -goal_diff,        # DESC
            -wins,             # DESC
            -points,           # DESC
        )
    )

    positions = np.empty(len(team_ids), dtype=np.int64)
    positions[order] = np.arange(1, len(team_ids) + 1, dtype=np.int64)
    return positions


def run_simulations(
    current_table: pd.DataFrame,
    remaining_matches: pd.DataFrame,
    n_sims: int = DEFAULT_N_SIMS,
    seed: int = DEFAULT_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)

    team_data = _prepare_team_arrays(current_table)
    team_ids = team_data["team_ids"]
    team_names = team_data["team_names"]
    team_short_names = team_data["team_short_names"]
    team_abbrs = team_data["team_abbrs"]

    base_points = team_data["base_points"]
    base_wins = team_data["base_wins"]
    base_goal_diff = team_data["base_goal_diff"]
    base_goals_for = team_data["base_goals_for"]
    team_idx = team_data["team_idx"]

    home_idx_arr, away_idx_arr, probs_list = _prepare_matches(remaining_matches, team_idx)

    n_teams = len(team_ids)

    # Acumuladores
    sum_points = np.zeros(n_teams, dtype=np.float64)
    sum_positions = np.zeros(n_teams, dtype=np.float64)

    count_title = np.zeros(n_teams, dtype=np.int64)
    count_g4 = np.zeros(n_teams, dtype=np.int64)
    count_g6 = np.zeros(n_teams, dtype=np.int64)
    count_sulamericana = np.zeros(n_teams, dtype=np.int64)
    count_rebaixamento = np.zeros(n_teams, dtype=np.int64)

    # posição 1..n_teams -> coluna 0..n_teams-1
    position_counts = np.zeros((n_teams, n_teams), dtype=np.int64)

    for sim in range(1, n_sims + 1):
        if sim % 500 == 0:
            print(f"Simulação {sim}/{n_sims}")

        points = base_points.copy()
        wins = base_wins.copy()

        # Mantidos congelados
        goal_diff = base_goal_diff.copy()
        goals_for = base_goals_for.copy()

        for home_idx, away_idx, probs in zip(home_idx_arr, away_idx_arr, probs_list):
            home_points, away_points = simulate_match_points(probs, rng)

            points[home_idx] += home_points
            points[away_idx] += away_points

            if home_points == 3:
                wins[home_idx] += 1
            elif away_points == 3:
                wins[away_idx] += 1

        final_positions = _rank_positions(points, wins, goal_diff, goals_for, team_ids)

        sum_points += points
        sum_positions += final_positions

        count_title += (final_positions == 1)
        count_g4 += (final_positions <= 4)
        count_g6 += (final_positions <= 6)
        count_sulamericana += ((final_positions >= 7) & (final_positions <= 12))
        count_rebaixamento += (final_positions >= 17)

        # acumula distribuição de posições
        for team_i, pos in enumerate(final_positions):
            position_counts[team_i, pos - 1] += 1

    summary = pd.DataFrame(
        {
            "team_id": team_ids,
            "team_name": team_names,
            "team_short_name": team_short_names,
            "team_abbr": team_abbrs,
            "avg_final_points": sum_points / n_sims,
            "avg_final_position": sum_positions / n_sims,
            "prob_titulo": count_title / n_sims,
            "prob_g4": count_g4 / n_sims,
            "prob_g6": count_g6 / n_sims,
            "prob_sulamericana": count_sulamericana / n_sims,
            "prob_rebaixamento": count_rebaixamento / n_sims,
            "season": SEASON,
            "n_sims": n_sims,
        }
    )

    distribution_rows: list[dict] = []
    for team_i in range(n_teams):
        for pos in range(1, n_teams + 1):
            sim_count = int(position_counts[team_i, pos - 1])
            if sim_count == 0:
                continue

            distribution_rows.append(
                {
                    "team_id": int(team_ids[team_i]),
                    "team_name": team_names[team_i],
                    "team_short_name": team_short_names[team_i],
                    "team_abbr": team_abbrs[team_i],
                    "final_position": pos,
                    "sim_count": sim_count,
                    "sim_pct": sim_count / n_sims,
                    "season": SEASON,
                }
            )

    distribution = pd.DataFrame(distribution_rows)

    # Marca a(s) posição(ões) com maior probabilidade por time
    distribution["max_sim_pct"] = distribution.groupby("team_id")["sim_pct"].transform("max")
    distribution["is_peak_position"] = (distribution["sim_pct"] == distribution["max_sim_pct"]).astype(int)
    distribution = distribution.drop(columns=["max_sim_pct"])

    # Adiciona ao summary a posição mais provável e sua probabilidade
    # Em caso de empate, mantém a menor posição final
    peak_positions = (
        distribution.loc[
            distribution["is_peak_position"] == 1,
            ["team_id", "final_position", "sim_pct"],
        ]
        .sort_values(["team_id", "final_position"])
        .drop_duplicates(subset=["team_id"], keep="first")
        .rename(
            columns={
                "final_position": "most_likely_position",
                "sim_pct": "most_likely_position_pct",
            }
        )
    )

    summary = summary.merge(peak_positions, on="team_id", how="left")

    return summary, distribution


def upload_dataframe(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_name: str,
) -> None:
    destination = table_id(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, destination, job_config=job_config).result()
    print(f"OK: {table_name}")


def run_monte_carlo(
    client: bigquery.Client,
    season: int = SEASON,
    n_sims: int = DEFAULT_N_SIMS,
    seed: int = DEFAULT_SEED,
) -> None:
    print("\n=== Rodando Monte Carlo ===\n")

    print("Buscando tabela atual...")
    current_table = fetch_current_table(client, season)

    print("Buscando jogos restantes...")
    remaining_matches = fetch_remaining_matches(client, season)

    print("Rodando simulações...")
    summary, distribution = run_simulations(
        current_table=current_table,
        remaining_matches=remaining_matches,
        n_sims=n_sims,
        seed=seed,
    )

    summary["season"] = season
    distribution["season"] = season

    upload_dataframe(client, summary, f"monte_carlo_results_{season}")
    upload_dataframe(client, distribution, f"monte_carlo_position_distribution_{season}")

    print("\n✅ Monte Carlo finalizado com sucesso\n")