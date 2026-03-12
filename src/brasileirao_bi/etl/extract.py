# src/brasileirao_bi/etl/extract.py
import os
import json
import time
from pathlib import Path
from typing import Optional, Dict

import requests
from dotenv import load_dotenv

BASE_URL = "https://api.football-data.org/v4"
RAW_DIR = Path("data/raw")
DEFAULT_SEASONS = [2023, 2024, 2025, 2026]
DEFAULT_COMPETITION = "BSA"


def get_token() -> str:
    load_dotenv()
    token = os.getenv("FOOTBALL_DATA_TOKEN")
    if not token:
        raise RuntimeError("FOOTBALL_DATA_TOKEN não encontrado. Confira seu .env na raiz do projeto.")
    return token


def request_json(url: str, headers: Dict, params: Optional[Dict] = None) -> Dict:
    resp = requests.get(url, headers=headers, params=params, timeout=30)

    if resp.status_code == 429:
        reset = int(resp.headers.get("X-RequestCounter-Reset", "60"))
        print(f"[429] Rate limit. Aguardando {reset}s e tentando novamente...")
        time.sleep(reset + 1)
        resp = requests.get(url, headers=headers, params=params, timeout=30)

    resp.raise_for_status()
    return resp.json()


def save_json(data: dict, filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def download_teams(season: int, headers: dict, competition: str) -> dict:
    url = f"{BASE_URL}/competitions/{competition}/teams"
    data = request_json(url, headers, params={"season": season})
    save_json(data, RAW_DIR / f"teams_{competition}_{season}.json")
    return data


def download_matches(season: int, headers: dict, competition: str) -> dict:
    url = f"{BASE_URL}/competitions/{competition}/matches"
    data = request_json(url, headers, params={"season": season})
    save_json(data, RAW_DIR / f"matches_{competition}_{season}.json")
    return data


def extract_raw(seasons: list[int] | None = None, competition: str = DEFAULT_COMPETITION) -> None:
    token = get_token()
    headers = {"X-Auth-Token": token}

    if seasons is None:
        seasons = DEFAULT_SEASONS

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for season in seasons:
        print(f"\n=== Competição {competition} | Temporada {season} ===")
        teams = download_teams(season, headers, competition)
        matches = download_matches(season, headers, competition)
        print(f"Salvo: {RAW_DIR / f'teams_{competition}_{season}.json'}")
        print(f"Salvo: {RAW_DIR / f'matches_{competition}_{season}.json'}")
        print(f"Times retornados: {len(teams.get('teams', []))}")
        print(f"Partidas retornadas: {len(matches.get('matches', []))}")
        time.sleep(7)


def main():
    extract_raw()


if __name__ == "__main__":
    main()