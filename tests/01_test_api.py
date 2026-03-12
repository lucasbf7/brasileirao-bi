import os
import requests
from dotenv import load_dotenv

BASE_URL = "https://api.football-data.org/v4"

def main():
    load_dotenv()
    token = os.getenv("FOOTBALL_DATA_TOKEN")
    if not token:
        raise RuntimeError("FOOTBALL_DATA_TOKEN não encontrado. Confira seu .env na raiz do projeto.")
    
    headers = {"X-Auth-Token": token}

    # Endpoint simples só para validar autenticação e resposta
    url = f"{BASE_URL}/competitions/BSA"
    resp = requests.get(url, headers=headers, timeout=30)

    print("Status:", resp.status_code)
    print("Headers (rate limit):")
    for k in ["X-Requests-Available-Minute", "X-RequestCounter-Reset"]:
        if k in resp.headers:
            print(f"  {k}: {resp.headers[k]}")

    resp.raise_for_status()
    data = resp.json()


    print("\nCompetição:", data.get("name"))
    print("Código:", data.get("code"))
    print("Área:", data.get("area", {}).get("name"))

if __name__ == "__main__":
    main()