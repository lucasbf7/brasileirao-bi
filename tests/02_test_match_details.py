import os
import requests
from dotenv import load_dotenv

BASE_URL = "https://api.football-data.org/v4"
MATCH_ID = 1780 # vasco

def main():
    load_dotenv()
    token = os.getenv("FOOTBALL_DATA_TOKEN")
    headers = {"X-Auth-Token": token}

    url = f"{BASE_URL}/matches/{MATCH_ID}"
    resp = requests.get(url, headers=headers, timeout=30)
    print("Status:", resp.status_code)
    print("Body snippet:", resp.text[:500])

if __name__ == "__main__":
    main()