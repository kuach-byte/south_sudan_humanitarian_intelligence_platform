import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
PROFILE_FILE = Path.home() / ".dbt" / "profiles.yml"

load_dotenv(ENV_FILE)

required_vars = [
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
]

missing = [key for key in required_vars if not os.getenv(key)]

if missing:
    raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

profile = {
    "south_sudan_humanitarian_intelligence_platform": {
        "target": "dev",
        "outputs": {
            "dev": {
                "type": "postgres",
                "host": os.environ["DB_HOST"],
                "port": int(os.environ["DB_PORT"]),
                "dbname": os.environ["DB_NAME"],
                "user": os.environ["DB_USER"],
                "password": os.environ["DB_PASSWORD"],
                "schema": "staging",
                "threads": 4,
            }
        },
    }
}

PROFILE_FILE.parent.mkdir(parents=True, exist_ok=True)

with PROFILE_FILE.open("w", encoding="utf-8") as file:
    yaml.safe_dump(profile, file, sort_keys=False)

print(f"dbt profile created at: {PROFILE_FILE}")