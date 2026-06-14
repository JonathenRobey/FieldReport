"""
fetch_games.py
DiamondSignal — Phase 1, Step 1

Pulls historical MLB game schedule and results from the MLB Stats API
for the 2022 and 2023 regular seasons, then stores them in SQLite.

MLB Stats API is free, public, no API key required.
Base URL: https://statsapi.mlb.com/api/v1

Run:
    python fetch_games.py
    python fetch_games.py --seasons 2021 2022 2023
    python fetch_games.py --dry-run   # fetch but don't write to DB
"""

import argparse
import sqlite3
import time
import json
import logging
from datetime import datetime
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config — centralise these in config.py once the project grows
# ---------------------------------------------------------------------------

BASE_URL = "https://statsapi.mlb.com/api/v1"
DEFAULT_SEASONS = [2022, 2023]
GAME_TYPE = "R"          # R = Regular season, P = Postseason, S = Spring
REQUEST_DELAY = 0.5      # seconds between requests — be polite to the API
REQUEST_TIMEOUT = 30     # seconds before a request times out
MAX_RETRIES = 3

DB_PATH = Path("data/diamondsignal.db")
RAW_PATH = Path("data/raw/games")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS games (
    game_pk         INTEGER PRIMARY KEY,
    season          INTEGER NOT NULL,
    game_date       TEXT NOT NULL,
    game_type       TEXT NOT NULL,
    status          TEXT,
    home_team_id    INTEGER,
    home_team_name  TEXT,
    away_team_id    INTEGER,
    away_team_name  TEXT,
    home_score      INTEGER,
    away_score      INTEGER,
    venue_name      TEXT,
    game_number     INTEGER,
    double_header   TEXT,
    fetched_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_games_season    ON games(season);
CREATE INDEX IF NOT EXISTS idx_games_date      ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_home_team ON games(home_team_id);
CREATE INDEX IF NOT EXISTS idx_games_away_team ON games(away_team_id);
"""


def get_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def get(endpoint: str, params: dict = None) -> dict:
    """GET from the MLB Stats API with retry logic."""
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            log.warning(f"HTTP {e.response.status_code} on attempt {attempt}: {url}")
            if e.response.status_code == 404:
                return {}           # resource doesn't exist, skip cleanly
            if attempt == MAX_RETRIES:
                raise
        except requests.exceptions.RequestException as e:
            log.warning(f"Request error on attempt {attempt}: {e}")
            if attempt == MAX_RETRIES:
                raise
        time.sleep(2 ** attempt)    # exponential backoff on retry
    return {}


# ---------------------------------------------------------------------------
# Fetch schedule
# ---------------------------------------------------------------------------

def fetch_schedule(season: int, game_type: str = GAME_TYPE) -> list[dict]:
    """
    Fetch every game in a season from the /schedule endpoint.
    Returns a flat list of raw game dicts.
    """
    log.info(f"Fetching schedule: season={season}, game_type={game_type}")

    data = get("schedule", params={
        "season": season,
        "gameType": game_type,
        "sportId": 1,                # 1 = MLB
        "hydrate": "linescore,team", # include scores + team names in one call
    })

    if not data or "dates" not in data:
        log.warning(f"No schedule data returned for season {season}")
        return []

    games = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            games.append(game)

    log.info(f"  Found {len(games)} games for {season}")
    return games


# ---------------------------------------------------------------------------
# Parse game record
# ---------------------------------------------------------------------------

def parse_game(raw: dict) -> dict:
    """
    Extract the fields we care about from a raw schedule game dict.
    Handles missing fields gracefully — the API can be inconsistent.
    """
    teams  = raw.get("teams", {})
    home   = teams.get("home", {})
    away   = teams.get("away", {})
    lscore = raw.get("linescore", {})

    home_team = home.get("team", {})
    away_team = away.get("team", {})

    # Scores live in linescore when hydrated, otherwise in teams block
    home_score = (
        lscore.get("teams", {}).get("home", {}).get("runs")
        or home.get("score")
    )
    away_score = (
        lscore.get("teams", {}).get("away", {}).get("runs")
        or away.get("score")
    )

    return {
        "game_pk":       raw.get("gamePk"),
        "season":        raw.get("season"),
        "game_date":     raw.get("gameDate", "")[:10],  # keep YYYY-MM-DD only
        "game_type":     raw.get("gameType"),
        "status":        raw.get("status", {}).get("detailedState"),
        "home_team_id":  home_team.get("id"),
        "home_team_name":home_team.get("name"),
        "away_team_id":  away_team.get("id"),
        "away_team_name":away_team.get("name"),
        "home_score":    home_score,
        "away_score":    away_score,
        "venue_name":    raw.get("venue", {}).get("name"),
        "game_number":   raw.get("gameNumber"),
        "double_header": raw.get("doubleHeader"),
        "fetched_at":    datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Save to SQLite
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO games (
    game_pk, season, game_date, game_type, status,
    home_team_id, home_team_name, away_team_id, away_team_name,
    home_score, away_score, venue_name, game_number, double_header, fetched_at
) VALUES (
    :game_pk, :season, :game_date, :game_type, :status,
    :home_team_id, :home_team_name, :away_team_id, :away_team_name,
    :home_score, :away_score, :venue_name, :game_number, :double_header, :fetched_at
)
ON CONFLICT(game_pk) DO UPDATE SET
    status        = excluded.status,
    home_score    = excluded.home_score,
    away_score    = excluded.away_score,
    fetched_at    = excluded.fetched_at;
"""


def save_games(conn: sqlite3.Connection, games: list[dict]) -> int:
    """Upsert a list of parsed game rows. Returns the number of rows affected."""
    if not games:
        return 0
    conn.executemany(UPSERT_SQL, games)
    conn.commit()
    return len(games)


# ---------------------------------------------------------------------------
# Optional: save raw JSON for debugging / replay
# ---------------------------------------------------------------------------

def save_raw(season: int, raw_games: list[dict]) -> None:
    RAW_PATH.mkdir(parents=True, exist_ok=True)
    out = RAW_PATH / f"schedule_{season}.json"
    with open(out, "w") as f:
        json.dump(raw_games, f, indent=2)
    log.info(f"  Raw JSON saved to {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(seasons: list[int], dry_run: bool = False, save_raw_json: bool = True):
    conn = None if dry_run else get_db(DB_PATH)

    total = 0
    for season in seasons:
        raw_games = fetch_schedule(season)

        if save_raw_json:
            save_raw(season, raw_games)

        parsed = [parse_game(g) for g in raw_games if g.get("gamePk")]

        # Filter to only Final games so we have actual scores
        final = [g for g in parsed if g["status"] == "Final"]
        log.info(f"  Parsed {len(final)} Final games out of {len(parsed)} total")

        if dry_run:
            log.info(f"  [dry-run] Would write {len(final)} rows for {season}")
            if final:
                log.info(f"  Sample row: {final[0]}")
        else:
            n = save_games(conn, final)
            log.info(f"  Saved {n} games for {season} to {DB_PATH}")
            total += n

        time.sleep(REQUEST_DELAY)

    if not dry_run:
        # Quick summary query
        row = conn.execute(
            "SELECT COUNT(*) as n, MIN(game_date) as first, MAX(game_date) as last FROM games"
        ).fetchone()
        log.info(
            f"\nDB summary: {row['n']} total games "
            f"({row['first']} → {row['last']})"
        )
        conn.close()

    log.info("Done.")
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch MLB game schedule into SQLite")
    parser.add_argument(
        "--seasons", nargs="+", type=int, default=DEFAULT_SEASONS,
        help="Season years to fetch (default: 2022 2023)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and parse but do not write to the database"
    )
    parser.add_argument(
        "--no-raw", action="store_true",
        help="Skip saving raw JSON files"
    )
    args = parser.parse_args()

    run(
        seasons=args.seasons,
        dry_run=args.dry_run,
        save_raw_json=not args.no_raw,
    )