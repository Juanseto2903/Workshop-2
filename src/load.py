"""Idempotent load into the Data Warehouse (Section 9 of the design document).

    python -m src.load

Strategy:
  * Dimensions dim_genre and dim_grammy_category: UPSERT on the business key.
  * Fact and bridges: TRUNCATE + INSERT inside one transaction, so a re-run
    with the same batch produces the same target state (safe rerun).
  * Fixed dimensions (dim_grammy_recognition, dim_popularity_tier) are seeded
    by sql/dw_schema.sql; the load never creates them.

The Airflow task load_dw (Step 10) calls run().
"""
from __future__ import annotations

import logging
import os

import pandas as pd

from . import paths

try:
    from dotenv import load_dotenv
    load_dotenv(paths.ROOT / ".env")
except ImportError:
    pass

log = logging.getLogger(__name__)


def _dw_url() -> str:
    user = os.getenv("WH_USER")
    password = os.getenv("WH_PASSWORD")
    host = os.getenv("WH_HOST", "localhost")
    port = os.getenv("WH_PORT", "5433")
    missing = [k for k, v in {"WH_USER": user, "WH_PASSWORD": password}.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}. "
            f"Check that .env exists at {paths.ROOT / '.env'} and defines them."
        )
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/music_dw"


def _engine():
    from sqlalchemy import create_engine
    return create_engine(_dw_url())


def _read(name: str) -> pd.DataFrame:
    path = paths.STAGING_DIR / name
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


# --------------------------------------------------------------------------- #
# Dimension loading
# --------------------------------------------------------------------------- #
def _upsert_genres(conn, bridge_genre: pd.DataFrame) -> dict[str, int]:
    from sqlalchemy import text
    genres = sorted(bridge_genre["track_genre"].dropna().unique().tolist())
    stmt = text("""
        INSERT INTO dim_genre (genre_name) VALUES (:name)
        ON CONFLICT (genre_name) DO NOTHING
    """)
    for name in genres:
        conn.execute(stmt, {"name": name})
    rows = conn.execute(text("SELECT genre_key, genre_name FROM dim_genre")).fetchall()
    return {name: key for key, name in rows}


def _upsert_categories(conn, dim_category: pd.DataFrame) -> dict[str, int]:
    from sqlalchemy import text
    stmt = text("""
        INSERT INTO dim_grammy_category (category_name, category_group)
        VALUES (:name, :group)
        ON CONFLICT (category_name) DO NOTHING
    """)
    for _, r in dim_category.iterrows():
        conn.execute(stmt, {"name": r["category_name"], "group": r["category_group"]})
    rows = conn.execute(
        text("SELECT category_key, category_name FROM dim_grammy_category")).fetchall()
    return {name: key for key, name in rows}


def _recognition_keys(conn) -> dict[tuple[str, str], int]:
    from sqlalchemy import text
    rows = conn.execute(text(
        "SELECT recognition_key, recognition_group, artist_grammy_wins_range "
        "FROM dim_grammy_recognition")).fetchall()
    return {(g, w): k for k, g, w in rows}


def _tier_keys(conn) -> dict[str, int]:
    from sqlalchemy import text
    rows = conn.execute(text(
        "SELECT popularity_tier_key, tier_name FROM dim_popularity_tier")).fetchall()
    return {name: key for key, name in rows}


# --------------------------------------------------------------------------- #
# Fact + bridges
# --------------------------------------------------------------------------- #
def _insert_facts(conn, tracks: pd.DataFrame,
                  recognition: dict, tiers: dict) -> dict[str, int]:
    from sqlalchemy import text
    stmt = text("""
        INSERT INTO fact_track (
            recognition_key, popularity_tier_key, track_id, track_name,
            album_name, popularity, duration_ms, danceability, energy,
            valence, acousticness, loudness, tempo, explicit_flag, track_count
        ) VALUES (
            :recognition_key, :popularity_tier_key, :track_id, :track_name,
            :album_name, :popularity, :duration_ms, :danceability, :energy,
            :valence, :acousticness, :loudness, :tempo, :explicit_flag, :track_count
        )
        RETURNING track_key, track_id
    """)
    track_keys: dict[str, int] = {}
    for _, r in tracks.iterrows():
        rec_key = recognition[(r["recognition_group"], r["artist_grammy_wins_range"])]
        tier_key = tiers[r["popularity_tier"]]
        result = conn.execute(stmt, {
            "recognition_key": rec_key,
            "popularity_tier_key": tier_key,
            "track_id": r["track_id"],
            "track_name": r["track_name"],
            "album_name": r["album_name"],
            "popularity": float(r["popularity"]),
            "duration_ms": int(r["duration_ms"]),
            "danceability": float(r["danceability"]),
            "energy": float(r["energy"]),
            "valence": float(r["valence"]),
            "acousticness": float(r["acousticness"]),
            "loudness": float(r["loudness"]),
            "tempo": None if pd.isna(r["tempo"]) else float(r["tempo"]),
            "explicit_flag": int(r["explicit_flag"]),
            "track_count": 1,
        }).first()
        track_keys[result[1]] = result[0]
    return track_keys


def _insert_bridge_genre(conn, bridge_genre: pd.DataFrame,
                         track_keys: dict[str, int],
                         genre_keys: dict[str, int]) -> int:
    from sqlalchemy import text
    stmt = text("""
        INSERT INTO bridge_track_genre (track_key, genre_key)
        VALUES (:track_key, :genre_key)
        ON CONFLICT (track_key, genre_key) DO NOTHING
    """)
    inserted = 0
    for _, r in bridge_genre.iterrows():
        tk = track_keys.get(r["track_id"])
        gk = genre_keys.get(r["track_genre"])
        if tk is None or gk is None:
            continue
        conn.execute(stmt, {"track_key": tk, "genre_key": gk})
        inserted += 1
    return inserted


def _insert_bridge_grammy_category(conn, bridge_cat: pd.DataFrame,
                                    track_keys: dict[str, int],
                                    category_keys: dict[str, int]) -> int:
    from sqlalchemy import text
    stmt = text("""
        INSERT INTO bridge_track_grammy_category
            (track_key, category_key, grammy_wins)
        VALUES (:track_key, :category_key, :grammy_wins)
        ON CONFLICT (track_key, category_key) DO NOTHING
    """)
    inserted = 0
    for _, r in bridge_cat.iterrows():
        tk = track_keys.get(r["track_id"])
        ck = category_keys.get(r["category_name"])
        if tk is None or ck is None:
            continue
        conn.execute(stmt, {
            "track_key": tk,
            "category_key": ck,
            "grammy_wins": int(r["grammy_wins"]),
        })
        inserted += 1
    return inserted


# --------------------------------------------------------------------------- #
# Public orchestration
# --------------------------------------------------------------------------- #
def run() -> dict:
    """Load the prepared datasets into the Data Warehouse. Idempotent."""
    from sqlalchemy import text

    tracks = _read("prepared_tracks.parquet")
    bridge_genre = _read("bridge_track_genre.parquet")
    bridge_category = _read("bridge_track_grammy_category.parquet")
    dim_category = _read("dim_grammy_category_prepared.parquet")

    engine = _engine()
    summary = {}
    try:
        with engine.begin() as conn:            # one transaction
            # 1. Dimensions
            genre_keys = _upsert_genres(conn, bridge_genre)
            category_keys = _upsert_categories(conn, dim_category)
            recognition = _recognition_keys(conn)
            tiers = _tier_keys(conn)

            # 2. Wipe fact and bridges (deterministic replace)
            conn.execute(text(
                "TRUNCATE TABLE bridge_track_grammy_category, "
                "bridge_track_genre, fact_track RESTART IDENTITY CASCADE"))

            # 3. Fact
            track_keys = _insert_facts(conn, tracks, recognition, tiers)

            # 4. Bridges
            n_bridge_genre = _insert_bridge_genre(
                conn, bridge_genre, track_keys, genre_keys)
            n_bridge_category = _insert_bridge_grammy_category(
                conn, bridge_category, track_keys, category_keys)

            # 5. Row counts
            summary = {
                "fact_track": conn.execute(
                    text("SELECT COUNT(*) FROM fact_track")).scalar(),
                "bridge_track_genre": conn.execute(
                    text("SELECT COUNT(*) FROM bridge_track_genre")).scalar(),
                "bridge_track_grammy_category": conn.execute(
                    text("SELECT COUNT(*) FROM bridge_track_grammy_category")).scalar(),
                "dim_genre": conn.execute(
                    text("SELECT COUNT(*) FROM dim_genre")).scalar(),
                "dim_grammy_category": conn.execute(
                    text("SELECT COUNT(*) FROM dim_grammy_category")).scalar(),
                "fact_tracks_inserted": len(track_keys),
                "bridge_track_genre_inserted": n_bridge_genre,
                "bridge_track_grammy_category_inserted": n_bridge_category,
            }
    finally:
        engine.dispose()

    log.info("Load summary: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()