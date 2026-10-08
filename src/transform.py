"""Transformation and integration (Section 7 of the design document).

    python -m src.transform

Reads:  data/staging/spotify_raw.parquet
        data/staging/grammys_raw.parquet
Writes: data/staging/prepared_tracks.parquet
        data/staging/grammy_artist_match.parquet
        data/staging/bridge_track_genre.parquet
        data/staging/dim_grammy_category_prepared.parquet

This module only reads Parquet and writes Parquet. It does not touch the DW.
The Airflow task transform_and_integrate (Step 10) calls run().
"""
from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

from . import paths

log = logging.getLogger(__name__)

POPULARITY_QUANTILE = 0.90          # TR-08
GRAMMY_ARTIST_SPLIT = re.compile(
    r"[;,]|\s+&\s+|\s+and\s+|\s+feat\.?\s+|\s+featuring\s+", flags=re.IGNORECASE
)
VOWS_PLACEHOLDERS = {"(various artists)", "various artists", ""}


# --------------------------------------------------------------------------- #
# TR-05: Artist key normalization
# --------------------------------------------------------------------------- #
def normalize_artist_name(name) -> str | None:
    """Lower case, accents removed, '&' -> 'and', punctuation removed,
    leading 'the' dropped, spaces collapsed. Returns None if empty."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return None
    text = str(name).strip().lower()
    if not text:
        return None
    text = text.replace("&", "and")
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("ASCII")
    if text.startswith("the "):
        text = text[4:]
    return text or None


def _split_grammy_artist(raw: str) -> set[str]:
    """Return the set of normalized artist keys for one Grammy 'artist' value.
    Tries the full string first, then splits on common delimiters (TR-05)."""
    keys: set[str] = set()
    full = normalize_artist_name(raw)
    if full:
        keys.add(full)
    for part in GRAMMY_ARTIST_SPLIT.split(raw):
        key = normalize_artist_name(part)
        if key:
            keys.add(key)
    return keys


# --------------------------------------------------------------------------- #
# TR-06: Grammy wins per artist
# --------------------------------------------------------------------------- #
def build_grammy_artist_wins(grammy_df: pd.DataFrame) -> pd.DataFrame:
    """One row per normalized Grammy artist key, with the number of Grammy wins.
    Each Grammy record counts as one win. Null artists and '(various artists)'
    are excluded here but kept in the match report (TR-06)."""
    counts: dict[str, int] = {}
    for raw in grammy_df["artist"].tolist():
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        text = str(raw).strip()
        if text.lower() in VOWS_PLACEHOLDERS:
            continue
        for key in _split_grammy_artist(text):
            counts[key] = counts.get(key, 0) + 1
    if not counts:
        return pd.DataFrame(columns=["artist_key", "grammy_wins"])
    return (
        pd.DataFrame({"artist_key": list(counts), "grammy_wins": list(counts.values())})
        .sort_values("artist_key")
        .reset_index(drop=True)
    )


def build_grammy_artist_match(grammy_df: pd.DataFrame,
                              spotify_artists: set[str]) -> pd.DataFrame:
    """DQ-12 dataset: one row per distinct normalized Grammy artist key,
    with matched_flag = 1 if the artist appears in the Spotify catalog."""
    keys: set[str] = set()
    for raw in grammy_df["artist"].tolist():
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        text = str(raw).strip()
        if text.lower() in VOWS_PLACEHOLDERS:
            continue
        keys.update(_split_grammy_artist(text))
    if not keys:
        return pd.DataFrame(columns=["artist_key", "matched_flag"])
    df = pd.DataFrame({"artist_key": sorted(keys)})
    df["matched_flag"] = df["artist_key"].isin(spotify_artists).astype(int)
    return df


# --------------------------------------------------------------------------- #
# TR-01, TR-02, TR-03, TR-04: track grain and clean measures
# --------------------------------------------------------------------------- #
def aggregate_tracks(spotify_df: pd.DataFrame) -> pd.DataFrame:
    """One row per unique track_id. Popularity is averaged (TR-02);
    other attributes take the first observed value. Tempo 0 -> NaN (TR-03).
    Rows without critical attributes are dropped (TR-04)."""
    df = spotify_df.dropna(subset=["artists", "track_name", "album_name"]).copy()
    if "tempo" in df.columns:
        df["tempo"] = pd.to_numeric(df["tempo"], errors="coerce").replace(0, np.nan)

    agg = {
        "track_id": "first", "track_name": "first", "album_name": "first",
        "artists": "first", "popularity": "mean",
        "duration_ms": "first", "danceability": "first", "energy": "first",
        "valence": "first", "acousticness": "first", "loudness": "first",
        "tempo": "first", "explicit": "first",
    }
    agg = {k: v for k, v in agg.items() if k in df.columns}
    tracks = df.groupby("track_id", as_index=False).agg(agg)
    return tracks


# --------------------------------------------------------------------------- #
# TR-07: Grammy recognition classification
# --------------------------------------------------------------------------- #
def _max_grammy_wins(artists_str, wins_by_artist: dict[str, int]) -> int:
    if pd.isna(artists_str):
        return 0
    best = 0
    for raw in str(artists_str).split(";"):
        key = normalize_artist_name(raw)
        if key:
            best = max(best, wins_by_artist.get(key, 0))
    return best


def _wins_range(wins: int) -> str:
    if wins <= 0:
        return "0"
    if wins == 1:
        return "1"
    if wins <= 4:
        return "2-4"
    return "5+"


def classify_recognition(tracks: pd.DataFrame,
                         wins_df: pd.DataFrame) -> pd.DataFrame:
    wins_by_artist = dict(zip(wins_df["artist_key"], wins_df["grammy_wins"]))
    tracks = tracks.copy()
    tracks["grammy_wins"] = tracks["artists"].apply(
        lambda s: _max_grammy_wins(s, wins_by_artist))
    tracks["is_grammy_track"] = (tracks["grammy_wins"] > 0).astype(int)
    tracks["recognition_group"] = np.where(
        tracks["is_grammy_track"] == 1, "Grammy", "No-Grammy")
    tracks["artist_grammy_wins_range"] = tracks["grammy_wins"].apply(_wins_range)
    return tracks


# --------------------------------------------------------------------------- #
# TR-08: popularity tier
# --------------------------------------------------------------------------- #
def assign_popularity_tier(tracks: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    cutoff = float(tracks["popularity"].quantile(POPULARITY_QUANTILE))
    tracks = tracks.copy()
    tracks["popularity_tier"] = np.where(
        tracks["popularity"] >= cutoff, "Most popular", "Rest")
    return tracks, cutoff


# --------------------------------------------------------------------------- #
# TR-11: bridge_track_genre
# --------------------------------------------------------------------------- #
def build_bridge_track_genre(spotify_df: pd.DataFrame) -> pd.DataFrame:
    bridge = (spotify_df[["track_id", "track_genre"]]
              .dropna()
              .drop_duplicates()
              .reset_index(drop=True))
    return bridge


# --------------------------------------------------------------------------- #
# TR-12: Grammy category grouping (simplified Pop / Rock / Rap / Country / Classical / Other)
# --------------------------------------------------------------------------- #
_CATEGORY_RULES = [
    ("Pop",       re.compile(r"\bpop\b|pop\s|pop\b|pop/", re.IGNORECASE)),
    ("Rock",      re.compile(r"rock|metal|punk|grunge", re.IGNORECASE)),
    ("Rap",       re.compile(r"rap|hip[- ]?hop|urban", re.IGNORECASE)),
    ("Country",   re.compile(r"country|bluegrass|americana|folk", re.IGNORECASE)),
    ("Classical", re.compile(r"classical|opera|orchestra|symphon|chamber|choral",
                             re.IGNORECASE)),
    ("Jazz",      re.compile(r"jazz|blues|swing", re.IGNORECASE)),
    ("Latin",     re.compile(r"latin|reggaeton|salsa|tango|bossa", re.IGNORECASE)),
    ("R&B",       re.compile(r"r&b|rhythm|soul|gospel", re.IGNORECASE)),
    ("Dance/Electronic",
                  re.compile(r"dance|electronic|edm|house|techno|trance|dj",
                             re.IGNORECASE)),
]


def categorize_group(category: str) -> str:
    if pd.isna(category):
        return "Other"
    text = str(category)
    for group, pattern in _CATEGORY_RULES:
        if pattern.search(text):
            return group
    return "Other"


def build_dim_grammy_category(grammy_df: pd.DataFrame) -> pd.DataFrame:
    cats = (grammy_df["category"].dropna().astype(str)
            .str.strip().drop_duplicates().sort_values())
    df = pd.DataFrame({"category_name": cats})
    df["category_group"] = df["category_name"].apply(categorize_group)
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Public orchestration
# --------------------------------------------------------------------------- #
def _spotify_artist_set(spotify_df: pd.DataFrame) -> set[str]:
    keys: set[str] = set()
    for raw in spotify_df["artists"].dropna().tolist():
        for part in str(raw).split(";"):
            key = normalize_artist_name(part)
            if key:
                keys.add(key)
    return keys


def run() -> dict:
    """Execute the full transformation. Returns a summary dict for logging."""
    spotify_path = paths.STAGING_DIR / "spotify_raw.parquet"
    grammy_path = paths.STAGING_DIR / "grammys_raw.parquet"

    log.info("Reading %s", spotify_path)
    spotify_df = pd.read_parquet(spotify_path)
    log.info("Reading %s", grammy_path)
    grammy_df = pd.read_parquet(grammy_path)

    # TR-05 / TR-06: Grammy side
    wins_df = build_grammy_artist_wins(grammy_df)
    spotify_artists = _spotify_artist_set(spotify_df)
    match_df = build_grammy_artist_match(grammy_df, spotify_artists)

    # TR-01..TR-04 / TR-08
    tracks = aggregate_tracks(spotify_df)
    tracks = classify_recognition(tracks, wins_df)
    tracks, cutoff = assign_popularity_tier(tracks)

    # TR-09 / TR-10: explicit_flag and track_count
    tracks["explicit_flag"] = tracks["explicit"].astype(int)
    tracks["track_count"] = 1

    # Prepared data contract (rules.PREPARED_TRACK_COLUMNS)
    prepared_tracks = tracks[[
        "track_id", "track_name", "album_name", "popularity", "duration_ms",
        "danceability", "energy", "valence", "acousticness", "loudness",
        "tempo", "explicit_flag", "track_count", "recognition_group",
        "artist_grammy_wins_range", "popularity_tier", "is_grammy_track",
    ]].copy()

    # Enforce the declared domain of Fact_Track.track_name and album_name
    # (VARCHAR(500)). Tracks whose names exceed the column width are truncated
    # in a deterministic way; the rest of the row is preserved.
    prepared_tracks["track_name"] = prepared_tracks["track_name"].astype(str).str.slice(0, 500)
    prepared_tracks["album_name"] = prepared_tracks["album_name"].astype(str).str.slice(0, 500)

    # Bridges and derived category dimension
    bridge_genre = build_bridge_track_genre(spotify_df)
    dim_category = build_dim_grammy_category(grammy_df)

    # Write Parquet (overwrite: safe to re-run)
    paths.STAGING_DIR.mkdir(parents=True, exist_ok=True)
    prepared_tracks.to_parquet(
        paths.STAGING_DIR / "prepared_tracks.parquet", index=False)
    match_df.to_parquet(
        paths.STAGING_DIR / "grammy_artist_match.parquet", index=False)
    bridge_genre.to_parquet(
        paths.STAGING_DIR / "bridge_track_genre.parquet", index=False)
    dim_category.to_parquet(
        paths.STAGING_DIR / "dim_grammy_category_prepared.parquet", index=False)

    summary = {
        "tracks": int(len(prepared_tracks)),
        "grammy_tracks": int(prepared_tracks["is_grammy_track"].sum()),
        "no_grammy_tracks": int((prepared_tracks["is_grammy_track"] == 0).sum()),
        "grammy_artists_matched": int(match_df["matched_flag"].sum()),
        "grammy_artists_total": int(len(match_df)),
        "popularity_cutoff": cutoff,
        "bridge_track_genre_rows": int(len(bridge_genre)),
        "grammy_categories": int(len(dim_category)),
    }
    log.info("Transformation summary: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()