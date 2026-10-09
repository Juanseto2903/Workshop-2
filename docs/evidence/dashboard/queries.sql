-- =========================================================================
-- Dashboard queries for the Workshop 2 Data Warehouse (music_dw).
-- Each query answers one analytical requirement (AR-01 to AR-03) and is
-- the source of one page in the Power BI dashboard.
-- =========================================================================

-- -------------------------------------------------------------------------
-- KPI-01 (AR-01): Average popularity by Grammy recognition group
-- Source: Fact_Track + Dim_Grammy_Recognition
-- Used in: dashboard page 1 (bar chart)
-- -------------------------------------------------------------------------
SELECT
    r.recognition_group,
    r.artist_grammy_wins_range,
    ROUND(AVG(f.popularity)::numeric, 2) AS avg_popularity,
    COUNT(*) AS tracks
FROM fact_track f
JOIN dim_grammy_recognition r ON f.recognition_key = r.recognition_key
GROUP BY r.recognition_group, r.artist_grammy_wins_range
ORDER BY r.recognition_group, r.artist_grammy_wins_range;

-- -------------------------------------------------------------------------
-- KPI-02 (AR-02): Average audio features by recognition group and tier
-- Source: Fact_Track + Dim_Grammy_Recognition + Dim_Popularity_Tier
-- Used in: dashboard page 2 (feature cards)
-- -------------------------------------------------------------------------
SELECT
    r.recognition_group,
    t.tier_name,
    ROUND(AVG(f.danceability)::numeric, 3) AS danceability,
    ROUND(AVG(f.energy)::numeric, 3) AS energy,
    ROUND(AVG(f.valence)::numeric, 3) AS valence,
    ROUND(AVG(f.acousticness)::numeric, 3) AS acousticness,
    ROUND(AVG(f.loudness)::numeric, 2) AS loudness,
    ROUND(AVG(f.tempo)::numeric, 2) AS tempo,
    COUNT(*) AS tracks
FROM fact_track f
JOIN dim_grammy_recognition r ON f.recognition_key = r.recognition_key
JOIN dim_popularity_tier t ON f.popularity_tier_key = t.popularity_tier_key
GROUP BY r.recognition_group, t.tier_name
ORDER BY r.recognition_group, t.tier_name;

-- -------------------------------------------------------------------------
-- KPI-03 (AR-03): Share of explicit tracks by Grammy category group
-- Source: Fact_Track + Dim_Grammy_Recognition + Bridge_Track_Grammy_Category + Dim_Grammy_Category
-- Used in: dashboard page 3 (donut chart and table)
-- Note: this query produces the pre-aggregated table imported into Power BI
-- as kpi03_explicit_by_category to avoid the many-to-many relation issue.
-- -------------------------------------------------------------------------
SELECT
    c.category_group,
    COUNT(*) AS tracks,
    ROUND(AVG(f.explicit_flag)::numeric * 100, 2) AS pct_explicit
FROM fact_track f
JOIN bridge_track_grammy_category b ON f.track_key = b.track_key
JOIN dim_grammy_category c ON b.category_key = c.category_key
GROUP BY c.category_group
ORDER BY pct_explicit DESC;