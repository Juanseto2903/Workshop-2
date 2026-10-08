# Test C — Safe rerun

## Strategy
TRUNCATE + INSERT inside one transaction for fact_track and bridges;
UPSERT for dim_genre and dim_grammy_category; fixed dimensions are seeded once.

## Evidence
| Step | fact_track rows |
|------|-----------------|
| Run 1 (before rerun) | 89740 |
| Run 2 (after rerun)  | 89740 |

## Result
Row count is identical before and after the rerun. No duplicates were created.

## Raw outputs
- Run 1: `Load summary: {'fact_track': 89740, ...}` (see attached log)
- Run 2: `Load summary: {'fact_track': 89740, ...}` (see attached log)