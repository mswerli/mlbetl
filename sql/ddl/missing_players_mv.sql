DROP MATERIALIZED VIEW IF EXISTS rosters.missing_players;

CREATE MATERIALIZED VIEW rosters.missing_players as (
  WITH players as (
    SELECT DISTINCT
      player_id
    FROM rosters.players
  ),
  missing_from_40 as (
    SELECT DISTINCT
      r.player_id
    FROM rosters.roster_40 r
    LEFT JOIN players p
    ON r.player_id = p.player_id
    WHERE p.player_id IS NULL
  ),
  missing_from_past as (
    SELECT DISTINCT
      r.player_id
    FROM rosters.historical_rosters r
    LEFT JOIN players p
    ON r.player_id = p.player_id
    WHERE p.player_id IS NULL
  )
  SELECT player_id FROM missing_from_40
  UNION
  SELECT player_id FROM missing_from_past
);