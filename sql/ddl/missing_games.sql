 CREATE MATERIALIZED VIEW league.missing_games as (
     WITH games AS (
             SELECT DISTINCT gamepk
               FROM league.schedule
               WHERE gametype in ('R','F','D','L','W')
            )
     SELECT a.gamepk as game_pk
       FROM games a
       LEFT JOIN league.atbat b
       ON a.gamepk = b.game_pk
       WHERE b.game_pk is NULL

);