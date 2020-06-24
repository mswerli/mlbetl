 CREATE MATERIALIZED VIEW league.missing_games as (
     WITH games AS (
             SELECT DISTINCT play_by_play.game_pk
               FROM league.play_by_play
            )
     SELECT games.game_pk
       FROM games
);