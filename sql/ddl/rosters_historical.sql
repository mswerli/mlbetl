CREATE TABLE rosters.historical_rosters(
   name_first_last        VARCHAR(30)
  ,weight                 INTEGER
  ,primary_position       VARCHAR(30)
  ,birth_date             VARCHAR(30)
  ,throws                 VARCHAR(30)
  ,stat_years             VARCHAR(30)
  ,height_inches          INTEGER
  ,name_sort              VARCHAR(30)
  ,status_short           VARCHAR(30)
  ,jersey_number          INTEGER
  ,player_first_last_html VARCHAR(30)
  ,bats                   VARCHAR(30)
  ,primary_position_cd    VARCHAR(30)
  ,position_desig         VARCHAR(30)
  ,forty_man_sw           VARCHAR(30)
  ,player_html            VARCHAR(30)
  ,height_feet            INTEGER
  ,player_id              INTEGER REFERENCES rosters.players(player_id)
  ,name_last_first        VARCHAR(30)
  ,current_sw             VARCHAR(30)
  ,roster_years           VARCHAR(30)
  ,team_id                INTEGER REFERENCES league.teams(team_id)
  ,active_sw              VARCHAR(30)
)