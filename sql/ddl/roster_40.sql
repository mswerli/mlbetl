CREATE TABLE rosters.roster_40(
   position_txt            VARCHAR(30)
  ,weight                  INTEGER
  ,name_display_first_last VARCHAR(30)
  ,college                 VARCHAR(30)
  ,height_inches           INTEGER
  ,starter_sw              VARCHAR(30)
  ,jersey_number           INTEGER
  ,end_date                VARCHAR(30)
  ,name_first              VARCHAR(30)
  ,bats                    VARCHAR(30)
  ,team_code               VARCHAR(30)
  ,height_feet             INTEGER
  ,pro_debut_date          VARCHAR(30)
  ,status_code             VARCHAR(30)
  ,primary_position        INTEGER
  ,birth_date              VARCHAR(30)
  ,team_abbrev             VARCHAR(30)
  ,throws                  VARCHAR(30)
  ,team_name               VARCHAR(30)
  ,name_display_last_first VARCHAR(30)
  ,name_use                VARCHAR(30)
  ,player_id               INTEGER REFERENCES rosters.players(player_id)
  ,name_last               VARCHAR(30)
  ,team_id                 INTEGER REFERENCES league.teams(team_id)
  ,start_date              VARCHAR(30)
  ,name_full               VARCHAR(30)
);