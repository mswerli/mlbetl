CREATE TABLE IF NOT EXISTS rosters.players(
   birth_country                VARCHAR(4) NULL
  ,name_prefix                  VARCHAR(30)
  ,name_display_first_last      VARCHAR(15) NOT NULL
  ,college                      VARCHAR(30)
  ,height_inches                INTEGER  NOT NULL
  ,death_country                VARCHAR(30)
  ,age                          INTEGER  NOT NULL
  ,name_display_first_last_html VARCHAR(15) NOT NULL
  ,gender                       VARCHAR(1) NOT NULL
  ,height_feet                  INTEGER  NOT NULL
  ,pro_debut_date               VARCHAR(19) NOT NULL
  ,death_date                   VARCHAR(30)
  ,primary_position             INTEGER  NOT NULL
  ,birth_date                   VARCHAR(19) NOT NULL
  ,team_abbrev                  VARCHAR(3) NOT NULL
  ,status                       VARCHAR(6) NOT NULL
  ,name_display_last_first_html VARCHAR(16) NOT NULL
  ,throws                       VARCHAR(1) NOT NULL
  ,death_city                   VARCHAR(30)
  ,primary_position_txt         VARCHAR(2) NOT NULL
  ,high_school                  VARCHAR(30)
  ,name_display_roster_html     VARCHAR(8) NOT NULL
  ,name_use                     VARCHAR(6) NOT NULL
  ,player_id                    INTEGER PRIMARY KEY NOT NULL
  ,status_date                  VARCHAR(19) NOT NULL
  ,primary_stat_type            VARCHAR(7) NOT NULL
  ,active_sw                    VARCHAR(1) NOT NULL
  ,primary_sport_code           VARCHAR(3) NOT NULL
  ,birth_state                  VARCHAR(30)
  ,weight                       INTEGER  NOT NULL
  ,name_middle                  VARCHAR(30)
  ,name_display_roster          VARCHAR(8) NOT NULL
  ,end_date                     VARCHAR(30)
  ,jersey_number                INTEGER  NOT NULL
  ,death_state                  VARCHAR(30)
  ,name_first                   VARCHAR(6) NOT NULL
  ,bats                         VARCHAR(1) NOT NULL
  ,team_code                    VARCHAR(3) NOT NULL
  ,birth_city                   VARCHAR(6) NOT NULL
  ,name_nick                    VARCHAR(11) NOT NULL
  ,status_code                  VARCHAR(1) NOT NULL
  ,name_matrilineal             VARCHAR(7) NOT NULL
  ,name_display_last_first      VARCHAR(16) NOT NULL
  ,twitter_id                   VARCHAR(9) NOT NULL
  ,name_title                   VARCHAR(30)
  ,file_code                    VARCHAR(3) NOT NULL
  ,name_last                    VARCHAR(8) NOT NULL
  ,start_date                   VARCHAR(19) NOT NULL
  ,name_full                    VARCHAR(16) NOT NULL
);