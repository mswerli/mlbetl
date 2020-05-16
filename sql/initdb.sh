#!/bin/bash
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB"  <<-EOSQL
    DROP SCHEMA  IF EXISTS league CASCADE;
    DROP SCHEMA  IF EXISTS rosters CASCADE;
     CREATE SCHEMA league
        AUTHORIZATION doadmin;

    CREATE SCHEMA rosters
        AUTHORIZATION doadmin;

    CREATE TABLE IF NOT EXISTS league.teams(
       venue_name           text
      ,franchise_code       text
      ,sport_code           text
      ,address_city         text
      ,city                 text
      ,name_display_full    text
      ,spring_league_abbrev text
      ,sport_id             INTEGER
      ,venue_id             INTEGER
      ,mlb_org_id           INTEGER
      ,mlb_org              text
      ,last_year_of_play    INTEGER
      ,league_full          text
      ,address_province     text
      ,league_id            INTEGER
      ,name_abbrev          text
      ,league               text
      ,spring_league        text
      ,base_url             text
      ,address_zip          text
      ,sport_code_display   text
      ,mlb_org_short        text
      ,time_zone            text
      ,address_line1        text
      ,mlb_org_brief        text
      ,address_line2        text
      ,season               INTEGER
      ,address_line3        text
      ,division_abbrev      text
      ,name_display_short   text
      ,team_id              INTEGER PRIMARY KEY
      ,active_sw            text
      ,address_intl         text
      ,state                text
      ,address_country      text
      ,mlb_org_abbrev       text
      ,division             text
      ,team_code            text
      ,name                 text
      ,website_url          text
      ,sport_code_name      text
      ,first_year_of_play   INTEGER
      ,league_abbrev        text
      ,name_display_long    text
      ,store_url            text
      ,time_zone_text       text
      ,name_short           text
      ,address_state        text
      ,division_full        text
      ,time_zone_num        INTEGER
      ,spring_league_full   text
      ,address              text
      ,name_display_brief   text
      ,file_code            text
      ,division_id          INTEGER
      ,spring_league_id     INTEGER
      ,venue_short          text
    );

  CREATE TABLE rosters.players(
     birth_country                text
    ,name_prefix                  text
    ,name_display_first_last      text
    ,college                      text
    ,height_inches                INTEGER
    ,death_country                text
    ,age                          INTEGER
    ,name_display_first_last_html text
    ,gender                       text
    ,height_feet                  INTEGER
    ,pro_debut_date               text
    ,death_date                   text
    ,primary_position             text
    ,birth_date                   text
    ,team_abbrev                  text
    ,status                       text
    ,name_display_last_first_html text
    ,throws                       text
    ,death_city                   text
    ,primary_position_txt         text
    ,high_school                  text
    ,name_display_roster_html     text
    ,name_use                     text
    ,player_id                    INTEGER PRIMARY KEY
    ,status_date                  text
    ,primary_stat_type            text
    ,team_id                      INTEGER
    ,active_sw                    text
    ,primary_sport_code           text
    ,birth_state                  text
    ,weight                       INTEGER
    ,name_middle                  text
    ,name_display_roster          text
    ,end_date                     text
    ,jersey_number                INTEGER
    ,death_state                  text
    ,name_first                   text
    ,bats                         text
    ,team_code                    text
    ,birth_city                   text
    ,name_nick                    text
    ,status_code                  text
    ,name_matrilineal             text
    ,team_name                    text
    ,name_display_last_first      text
    ,twitter_id                   text
    ,name_title                   text
    ,file_code                    text
    ,name_last                    text
    ,start_date                   text
    ,name_full                    text
  );

    CREATE TABLE rosters.roster_40(
       position_txt            text
      ,weight                  INTEGER
      ,name_display_first_last text
      ,college                 text
      ,height_inches           INTEGER
      ,starter_sw              text
      ,jersey_number           INTEGER
      ,end_date                date
      ,name_first              text
      ,bats                    text
      ,team_code               text
      ,height_feet             INTEGER
      ,pro_debut_date          date
      ,status_code             text
      ,primary_position        text
      ,birth_date              date
      ,team_abbrev             text
      ,throws                  text
      ,team_name               text
      ,name_display_last_first text
      ,name_use                text
      ,player_id               INTEGER
      ,name_last               text
      ,team_id                 INTEGER
      ,start_date              text
      ,name_full               text
    );

    CREATE TABLE rosters.historical_rosters(
       name_first_last        text
      ,weight                 INTEGER
      ,primary_position       text
      ,birth_date             date
      ,throws                 text
      ,stat_years             text
      ,height_inches          INTEGER
      ,name_sort              text
      ,status_short           text
      ,jersey_number          INTEGER
      ,player_first_last_html text
      ,bats                   text
      ,primary_position_cd    text
      ,position_desig         text
      ,forty_man_sw           text
      ,player_html            text
      ,height_feet            INTEGER
      ,player_id              INTEGER
      ,name_last_first        text
      ,current_sw             text
      ,roster_years           text
      ,team_id                INTEGER
      ,active_sw              text
    );

    CREATE TABLE rosters.transactions(
       trans_date_cd           text
      ,from_team_id            INTEGER
      ,orig_asset              text
      ,final_asset_type        text
      ,player                  text
      ,resolution_cd           text
      ,final_asset             text
      ,name_display_first_last text
      ,type_cd                 text
      ,name_sort               text
      ,resolution_date         text
      ,conditional_sw          text
      ,team                    text
      ,"type"                    text
      ,name_display_last_first text
      ,transaction_id          INTEGER
      ,trans_date              date
      ,effective_date          date
      ,player_id               INTEGER
      ,orig_asset_type         text
      ,from_team               text
      ,team_id                 INTEGER
      ,note                    text
    );
CREATE TABLE league.play_by_play
(
    pitch_type text COLLATE pg_catalog."default",
    game_date date,
    release_speed numeric(4,1),
    release_pos_x numeric(7,4),
    release_pos_z numeric(6,4),
    player_name text COLLATE pg_catalog."default",
    batter integer,
    pitcher integer,
    events text COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    spin_dir text COLLATE pg_catalog."default",
    spin_rate_deprecated numeric(7,4),
    break_angle_deprecated numeric(7,4),
    break_length_deprecated numeric(7,4),
    zone numeric(3,0),
    des text COLLATE pg_catalog."default",
    game_type text COLLATE pg_catalog."default",
    stand text COLLATE pg_catalog."default",
    p_throws text COLLATE pg_catalog."default",
    home_team text COLLATE pg_catalog."default",
    away_team text COLLATE pg_catalog."default",
    type text COLLATE pg_catalog."default",
    hit_location numeric(7,4),
    bb_type text COLLATE pg_catalog."default",
    balls integer,
    strikes integer,
    game_year text COLLATE pg_catalog."default",
    pfx_x numeric(7,4),
    pfx_z numeric(6,4),
    plate_x numeric(6,4),
    plate_z numeric(6,4),
    on_3b numeric(8,0),
    on_2b numeric(8,0),
    on_1b numeric(8,0),
    outs_when_up integer,
    inning integer,
    inning_topbot text COLLATE pg_catalog."default",
    hc_x numeric(6,2),
    hc_y numeric(5,2),
    tfs_deprecated text COLLATE pg_catalog."default",
    tfs_zulu_deprecated text COLLATE pg_catalog."default",
    fielder_2 numeric(8,0),
    umpire text COLLATE pg_catalog."default",
    sv_id text COLLATE pg_catalog."default",
    vx0 numeric(6,4),
    vy0 numeric(9,4),
    vz0 numeric(7,4),
    ax numeric(8,4),
    ay numeric(7,4),
    az numeric(8,4),
    sz_top numeric(6,4),
    sz_bot numeric(6,4),
    hit_distance_sc numeric(7,4),
    launch_speed numeric(5,1),
    launch_angle numeric(4,1),
    effective_speed numeric(7,4),
    release_spin_rate numeric(6,2),
    release_extension numeric(6,4),
    game_pk integer,
    fielder_3 numeric(8,0),
    fielder_4 numeric(8,0),
    fielder_5 numeric(8,0),
    fielder_6 numeric(8,0),
    fielder_7 numeric(8,0),
    fielder_8 numeric(8,0),
    fielder_9 numeric(8,0),
    release_pos_y numeric(7,4),
    estimated_ba_using_speedangle numeric(5,3),
    estimated_woba_using_speedangle numeric(5,3),
    woba_value numeric(4,2),
    woba_denom numeric(5,3),
    babip_value numeric(5,3),
    iso_value numeric(5,3),
    launch_speed_angle numeric(7,4),
    at_bat_number integer,
    pitch_number integer,
    pitch_name text COLLATE pg_catalog."default",
    home_score integer,
    away_score integer,
    bat_score integer,
    fld_score integer,
    post_away_score integer,
    post_home_score integer,
    post_bat_score integer,
    post_fld_score integer,
    if_fielding_alignment text COLLATE pg_catalog."default",
    of_fielding_alignment text COLLATE pg_catalog."default"
);
EOSQL