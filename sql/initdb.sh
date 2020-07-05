#!/bin/bash
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB"  <<-EOSQL
    DROP SCHEMA  IF EXISTS league CASCADE;
    DROP SCHEMA  IF EXISTS rosters CASCADE;
    DROP SCHEMA  IF EXISTS viz_tables CASCADE;
     CREATE SCHEMA league;

    CREATE SCHEMA rosters;

    CREATE SCHEMA viz_tables;

CREATE TABLE league.play_by_play (
	pitch_type text NULL,
	game_date date NULL,
	release_speed float8 NULL,
	release_pos_x float8 NULL,
	release_pos_z float8 NULL,
	player_name text NULL,
	batter int4 NULL,
	pitcher int4 NULL,
	events text NULL,
	description text NULL,
	spin_dir text NULL,
	spin_rate_deprecated float8 NULL,
	break_angle_deprecated float8 NULL,
	break_length_deprecated float8 NULL,
	"zone" float8 NULL,
	des text NULL,
	game_type text NULL,
	stand text NULL,
	p_throws text NULL,
	home_team text NULL,
	away_team text NULL,
	"type" text NULL,
	hit_location float8 NULL,
	bb_type text NULL,
	balls int4 NULL,
	strikes int4 NULL,
	game_year text NULL,
	pfx_x float8 NULL,
	pfx_z float8 NULL,
	plate_x float8 NULL,
	plate_z float8 NULL,
	on_3b float8 NULL,
	on_2b float8 NULL,
	on_1b float8 NULL,
	outs_when_up int4 NULL,
	inning int4 NULL,
	inning_topbot text NULL,
	hc_x float8 NULL,
	hc_y float8 NULL,
	tfs_deprecated text NULL,
	tfs_zulu_deprecated text NULL,
	fielder_2 float8 NULL,
	umpire text NULL,
	sv_id text NULL,
	vx0 float8 NULL,
	vy0 float8 NULL,
	vz0 float8 NULL,
	ax float8 NULL,
	ay float8 NULL,
	az float8 NULL,
	sz_top float8 NULL,
	sz_bot float8 NULL,
	hit_distance_sc float8 NULL,
	launch_speed float8 NULL,
	launch_angle float8 NULL,
	effective_speed float8 NULL,
	release_spin_rate float8 NULL,
	release_extension float8 NULL,
	game_pk int4 NULL,
	fielder_3 float8 NULL,
	fielder_4 float8 NULL,
	fielder_5 float8 NULL,
	fielder_6 float8 NULL,
	fielder_7 float8 NULL,
	fielder_8 float8 NULL,
	fielder_9 float8 NULL,
	release_pos_y float8 NULL,
	estimated_ba_using_speedangle float8 NULL,
	estimated_woba_using_speedangle float8 NULL,
	woba_value float8 NULL,
	woba_denom float8 NULL,
	babip_value float8 NULL,
	iso_value float8 NULL,
	launch_speed_angle float8 NULL,
	at_bat_number int4 NULL,
	pitch_number int4 NULL,
	pitch_name text NULL,
	home_score int4 NULL,
	away_score int4 NULL,
	bat_score int4 NULL,
	fld_score int4 NULL,
	post_away_score int4 NULL,
	post_home_score int4 NULL,
	post_bat_score int4 NULL,
	post_fld_score int4 NULL,
	if_fielding_alignment text NULL,
	of_fielding_alignment text NULL
);

CREATE TABLE rosters.players (
	birth_country text NULL,
	name_prefix text NULL,
	name_display_first_last text NULL,
	college text NULL,
	height_inches int4 NULL,
	death_country text NULL,
	age int4 NULL,
	name_display_first_last_html text NULL,
	gender text NULL,
	height_feet int4 NULL,
	pro_debut_date text NULL,
	death_date text NULL,
	primary_position text NULL,
	birth_date text NULL,
	team_abbrev text NULL,
	status text NULL,
	name_display_last_first_html text NULL,
	throws text NULL,
	death_city text NULL,
	primary_position_txt text NULL,
	high_school text NULL,
	name_display_roster_html text NULL,
	name_use text NULL,
	player_id int4 NOT NULL,
	status_date text NULL,
	primary_stat_type text NULL,
	team_id int4 NULL,
	active_sw text NULL,
	primary_sport_code text NULL,
	birth_state text NULL,
	weight int4 NULL,
	name_middle text NULL,
	name_display_roster text NULL,
	end_date text NULL,
	jersey_number float8 NULL,
	death_state text NULL,
	name_first text NULL,
	bats text NULL,
	team_code text NULL,
	birth_city text NULL,
	name_nick text NULL,
	status_code text NULL,
	name_matrilineal text NULL,
	team_name text NULL,
	name_display_last_first text NULL,
	twitter_id text NULL,
	name_title text NULL,
	file_code text NULL,
	name_last text NULL,
	start_date text NULL,
	name_full text NULL,
	CONSTRAINT players_pkey PRIMARY KEY (player_id)
);

CREATE TABLE rosters.roster_40 (
	position_txt text NULL,
	weight int4 NULL,
	name_display_first_last text NULL,
	college text NULL,
	height_inches int4 NULL,
	starter_sw text NULL,
	jersey_number int4 NULL,
	end_date date NULL,
	name_first text NULL,
	bats text NULL,
	team_code text NULL,
	height_feet int4 NULL,
	pro_debut_date date NULL,
	status_code text NULL,
	primary_position text NULL,
	birth_date date NULL,
	team_abbrev text NULL,
	throws text NULL,
	team_name text NULL,
	name_display_last_first text NULL,
	name_use text NULL,
	player_id int4 NULL,
	name_last text NULL,
	team_id int4 NULL,
	start_date text NULL,
	name_full text NULL
);

CREATE TABLE rosters.historical_rosters (
	name_first_last text NULL,
	weight int4 NULL,
	primary_position text NULL,
	birth_date date NULL,
	throws text NULL,
	stat_years text NULL,
	height_inches int4 NULL,
	name_sort text NULL,
	status_short text NULL,
	jersey_number int4 NULL,
	player_first_last_html text NULL,
	bats text NULL,
	primary_position_cd text NULL,
	position_desig text NULL,
	forty_man_sw text NULL,
	player_html text NULL,
	height_feet int4 NULL,
	player_id int4 NULL,
	name_last_first text NULL,
	current_sw text NULL,
	roster_years text NULL,
	team_id int4 NULL,
	active_sw text NULL
);

CREATE TABLE league.schedule (
	gamepk int4 NULL,
	link varchar(120) NULL,
	gametype varchar(8) NULL,
	season int4 NULL,
	gamedate timestamp NULL,
	officialdate date NULL,
	istie bool NULL,
	gamenumber int4 NULL,
	publicfacing bool NULL,
	doubleheader varchar(8) NULL,
	gamedaytype varchar(8) NULL,
	tiebreaker varchar(8) NULL,
	calendareventid varchar(120) NULL,
	seasondisplay int4 NULL,
	daynight varchar(8) NULL,
	scheduledinnings int4 NULL,
	inningbreaklength int4 NULL,
	gamesinseries float8 NULL,
	seriesgamenumber float8 NULL,
	seriesdescription varchar(200) NULL,
	recordsource varchar(10) NULL,
	ifnecessary varchar(8) NULL,
	ifnecessarydescription varchar(200) NULL,
	status_abstractgamestate varchar(120) NULL,
	status_codedgamestate varchar(12) NULL,
	status_detailedstate varchar(120) NULL,
	status_statuscode varchar(12) NULL,
	status_abstractgamecode varchar(12) NULL,
	teams_away_leaguerecord_wins int4 NULL,
	teams_away_leaguerecord_losses int4 NULL,
	teams_away_leaguerecord_pct float8 NULL,
	teams_away_score float8 NULL,
	teams_away_team_id int4 NULL,
	teams_away_team_name varchar(200) NULL,
	teams_away_team_link varchar(120) NULL,
	teams_away_iswinner bool NULL,
	teams_away_splitsquad bool NULL,
	teams_away_seriesnumber float8 NULL,
	teams_home_leaguerecord_wins int4 NULL,
	teams_home_leaguerecord_losses int4 NULL,
	teams_home_leaguerecord_pct float8 NULL,
	teams_home_score float8 NULL,
	teams_home_team_id int4 NULL,
	teams_home_team_name varchar(200) NULL,
	teams_home_team_link varchar(120) NULL,
	teams_home_iswinner bool NULL,
	teams_home_splitsquad bool NULL,
	teams_home_seriesnumber float8 NULL,
	venue_id int4 NULL,
	venue_name varchar(400) NULL,
	venue_link varchar(120) NULL,
	content_link varchar(250) NULL,
	status_reason varchar(120) NULL,
	description varchar(1000) NULL,
	rescheduledate timestamp NULL,
	rescheduledfrom timestamp NULL,
	status_starttimetbd bool NULL,
	resumedate timestamp NULL,
	resumedfrom timestamp NULL
);

CREATE TABLE league.teams (
	venue_name text NULL,
	franchise_code text NULL,
	sport_code text NULL,
	address_city text NULL,
	city text NULL,
	name_display_full text NULL,
	spring_league_abbrev text NULL,
	sport_id int4 NULL,
	venue_id int4 NULL,
	mlb_org_id int4 NULL,
	mlb_org text NULL,
	last_year_of_play int4 NULL,
	league_full text NULL,
	address_province text NULL,
	league_id int4 NULL,
	name_abbrev text NULL,
	league text NULL,
	spring_league text NULL,
	base_url text NULL,
	address_zip text NULL,
	sport_code_display text NULL,
	mlb_org_short text NULL,
	time_zone text NULL,
	address_line1 text NULL,
	mlb_org_brief text NULL,
	address_line2 text NULL,
	season int4 NULL,
	address_line3 text NULL,
	division_abbrev text NULL,
	name_display_short text NULL,
	team_id int4 NOT NULL,
	active_sw text NULL,
	address_intl text NULL,
	state text NULL,
	address_country text NULL,
	mlb_org_abbrev text NULL,
	division text NULL,
	team_code text NULL,
	"name" text NULL,
	website_url text NULL,
	sport_code_name text NULL,
	first_year_of_play int4 NULL,
	league_abbrev text NULL,
	name_display_long text NULL,
	store_url text NULL,
	time_zone_text text NULL,
	name_short text NULL,
	address_state text NULL,
	division_full text NULL,
	time_zone_num float8 NULL,
	spring_league_full text NULL,
	address text NULL,
	name_display_brief text NULL,
	file_code text NULL,
	division_id int4 NULL,
	spring_league_id int4 NULL,
	venue_short text NULL,
	CONSTRAINT teams_pkey PRIMARY KEY (team_id)
);

CREATE TABLE rosters.transactions (
	trans_date_cd text NULL,
	from_team_id int4 NULL,
	orig_asset text NULL,
	final_asset_type text NULL,
	player text NULL,
	resolution_cd text NULL,
	final_asset text NULL,
	name_display_first_last text NULL,
	type_cd text NULL,
	name_sort text NULL,
	resolution_date text NULL,
	conditional_sw text NULL,
	team text NULL,
	"type" text NULL,
	name_display_last_first text NULL,
	transaction_id int4 NULL,
	trans_date date NULL,
	effective_date date NULL,
	player_id int4 NULL,
	orig_asset_type text NULL,
	from_team text NULL,
	team_id int4 NULL,
	note text NULL
);

CREATE TABLE league.events (
    "starttime" timestamp NULL,
    "endtime" timestamp NULL,
    "ispitch" boolean NULL,
    "type" varchar(50),
    "details_description" text NULL,
    "details_event" text NULL,
    "details_eventtype" varchar(500) NULL,
    "details_awayscore" float NULL ,
    "details_homescore" float NULL,
    "details_isscoringplay" boolean NULL,
    "details_hasreview" boolean NULL,
    "count_balls" float NULL,
    "count_strikes" float NULL,
    "count_outs" float NULL,
    "player_id" float NULL,
    "player_link" varchar(500) NULL,
    "pfxid" varchar(50) NULL,
    "playid" varchar(100) NULL,
    "pitchnumber" float NULL,
    "details_call_code" varchar(10) NULL,
    "details_call_description" varchar(100) NULL,
    "details_code" varchar(100) NULL,
    "details_ballcolor" varchar(500) NULL,
    "details_trailcolor" varchar(500) NULL,
    "details_isinplay" boolean NULL,
    "details_isstrike" boolean NULL,
    "details_isball" boolean NULL,
    "details_type_code" varchar(10) NULL,
    "details_type_description" text NULL,
    "pitchdata_startspeed" float NULL,
    "pitchdata_endspeed" float NULL,
    "pitchdata_strikezonetop" float NULL,
    "pitchdata_strikezonebottom" float NULL,
    "pitchdata_coordinates_ay" float NULL,
    "pitchdata_coordinates_az" float NULL,
    "pitchdata_coordinates_pfxx" float NULL,
    "pitchdata_coordinates_pfxz" float NULL,
    "pitchdata_coordinates_px" float NULL,
    "pitchdata_coordinates_pz" float NULL,
    "pitchdata_coordinates_vx0" float NULL,
    "pitchdata_coordinates_vy0" float NULL,
    "pitchdata_coordinates_vz0" float NULL,
    "pitchdata_coordinates_x" float NULL,
    "pitchdata_coordinates_y" float NULL,
    "pitchdata_coordinates_x0" float NULL,
    "pitchdata_coordinates_y0" float NULL,
    "pitchdata_coordinates_z0" float NULL,
    "pitchdata_coordinates_ax" float NULL,
    "pitchdata_breaks_breakangle" float NULL,
    "pitchdata_breaks_breaklength" float NULL,
    "pitchdata_breaks_breaky" float NULL,
    "pitchdata_breaks_spinrate" float NULL,
    "pitchdata_breaks_spindirection" float NULL,
    "pitchdata_zone" float NULL,
    "pitchdata_typeconfidence" float NULL,
    "pitchdata_platetime" float NULL,
    "pitchdata_extension" float NULL,
    "hitdata_launchspeed" float NULL,
    "hitdata_launchangle" float NULL,
    "hitdata_totaldistance" float NULL,
    "hitdata_trajectory" varchar(200) NULL,
    "hitdata_hardness" varchar(10) NULL,
    "hitdata_location" float NULL,
    "hitdata_coordinates_coordx" float NULL,
    "hitdata_coordinates_coordy" float NULL,
    "details_fromcatcher" boolean NULL,
    "position_code" float NULL,
    "position_name" varchar(100) NULL,
    "position_type" varchar(100) NULL,
    "position_abbreviation" varchar(100) NULL,
    "battingorder" float NULL,
    "details_runnergoing" boolean NULL,
    "result_event" text,
	  "game_pk" float,
    "actionplayid" varchar(500) NULL,
    "base" float NULL,
    "injurytype" varchar(200)NULL,
    "umpire_id" float NULL,
    "umpire_link" varchar(500) NULL,
    "reviewdetails_isoverturned" boolean NULL ,
    "reviewdetails_reviewtype" varchar(10) NULL,
    "reviewdetails_challengeteamid" float
);

CREATE TABLE league.atbat (
    "pitchindex" text NULL,
    "actionindex" text NULL,
    "runnerindex" text NULL,
    "runners" json NULL,
    "playevents" json NULL,
    "atbatindex" text NULL,
    "playendtime" timestamp,
    "result_type" varchar(100) NULL ,
    "result_event" varchar(500) NULL,
    "result_eventtype" varchar(100),
    "result_description" text,
    "result_rbi" float NULL,
    "result_awayscore" float NULL,
    "result_homescore" float NULL,
    "about_atbatindex" float NULL,
    "about_halfinning" varchar(50) NULL,
    "about_istopinning" boolean NULL,
    "about_inning" float NULL,
    "about_starttime" timestamp,
    "about_endtime" timestamp,
    "about_iscomplete" boolean NULL,
    "about_isscoringplay" boolean NULL,
    "about_hasreview" boolean NULL,
    "about_hasout" boolean NULL,
    "about_captivatingindex" float NULL ,
    "count_balls" float NULL,
    "count_strikes" float NULL,
    "count_outs" float NULL,
    "matchup_batter_id" float NULL,
    "matchup_batter_fullname" varchar(500) NULL,
    "matchup_batter_link" varchar(500) NULL,
    "matchup_batside_code" varchar(10),
    "matchup_batside_description" varchar(10),
    "matchup_pitcher_id" float NULL,
    "matchup_pitcher_fullname" varchar(1000) NULL,
    "matchup_pitcher_link" varchar(100) NULL,
    "matchup_pitchhand_code" varchar(10) NULL,
    "matchup_pitchhand_description" varchar(10) NULL,
    "matchup_batterhotcoldzones" text NULL ,
    "matchup_pitcherhotcoldzones" text NULL,
    "matchup_splits_batter" varchar(10) NULL,
    "matchup_splits_pitcher" varchar(10) NULL,
    "matchup_splits_menonbase" varchar(50),
    "matchup_postonfirst_id" float NULL,
    "matchup_postonfirst_fullname" varchar(1000) NULL,
    "matchup_postonfirst_link" varchar(500) NULL,
    "matchup_postonsecond_id" float NULL,
    "matchup_postonsecond_fullname" varchar(1000) NULL,
    "matchup_postonsecond_link" varchar(500) NULL,
    "matchup_postonthird_id" float NULL,
    "matchup_postonthird_fullname" varchar(1000) NULL,
    "matchup_postonthird_link" varchar(500) NULL,
    "matchup_batterhotcoldzonestats_stats" jsonb NULL,
    "matchup_pitcherhotcoldzonestats_stats" jsonb NULL
);

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

 CREATE MATERIALIZED VIEW league.missing_games as (
     WITH games AS (
             SELECT DISTINCT play_by_play.game_pk
               FROM league.play_by_play
            )
     SELECT games.game_pk
       FROM games
);

EOSQL