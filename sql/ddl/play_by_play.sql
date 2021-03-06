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
    "matchup_pitcherhotcoldzonestats_stats" jsonb NULL,
    "game_pk" float NULL
);