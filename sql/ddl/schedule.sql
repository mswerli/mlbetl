DROP TABLE IF EXISTS league.schedule;
CREATE TABLE league.schedule (
	gamepk foat8 NULL,
	link varchar(120) NULL,
	gametype varchar(8) NULL,
	season foat8 NULL,
	gamedate timestamp NULL,
	officialdate date NULL,
	istie bool NULL,
	gamenumber foat8 NULL,
	publicfacing bool NULL,
	doubleheader varchar(8) NULL,
	gamedaytype varchar(8) NULL,
	tiebreaker varchar(8) NULL,
	calendareventid varchar(120) NULL,
	seasondisplay foat8 NULL,
	daynight varchar(8) NULL,
	scheduledinnings foat8 NULL,
	inningbreaklength foat8 NULL,
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
	teams_away_leaguerecord_wins foat8 NULL,
	teams_away_leaguerecord_losses foat8 NULL,
	teams_away_leaguerecord_pct float8 NULL,
	teams_away_score float8 NULL,
	teams_away_team_id foat8 NULL,
	teams_away_team_name varchar(200) NULL,
	teams_away_team_link varchar(120) NULL,
	teams_away_iswinner bool NULL,
	teams_away_splitsquad bool NULL,
	teams_away_seriesnumber float8 NULL,
	teams_home_leaguerecord_wins foat8 NULL,
	teams_home_leaguerecord_losses foat8 NULL,
	teams_home_leaguerecord_pct float8 NULL,
	teams_home_score float8 NULL,
	teams_home_team_id foat8 NULL,
	teams_home_team_name varchar(200) NULL,
	teams_home_team_link varchar(120) NULL,
	teams_home_iswinner bool NULL,
	teams_home_splitsquad bool NULL,
	teams_home_seriesnumber float8 NULL,
	venue_id foat8 NULL,
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
