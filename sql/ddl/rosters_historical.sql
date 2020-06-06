DROP TABLE IF EXISTS rosters.historical_rosters;

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
