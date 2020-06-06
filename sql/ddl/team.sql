DROP TABLE IF EXISTS league.teams;
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