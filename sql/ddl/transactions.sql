DROP TABLE IF EXISTS rosters.transactions;

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