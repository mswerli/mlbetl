create table league.pitch_mix as (
	with totals as (
		select
			pitcher,
			game_year,
			pitcher || '_' || game_year as pitcher_year,
			count(*) as total_pitches
		from league.play_by_play pbp2
		group by
			pitcher, game_year
	),

	cross_tabs as (
	SELECT *
		FROM crosstab( $$
					  with totals as (
						select
							pitcher || '_' || game_year as pitcher_year,
							count(*) as total_pitches
						from league.play_by_play pbp2
						group by
						pitcher || '_' || game_year
					)
						select
							pitcher || '_' || game_year as pitcher_year,
							pitch_type,
							COUNT(*)::float/t.total_pitches::float
						from league.play_by_play pbp
						left join totals t
						on pbp.pitcher || '_' || pbp.game_year = t.pitcher_year
						group by pitcher || '_' || game_year,pitch_type,total_pitches
						order by pitcher || '_' || game_year,pitch_type,total_pitches $$ )
						AS final_result(
							pitcher_year text,
							FC  float,
							SI  float,
							PO  float,
							KC  float,
							SL  float,
							CU  float,
							FF  float,
							"FS"  float,
							KN  float,
							CH  float,
							SC  float,
							FT  float,
							FO  float,
							EPt  float
					)
			)

		select
			pitcher,
			game_year,
			total_pitches,
			FC,
			SI,
			PO,
			KC,
			SL,
			CU,
			FF,
			"FS",
			KN,
			CH,
			SC,
			FT,
			FO,
			EPt
	  from totals t
	  left join cross_tabs ct
	  on t.pitcher_year = ct.pitcher_year
);


select
	date_part('year', game_date) as year,
	date_part('month', game_date) as month,
	game_date,
	pitcher,
	pitch_type,
	count(*)
from league.play_by_play pbp
where pitch_type is not null and pitch_type != ''
GROUP by
	date_part('year', game_date),
    CUBE (
		date_part('month', game_date),
		game_date,
		pitcher,
		pitch_type
);

drop table league.at_bat_pitch_sequences;

create table league.at_bat_pitch_sequences as (
	select
		pbp.game_date::text||':'||pbp.game_pk::text ||':'||pbp.batter::text ||':'||pbp.at_bat_number as ab_id,
		pbp.game_date,
		pbp.game_pk,
		pbp.batter,
		case when ab.events is null THEN string_agg(pbp.pitch_type, '-')
		else string_agg(pbp.pitch_type, '-') || '-' || ab.events END as pitch_sequence
	from 	league.play_by_play pbp
	left join league.at_bats ab
	ON pbp.game_date::text||':'||pbp.game_pk::text ||':'||pbp.batter::text ||':'||pbp.at_bat_number = ab.ab_id
	group BY
		pbp.game_date,
		pbp.game_pk,
		pbp.batter,
		pbp.at_bat_number,
		ab.events
	)


