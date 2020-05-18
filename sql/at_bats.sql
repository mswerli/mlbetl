drop table if exists  league.at_bats;
create table league.at_bats as (
with at_bats as (
	select distinct
		game_date,
		game_pk,
		batter,
		at_bat_number as ab_overall,
		max(pitch_number)  over (partition by game_date, game_pk, batter, at_bat_number) as  final_pitch,
		min(pitch_number)  over (partition by game_date, game_pk, batter, at_bat_number) as  first_pitch
	from league.play_by_play pbp
)
	select distinct
		pbp.game_date::text||':'||pbp.game_pk::text ||':'||pbp.batter::text ||':'||pbp.at_bat_number as ab_id,
		pbp.game_date,
		pbp.game_pk,
		pbp.batter,
		pbp.des,
		pbp.description,
		pbp.events,
		row_number() over (partition by pbp.game_date, pbp.game_pk, pbp.batter order by pbp.game_date, pbp.game_pk, pbp.batter) as at_bat_number,
		1 + (ab.final_pitch - ab.first_pitch) as total_pitches,
		pbp.bb_type,
		pbp.of_fielding_alignment,
		pbp.if_fielding_alignment,
		pbp.inning,
		pbp.inning_topbot,
		pbp.outs_when_up,
		pbp.balls||'-'|| pbp.strikes as final_count,
		pbp.stand,
		pbp.p_throws
	from league.play_by_play pbp
	left join at_bats ab
	on pbp.game_date = ab.game_date and
	   pbp.game_pk = ab.game_pk and
	   pbp.batter = ab.batter and
	   pbp.at_bat_number = ab.ab_overall and
	   pbp.pitch_number = ab.final_pitch
	where ab.final_pitch is not null
    order by
   		pbp.game_date,
		pbp.game_pk,
		pbp.batter
);


drop table if exists league.ab_pitch_sequence;
create table league.ab_pitch_sequence as (
	SELECT *
	FROM crosstab( $$
					select
						game_date::text||':'||game_pk::text ||':'|| batter::text ||':'|| at_bat_number as ab_id,
						'pitch_'|| pitch_number as pitch_number,
						pitch_type
					from league.play_by_play pbp
					where pitch_number <= 20 order by 1,2$$ )
	AS final_result(
			ab_id text,
			pitch_type_1 text,
			pitch_type_2 text,
			pitch_type_3 text,
			pitch_type_4 text,
			pitch_type_5 text,
			pitch_type_6 text,
			pitch_type_7 text,
			pitch_type_8 text,
			pitch_type_9 text,
			pitch_type_10 text,
			pitch_type_11 text,
			pitch_type_12 text,
			pitch_type_13 text,
			pitch_type_14 text,
			pitch_type_15 text,
			pitch_type_16 text,
			pitch_type_17 text,
			pitch_type_18 text,
			pitch_type_19 text,
			pitch_type_20 text
	)
);

drop table if exists league.ab_outcome_sequence;
create table league.ab_outcome_sequence as (
	SELECT *
	FROM crosstab( $$
					select
						game_date::text||':'||game_pk::text ||':'|| batter::text ||':'|| at_bat_number as ab_id,
						'pitch_'|| pitch_number as pitch_number,
						description
					from league.play_by_play pbp
					where pitch_number <= 20 order by 1,2$$ )
	AS final_result(
			ab_id text,
			outcome_1 text,
			outcome_2 text,
			outcome_3 text,
			outcome_4 text,
			outcome_5 text,
			outcome_6 text,
			outcome_7 text,
			outcome_8 text,
			outcome_9 text,
			outcome_10 text,
			outcome_11 text,
			outcome_12 text,
			outcome_13 text,
			outcome_14 text,
			outcome_15 text,
			outcome_16 text,
			outcome_17 text,
			outcome_18 text,
			outcome_19 text,
			outcome_20 text
		)
);

drop table if exists league.ab_pitch_count_sequence;
create table league.ab_pitch_count_sequence as (
	SELECT *
	FROM crosstab( $$
					select
						game_date::text||':'||game_pk::text ||':'|| batter::text ||':'|| at_bat_number as ab_id,
						'pitch_'|| pitch_number as pitch_number,
						balls::text || '-'|| strikes::text as "pitch_count"
					from league.play_by_play pbp
					where pitch_number <= 20 order by 1,2$$ )
	AS final_result(
			ab_id text,
			pitch_count_1 text,
			pitch_count_2 text,
			pitch_count_3 text,
			pitch_count_4 text,
			pitch_count_5 text,
			pitch_count_6 text,
			pitch_count_7 text,
			pitch_count_8 text,
			pitch_count_9 text,
			pitch_count_10 text,
			pitch_count_11 text,
			pitch_count_12 text,
			pitch_count_13 text,
			pitch_count_14 text,
			pitch_count_15 text,
			pitch_count_16 text,
			pitch_count_17 text,
			pitch_count_18 text,
			pitch_count_19 text,
			pitch_count_20 text
	)
);

drop table if exists league.ab_pitch_location_sequence;
create table league.ab_pitch_location_sequence as (
	SELECT *
	FROM crosstab( $$
					select
						game_date::text||':'||game_pk::text ||':'|| batter::text ||':'|| at_bat_number as ab_id,
						'pitch_'|| pitch_number as pitch_number,
						case when zone = 'NaN' then null::int else zone::int end as "zone"
					from league.play_by_play pbp
					where pitch_number <= 20 order by 1,2$$ )
	AS final_result(
			ab_id text,
			pitch_zone_1 int,
			pitch_zone_2 int,
			pitch_zone_3 int,
			pitch_zone_4 int,
			pitch_zone_5 int,
			pitch_zone_6 int,
			pitch_zone_7 int,
			pitch_zone_8 int,
			pitch_zone_9 int,
			pitch_zone_10 int,
			pitch_zone_11 int,
			pitch_zone_12 int,
			pitch_zone_13 int,
			pitch_zone_14 int,
			pitch_zone_15 int,
			pitch_zone_16 int,
			pitch_zone_17 int,
			pitch_zone_18 int,
			pitch_zone_19 int,
			pitch_zone_20 int
	)
);

drop table league.ab_paths;
create table league.ab_paths as (
	select
		ab.batter,
		ab.events as ab_outcome,
		pitch_count_1 || ':' || pitch_type_1   as pitch_1,
		pitch_count_2 || ':' || pitch_type_2   as pitch_2,
		pitch_count_3 || ':' || pitch_type_3   as pitch_3,
		pitch_count_4 || ':' || pitch_type_4   as pitch_4,
		pitch_count_5 || ':' || pitch_type_5   as pitch_5,
		pitch_count_6 || ':' || pitch_type_6  as pitch_6,
		pitch_count_7 || ':' || pitch_type_7   as pitch_7,
		pitch_count_8 || ':' || pitch_type_8   as pitch_8,
		pitch_count_9 || ':' || pitch_type_9   as pitch_9,
		pitch_count_10 || ':' || pitch_type_10   as pitch_10,
		pitch_count_11 || ':' || pitch_type_11   as pitch_11,
		pitch_count_12 || ':' || pitch_type_12   as pitch_12,
		pitch_count_13 || ':' || pitch_type_13   as pitch_13,
		pitch_count_14 || ':' || pitch_type_14   as pitch_14,
		pitch_count_15 || ':' || pitch_type_15   as pitch_15,
		pitch_count_16 || ':' || pitch_type_16   as pitch_16,
		pitch_count_17 || ':' || pitch_type_17   as pitch_17,
		pitch_count_18 || ':' || pitch_type_18   as pitch_18,
		pitch_count_19 || ':' || pitch_type_19   as pitch_19,
		pitch_count_20 || ':' || pitch_type_20   as pitch_20,
		COUNT(*)
	from league.at_bats ab
	left join league.ab_pitch_sequence ps
	on ab.ab_id = ps.ab_id
	left join league.ab_outcome_sequence os
	on ab.ab_id = os.ab_id
	left join league.ab_pitch_count_sequence pcs
	on ab.ab_id = pcs.ab_id
	group by
		ab.batter,
		ab.events ,
		pitch_count_1 || ':' || pitch_type_1 ,
		pitch_count_2 || ':' || pitch_type_2 ,
		pitch_count_3 || ':' || pitch_type_3 ,
		pitch_count_4 || ':' || pitch_type_4 ,
		pitch_count_5 || ':' || pitch_type_5 ,
		pitch_count_6 || ':' || pitch_type_6 ,
		pitch_count_7 || ':' || pitch_type_7 ,
		pitch_count_8 || ':' || pitch_type_8 ,
		pitch_count_9 || ':' || pitch_type_9 ,
		pitch_count_10 || ':' || pitch_type_10 ,
		pitch_count_11 || ':' || pitch_type_11 ,
		pitch_count_12 || ':' || pitch_type_12 ,
		pitch_count_13 || ':' || pitch_type_13 ,
		pitch_count_14 || ':' || pitch_type_14 ,
		pitch_count_15 || ':' || pitch_type_15 ,
		pitch_count_16 || ':' || pitch_type_16 ,
		pitch_count_17 || ':' || pitch_type_17 ,
		pitch_count_18 || ':' || pitch_type_18 ,
		pitch_count_19 || ':' || pitch_type_19 ,
		pitch_count_20 || ':' || pitch_type_20
	order by
	ab.batter,
	count(*)
)





