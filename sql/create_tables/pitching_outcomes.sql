create table league.weekly_pitcher_stats as (
select
	pitcher,
	p_throws,
	stand,
	date_part('month' , game_date) as month,
	date_part('year' , game_date) as year,
	date_part('week' , game_date) as week,
	inning_topbot = 'Bottom' as at_home,
	count(*) as pitches_thrown,
	sum((description = 'swinging_strike')::int) as swinging_strikes,
	sum((description = 'called_strike')::int) as called_strikes,
	sum((description = 'ball')::int) as overall_balls,
	max(at_bat_number) - min(at_bat_number) as at_bats,
	sum((pitch_type in ('FF', 'FT'))::int) as fastballs,
    sum((pitch_type in ('CH', 'FS'))::int) as split_or_change,
	sum((pitch_type not in ('CH', 'FS','FF', 'FT'))::int) as breaking_pitches,
	sum((description ='hit_into_play_no_out' and events != 'error')::int) as hits,
	sum((events = 'walk')::int) as walks,
	sum((events similar to '%strikeout')::int) as strikeouts,
	sum((events = 'hit_by_pitch')::int) as hit_by_pitch,
	sum((events = 'home_run')::int) as home_runs,
	sum((events = 'single')::int) as singles,
	sum((events = 'double')::int) as doubles,
	sum((events = 'triple')::int) as triple,
	sum((events similar to  '%double_play%')::int) as gidp,
	(sum(woba_value) filter (where woba_value != 'NaN'))::float as woba_numerator,
	(sum(woba_denom) filter (where woba_denom != 'NaN'))::float as woba_denomenator,
	(sum(babip_value) filter (where babip_value != 'NaN'))::float as babip_numerator,
	sum((babip_value != 'NaN')::int)::float as babip_denomenator,
	(sum(iso_value) filter (where iso_value != 'NaN'))::float as iso_numerator,
	sum(case when events = 'home_run' then 4
			 when events = 'tripple' then 3
			 when events = 'double' then 2
			 when events = 'single' then 1
			 else 0 END) as total_bases,
	avg(release_speed) filter (where pitch_type in ('FF', 'FT')) as avg_fastball_velo,
	avg(launch_angle) filter (where launch_angle != 'NaN') as average_launch_angle,
	avg(launch_speed) filter (where launch_speed != 'NaN')  as average_exit_velocity,
	sum((bb_type = 'ground_ball')::int) as ground_balls,
	sum((bb_type = 'line_drive')::int) as line_drives,
	sum((bb_type = 'fly_ball')::int) as fly_balls,
	sum((plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4)::int) as in_zone,
    sum((plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4 and type = 'S')::int) as swing_in_zone,
    sum((not (plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4) and (type = 'S'))::int) as swing_out_zone
from league.play_by_play pbp
group by
	pitcher,
	p_throws,
	stand,
	date_part('month' , game_date) ,
	date_part('year' , game_date)  ,
	date_part('week', game_date),
	inning_topbot = 'Bottom'
order by
	pitcher,
	p_throws,
	stand,
	date_part('month' , game_date) ,
	date_part('year' , game_date)  ,
	date_part('week', game_date),
	inning_topbot = 'Bottom'
);

create table league.monthly_pitcher_stats as (
select
	pitcher,
	p_throws,
	stand,
	date_part('month' , game_date) as month,
	date_part('year' , game_date) as year,
	inning_topbot = 'Bottom' as at_home,
	count(*) as pitches_thrown,
	sum((description = 'swinging_strike')::int) as swinging_strikes,
	sum((description = 'called_strike')::int) as called_strikes,
	sum((description = 'ball')::int) as overall_balls,
	max(at_bat_number) - min(at_bat_number) as at_bats,
	sum((pitch_type in ('FF', 'FT'))::int) as fastballs,
    sum((pitch_type in ('CH', 'FS'))::int) as split_or_change,
	sum((pitch_type not in ('CH', 'FS','FF', 'FT'))::int) as breaking_pitches,
	sum((description ='hit_into_play_no_out' and events != 'error')::int) as hits,
	sum((events = 'walk')::int) as walks,
	sum((events similar to '%strikeout')::int) as strikeouts,
	sum((events = 'hit_by_pitch')::int) as hit_by_pitch,
	sum((events = 'home_run')::int) as home_runs,
	sum((events = 'single')::int) as singles,
	sum((events = 'double')::int) as doubles,
	sum((events = 'triple')::int) as triple,
	sum((events similar to  '%double_play%')::int) as gidp,
	(sum(woba_value) filter (where woba_value != 'NaN'))::float as woba_numerator,
	(sum(woba_denom) filter (where woba_denom != 'NaN'))::float as woba_denomenator,
	(sum(babip_value) filter (where babip_value != 'NaN'))::float as babip_numerator,
	sum((babip_value != 'NaN')::int)::float as babip_denomenator,
	(sum(iso_value) filter (where iso_value != 'NaN'))::float as iso_numerator,
	sum(case when events = 'home_run' then 4
			 when events = 'tripple' then 3
			 when events = 'double' then 2
			 when events = 'single' then 1
			 else 0 END) as total_bases,
	avg(release_speed) filter (where pitch_type in ('FF', 'FT')) as avg_fastball_velo,
	avg(launch_angle) filter (where launch_angle != 'NaN') as average_launch_angle,
	avg(launch_speed) filter (where launch_speed != 'NaN')  as average_exit_velocity,
	sum((bb_type = 'ground_ball')::int) as ground_balls,
	sum((bb_type = 'line_drive')::int) as line_drives,
	sum((bb_type = 'fly_ball')::int) as fly_balls,
	sum((plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4)::int) as in_zone,
    sum((plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4 and type = 'S')::int) as swing_in_zone,
    sum((not (plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4) and (type = 'S'))::int) as swing_out_zone
from league.play_by_play pbp
group by
	pitcher,
	p_throws,
	stand,
	date_part('year' , game_date) ,
	date_part('month', game_date),
	inning_topbot = 'Bottom'
order by
	pitcher,
	p_throws,
	stand,
	date_part('year' , game_date)  ,
	date_part('month', game_date),
	inning_topbot = 'Bottom'
);




create table league.yearly_pitcher_stats as (
select
	pitcher,
	p_throws,
	stand,
	date_part('year' , game_date) as year,
	inning_topbot = 'Bottom' as at_home,
	count(*) as pitches_thrown,
	sum((description = 'swinging_strike')::int) as swinging_strikes,
	sum((description = 'called_strike')::int) as called_strikes,
	sum((description = 'ball')::int) as overall_balls,
	max(at_bat_number) - min(at_bat_number) as at_bats,
	sum((pitch_type in ('FF', 'FT'))::int) as fastballs,
    sum((pitch_type in ('CH', 'FS'))::int) as split_or_change,
	sum((pitch_type not in ('CH', 'FS','FF', 'FT'))::int) as breaking_pitches,
	sum((description ='hit_into_play_no_out' and events != 'error')::int) as hits,
	sum((events = 'walk')::int) as walks,
	sum((events similar to '%strikeout')::int) as strikeouts,
	sum((events = 'hit_by_pitch')::int) as hit_by_pitch,
	sum((events = 'home_run')::int) as home_runs,
	sum((events = 'single')::int) as singles,
	sum((events = 'double')::int) as doubles,
	sum((events = 'triple')::int) as triple,
	sum((events similar to  '%double_play%')::int) as gidp,
	(sum(woba_value) filter (where woba_value != 'NaN'))::float as woba_numerator,
	(sum(woba_denom) filter (where woba_denom != 'NaN'))::float as woba_denomenator,
	(sum(babip_value) filter (where babip_value != 'NaN'))::float as babip_numerator,
	sum((babip_value != 'NaN')::int)::float as babip_denomenator,
	(sum(iso_value) filter (where iso_value != 'NaN'))::float as iso_numerator,
	sum(case when events = 'home_run' then 4
			 when events = 'tripple' then 3
			 when events = 'double' then 2
			 when events = 'single' then 1
			 else 0 END) as total_bases,
	avg(release_speed) filter (where pitch_type in ('FF', 'FT')) as avg_fastball_velo,
	avg(launch_angle) filter (where launch_angle != 'NaN') as average_launch_angle,
	avg(launch_speed) filter (where launch_speed != 'NaN')  as average_exit_velocity,
	sum((bb_type = 'ground_ball')::int) as ground_balls,
	sum((bb_type = 'line_drive')::int) as line_drives,
	sum((bb_type = 'fly_ball')::int) as fly_balls,
	sum((plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4)::int) as in_zone,
    sum((plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4 and type = 'S')::int) as swing_in_zone,
    sum((not (plate_x >= -.85 and plate_x <= .85 and
            plate_z >= 1.6 and plate_z <= 3.4) and (type = 'S'))::int) as swing_out_zone
from league.play_by_play pbp
group by
	pitcher,
	p_throws,
	stand,
	date_part('year' , game_date)  ,
	inning_topbot = 'Bottom'
order by
	pitcher,
	p_throws,
	stand,
	date_part('year' , game_date)  ,
	inning_topbot = 'Bottom'
);









