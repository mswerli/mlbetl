drop table if exists league.pitch_coordinates;
create table league.pitch_coordinates as (
	select
		pitcher,
		batter,
		inning_topbot = 'Top' as pitcher_home,
		pitch_type,
		p_throws,
		stand ,
		plate_x,
		plate_z,
		pfx_x ,
		pfx_z ,
		"zone" ,
		release_speed,
		release_spin_rate,
		release_pos_x ,
		release_pos_y ,
		game_date ,
		balls,
		strikes,
		spin_dir,
		(plate_x >= -.85 and plate_x <= .85 and
	            plate_z >= 1.6 and plate_z <= 3.4)::int as in_zone
	from league.play_by_play
);

create table league.zone_outcomes as (

	select
		game_date,
		pitcher,
		p_throws,
		stand,
		pitch_type,
		zone,
		sum((description = 'swinging_strike')::int) as swinging_strikes,
		sum((description = 'called_strike')::int) as called_strikes,
		sum((description = 'ball')::int) as overall_balls,
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
		sum(in_zone::int) as in_zone,
	    sum((plate_x >= -.85 and plate_x <= .85 and
	            plate_z >= 1.6 and plate_z <= 3.4 and type = 'S')::int) as swing_in_zone,
	    sum((not (plate_x >= -.85 and plate_x <= .85 and
	            plate_z >= 1.6 and plate_z <= 3.4) and (type = 'S'))::int) as swing_out_zone
	from league.play_by_play
	group by
		game_date,
		pitcher,
		p_throws,
		stand,
		pitch_type,
		zone
)