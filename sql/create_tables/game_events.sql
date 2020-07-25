insert into league.runner_details (
with existing_ids as (
	select distinct runner_id from league.runner_details a
	),
	new_data as (
		select
			game_pk::varchar || '_' || atbatindex::varchar ||'_' ||  json_array_elements_text(runnerindex::json)::varchar as runner_id,
			game_pk,
			atbatindex,
			about_halfinning ,
			about_istopinning ,
			about_inning,
			count_balls,
			count_strikes,
			count_outs,
			matchup_batter_id,
			matchup_pitcher_id,
			matchup_batside_code,
			matchup_pitchhand_code,
			json_array_elements_text(pitchindex::json)::int as pitch_index,
			json_array_elements_text(actionindex::json)::int as action_index,
			json_array_elements_text(runnerindex::json)::int as runner_index,
			(json_array_elements(playevents) ->> 'index')::int as "index",
			json_array_elements(playevents) ->> 'pfxId' as pfxid,
			json_array_elements(playevents) ->> 'playId' as playid,
			(json_array_elements(playevents) ->> 'pitchNumber')::int as pitch_number,
			(json_array_elements(playevents) ->> 'startTime')::timestamp as start_time,
			(json_array_elements(playevents) ->> 'endTime')::timestamp as end_time,
			(json_array_elements(playevents) ->> 'isPitch')::boolean as is_pitch,
			json_array_elements(playevents) ->> 'type' as type_pitch,
			json_array_elements(runners) -> 'details' ->> 'event' as runners_details_event,
			json_array_elements(runners) -> 'details' ->> 'eventType' as runners_details_event_type,
			json_array_elements(runners) -> 'details' ->> 'movementReason' as runners_details_movement_reason,
			(json_array_elements(runners) -> 'details' -> 'runner' ->> 'id')::int as details_runner_id,
			json_array_elements(runners) -> 'details' -> 'runner' ->> 'fullName' as details_runner_full_name,
			json_array_elements(runners) -> 'details' -> 'runner' ->> 'link' as details_runner_link,
			(json_array_elements(runners) -> 'details' -> 'responsiblePitcher'->> 'id')::int as pitcher,
			json_array_elements(runners) -> 'details' -> 'responsiblePitcher'->> 'link' as runners_details_responsible_pitcher_link,
			(json_array_elements(runners) -> 'details' ->> 'isScoringEvent')::boolean as runners_details_responsible_is_scoring_event,
			(json_array_elements(runners) -> 'details' ->> 'rbi')::boolean as runners_details_responsible_rbi,
			(json_array_elements(runners) -> 'details' ->> 'earned')::boolean as earned,
			(json_array_elements(runners) -> 'details' ->> 'teamUnearned')::boolean as runners_details_responsible_team_unearned,
			(json_array_elements(runners) -> 'details' ->> 'playIndex')::int as runners_details_responsible_play_index
		from league.atbat a
		)
select a.* from new_data a
left join existing_ids b
on a.runner_id = b.runner_id
where a.runner_index is not null and b.runner_id is NULL
);

insert into league.event_details (
with existing_ids as (
	select distinct event_id from league.event_details
	),
	new_data as (
	select
		game_pk || '_' || atbatindex || '_' || (json_array_elements(playevents) ->> 'index')::int as event_id,
		game_pk,
		atbatindex,
		about_halfinning ,
		about_istopinning ,
		about_inning,
		count_balls,
		count_strikes,
		count_outs,
		matchup_batter_id,
		matchup_pitcher_id,
		matchup_batside_code,
		matchup_pitchhand_code,
		(json_array_elements(playevents) ->> 'index')::int as "index",
		json_array_elements_text(pitchindex::json)::int as pitch_index,
		json_array_elements_text(actionindex ::json)::int as action_index,
		json_array_elements_text(runnerindex ::json)::int as runner_index,
		json_array_elements(playevents) ->> 'pfxId' as pfxid,
		json_array_elements(playevents) ->> 'playId' as playid,
		(json_array_elements(playevents) ->> 'pitchNumber')::int as pitch_number,
		(json_array_elements(playevents) ->> 'startTime')::timestamp as start_time,
		(json_array_elements(playevents) ->> 'endTime')::timestamp as end_time,
		(json_array_elements(playevents) ->> 'isPitch')::boolean as is_pitch,
		json_array_elements(playevents) ->> 'type' as type_pitch,
		(json_array_elements(playevents) -> 'count' ->> 'strikes')::int as plaey_events_count_strikes,
		(json_array_elements(playevents) -> 'count' ->> 'balls')::int as player_events_count_ball,
		json_array_elements(playevents) -> 'details' -> 'call' -> 'code' as call_code,
		json_array_elements(playevents) -> 'details' -> 'call' -> 'description' as call_description,
		json_array_elements(playevents) -> 'details' -> 'description' as details_description,
		json_array_elements(playevents) -> 'details' -> 'code' as details_code,
		json_array_elements(playevents) -> 'details' -> 'ballColor' as details_ball_color,
		json_array_elements(playevents) -> 'details' -> 'trailColor' as details_trail_color,
		json_array_elements(playevents) -> 'details' -> 'isInPlay' as details_is_in_play,
		json_array_elements(playevents) -> 'details' -> 'isStrike' as details_is_strike,
		json_array_elements(playevents) -> 'details' -> 'isBall' as details_is_ball
	from league.atbat a
	)
select a.* from new_data a
left join existing_ids b
on a.event_id = b.event_id
where a."index" is not null and b.event_id is NULL
);


insert into league.pitch_data (
with existing_ids as (
	select distinct pitch_id from league.pitch_data
	),
	new_data as (
	select
		game_pk || '_' || atbatindex || '_' || (json_array_elements(playevents) ->> 'pitchNumber')::int as pitch_id,
		game_pk,
		atbatindex,
		about_halfinning ,
		about_istopinning ,
		about_inning,
		count_balls,
		count_strikes,
		count_outs,
		matchup_batter_id,
		matchup_pitcher_id,
		matchup_batside_code,
		matchup_pitchhand_code,
		(json_array_elements(playevents) ->> 'index')::int as "index",
		json_array_elements(playevents) ->> 'pfxId' as pfxid,
		json_array_elements(playevents) ->> 'playId' as playid,
		(json_array_elements(playevents) ->> 'pitchNumber')::int as pitch_number,
		(json_array_elements(playevents) ->> 'startTime')::timestamp as start_time,
		(json_array_elements(playevents) ->> 'endTime')::timestamp as end_time,
		(json_array_elements(playevents) ->> 'isPitch')::boolean as is_pitch,
		json_array_elements(playevents) ->> 'type' as type_pitch,
		json_array_elements_text(pitchindex::json)::int as pitch_index,
		json_array_elements_text(actionindex ::json)::int as action_index,
		json_array_elements_text(runnerindex ::json)::int as runner_index,
		(json_array_elements(playevents) -> 'pitchData' ->> 'startSpeed')::float as pitch_data_start_speed,
		(json_array_elements(playevents) -> 'pitchData' ->> 'endSpeed')::float as pitch_data_end_speed,
		(json_array_elements(playevents) -> 'pitchData' ->> 'strikeZoneTop')::float as pitch_data_strike_zone_top,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'strikeZoneBottom')::float as pitch_data_strike_zone_bottom,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->>  'aY')::float as pitch_data_coordinates_ay,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->>  'aZ')::float as pitch_data_coordinates_az,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'pfxX')::float as pitch_data_coordinates_pfxx,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'pfxZ')::float as pitch_data_coordinates_pfxz ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'pX')::float as pitch_data_coordinates_px ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'pZ')::float as pitch_data_coordinates_pz ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'vX0')::float as pitch_data_coordinates_vx0 ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'vY0')::float as pitch_data_coordinates_vy0 ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'vZ0')::float as pitch_data_coordinates_vz0 ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'x')::float as pitch_data_coordinates_x ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'y')::float as pitch_data_coordinates_y ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'x0')::float as pitch_data_coordinates_x0 ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'y0')::float as pitch_data_coordinates_y0 ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'z0')::float as pitch_data_coordinates_z0 ,
		(json_array_elements(playevents) -> 'pitchData' -> 'coordinates' ->> 'aX')::float as pitch_data_coordinates_ax,
		(json_array_elements(playevents) -> 'pitchData' -> 'breaks' ->> 'breakAngle')::float as pitch_data_breaks_angle,
		(json_array_elements(playevents) -> 'pitchData' -> 'breaks' ->> 'breakLength')::float as pitch_data_breaks_length ,
		(json_array_elements(playevents) -> 'pitchData' -> 'breaks' ->> 'breakY')::float as pitch_data_breaks_y,
		(json_array_elements(playevents) -> 'pitchData' -> 'breaks' ->> 'spinRate')::float as pitch_data_breaks_spin_rate,
		(json_array_elements(playevents) -> 'pitchData' -> 'breaks' ->> 'spinDirection')::float as pitch_data_breaks_spin_direction,
		(json_array_elements(playevents) -> 'pitchData' ->> 'zone')::int as pitch_data_zone,
		(json_array_elements(playevents) -> 'pitchData' ->> 'typeConfidence')::float as pitch_data_type_confidence,
		(json_array_elements(playevents) -> 'pitchData' ->> 'plateTime')::decimal as pitch_data_plate_time,
		(json_array_elements(playevents) -> 'pitchData' ->> 'extension')::decimal as pitch_data_extension
	from league.atbat a
	)
select a.* from new_data a
left join existing_ids b
on a.pitch_id = b.pitch_id
where a.pitch_id is not null and b.pitch_id is NULL
);

insert into league.hit_data (
with existing_ids as (
	select distinct hit_id from league.hit_data
	),
	new_data as (
		select
			game_pk || '_' || atbatindex || '_' || (json_array_elements(playevents) ->> 'pitchNumber')::int as hit_id,
			game_pk,
			atbatindex,
			about_halfinning ,
			about_istopinning ,
			about_inning,
			count_balls,
			count_strikes,
			count_outs,
			matchup_batter_id,
			matchup_pitcher_id,
			matchup_batside_code,
			matchup_pitchhand_code,
			(json_array_elements(playevents) ->> 'index')::int as "index",
			json_array_elements(playevents) ->> 'pfxId' as pfxid,
			json_array_elements(playevents) ->> 'playId' as playid,
			(json_array_elements(playevents) ->> 'pitchNumber')::int as pitch_number,
			(json_array_elements(playevents) ->> 'startTime')::timestamp as start_time,
			(json_array_elements(playevents) ->> 'endTime')::timestamp as end_time,
			(json_array_elements(playevents) ->> 'isPitch')::boolean as is_pitch,
			json_array_elements(playevents) ->> 'type' as type_pitch,
			json_array_elements_text(pitchindex::json)::int as pitch_index,
			json_array_elements_text(actionindex ::json)::int as action_index,
			json_array_elements_text(runnerindex ::json)::int as runner_index,
			(json_array_elements(playevents) -> 'hitData' ->> 'launchSpeed')::float as hit_data_launch_speed,
			(json_array_elements(playevents) -> 'hitData' ->> 'launchAngle')::float as hit_data_launch_angle,
			(json_array_elements(playevents) -> 'hitData' ->> 'totalDistance')::float as hit_data_total_distance,
			json_array_elements(playevents) -> 'hitData' ->> 'trajectory' as hit_data_trajectory,
			json_array_elements(playevents) -> 'hitData' ->> 'hardness' as hit_data_hardness,
			(json_array_elements(playevents) -> 'hitData' ->> 'location')::int as hit_data_location,
			(json_array_elements(playevents) -> 'hitData' -> 'coordinates' ->> 'coordX')::float as hit_data_coordinates_coordX,
			(json_array_elements(playevents) -> 'hitData' -> 'coordinates' ->> 'coordY')::float as hit_data_coordinates_coordY
		from league.atbat a
		)
select a.* from new_data a
left join existing_ids b
on a.hit_id = b.hit_id
where a.hit_id is not null and b.hit_id is NULL
);

insert into league.runner_credits (
with existing_ids as (
	select distinct runner_id from league.runner_credits
	),
	new_data as (
	select
		game_pk::varchar || '_' || atbatindex::varchar ||'_' ||  json_array_elements_text(runnerindex::json)::varchar as runner_id,
		game_pk,
		atbatindex,
		about_halfinning ,
		about_istopinning ,
		about_inning,
		count_balls,
		count_strikes,
		count_outs,
		matchup_batter_id,
		matchup_pitcher_id,
		matchup_batside_code,
		matchup_pitchhand_code,
		json_array_elements_text(pitchindex::json)::int as pitch_index,
		json_array_elements_text(actionindex ::json)::int as action_index,
		json_array_elements_text(runnerindex ::json)::int as runner_index,
		(json_array_elements(playevents) ->> 'index')::int as "index",
		json_array_elements(playevents) ->> 'pfxId' as pfxid,
		json_array_elements(playevents) ->> 'playId' as playid,
		(json_array_elements(playevents) ->> 'pitchNumber')::int as pitch_number,
		(json_array_elements(playevents) ->> 'startTime')::timestamp as start_time,
		(json_array_elements(playevents) ->> 'endTime')::timestamp as end_time,
		(json_array_elements(playevents) ->> 'isPitch')::boolean as is_pitch,
		json_array_elements(playevents) ->> 'type' as type_pitch,
		(json_array_elements(json_array_elements(runners) -> 'credits') -> 'player' ->> 'id')::int as runners_credits_player_id,
		json_array_elements(json_array_elements(runners) -> 'credits') -> 'player' ->> 'link' as runners_credits_player_link,
		(json_array_elements(json_array_elements(runners) -> 'credits') -> 'position' ->> 'code')::int as runners_credits_position_code,
		json_array_elements(json_array_elements(runners) -> 'credits') -> 'position' ->> 'name' as runners_credits_position_name,
		json_array_elements(json_array_elements(runners) -> 'credits') -> 'position' ->> 'type' as runners_credits_position_type,
		json_array_elements(json_array_elements(runners) -> 'credits') -> 'position' ->> 'abbreviation'  as runners_credits_position_abbreviation,
		json_array_elements(json_array_elements(runners) -> 'credits') ->> 'credit' as runners_credits_credit
	from league.atbat a
	)
select a.* from new_data a
left join existing_ids b
on a.runner_id = b.runner_id
where a.runner_id is not null and b.runner_id is NULL

);


insert into league.matchups (
with existing_ids as (
	select distinct matchup_id from league.matchups
)

select distinct
	game_pk || '_' || '_' || atbatindex as matchup_id,
	game_pk,
	atbatindex ,
	result_type ,
	playendtime ,
	result_event ,
	result_eventtype ,
	result_description ,
	result_rbi ,
	result_homescore ,
	about_atbatindex ,
	about_captivatingindex ,
	about_endtime ,
	about_halfinning ,
	about_hasout ,
	about_hasreview ,
	about_inning ,
	about_iscomplete ,
	about_isscoringplay ,
	about_istopinning ,
	about_starttime ,
	count_balls ,
	count_strikes ,
	count_outs ,
	matchup_batside_code ,
	matchup_batside_description ,
	matchup_batter_fullname ,
	matchup_batter_id ,
	matchup_batter_link ,
	matchup_batterhotcoldzones ,
	matchup_batterhotcoldzonestats_stats,
	matchup_pitcher_fullname ,
	matchup_pitcher_id ,
	matchup_pitcher_link ,
	matchup_pitcherhotcoldzones ,
	matchup_pitcherhotcoldzonestats_stats,
	matchup_pitchhand_code ,
	matchup_pitchhand_description ,
	matchup_postonfirst_fullname ,
	matchup_postonfirst_id ,
	matchup_postonfirst_link ,
	matchup_postonsecond_fullname ,
	matchup_postonsecond_id ,
	matchup_postonsecond_link,
	matchup_postonthird_fullname ,
	matchup_postonthird_id ,
	matchup_postonthird_link,
	matchup_splits_batter ,
	matchup_splits_menonbase ,
	matchup_splits_pitcher
from league.atbat a
left join existing_ids b
on a.game_pk || '_' || '_' || atbatindex = b.matchup_id
where b.matchup_id is NULL
)


