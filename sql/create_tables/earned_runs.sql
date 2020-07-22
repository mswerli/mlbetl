create table league.earned_runs as (
select
	game_pk,
	matchup_batside_code,
	pitcher,
	about_inning ,
	about_halfinning ,
	sum(earned::int)
from league.runner_details
where pitcher is not null
group by 	game_pk,
	matchup_batside_code,
	pitcher,
	about_inning ,
	about_halfinning
)