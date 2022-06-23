with max_team_points as (
	select team_name as max_team_name,
	max(points) as max_points
	from {{ ref('nhl_players') }}
	group by team_name
	having max(points) >= 1
)
select 
team_name,
full_name,
points
from {{ ref('nhl_players') }}  -- or other tables
inner join max_team_points
on {{ ref('nhl_players') }}.points = max_team_points.max_points
and {{ ref('nhl_players') }}.team_name = max_team_points.max_team_name
order by points desc