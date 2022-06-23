select distinct on (nhl_player_id)
  -- * -- TODO replace this with correct columns
  nhl_player_id as id,
  full_name,
  game_team_name as team_name,
  sum(stats_assists) as assists,
  sum(stats_goals) as goals,
  sum(stats_assists + stats_goals) as points
-- from game_stats --or whatever other table reference
from {{ ref('player_game_stats') }}
group by id, full_name, team_name
order by nhl_player_id, team_name desc