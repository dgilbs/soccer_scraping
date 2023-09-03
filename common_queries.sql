-- look at team results by formation used and season
select
squad,
extract(year from match_date) season,
formation,
count(distinct match_id) matches,
count(*) filter (where match_result = 'W') wins,
count(*) filter (where match_result = 'L') losses,
count(*) filter (where match_result = 'D') draws,
round(sum(goals_for::numeric)/count(distinct match_id), 2) goals_per_match,
round(sum(goals_against::numeric)/count(distinct match_id), 2) goals_allowed_per_match,
round(sum(goals_for::numeric)/count(distinct match_id) - sum(goals_against::numeric)/count(distinct match_id), 2) gd_per_match,
round(sum(xg_for::numeric)/count(distinct match_id), 2) xg_per_match,
round(sum(xg_against::numeric)/count(distinct match_id), 2) xg_against_per_match,
round(sum(xg_for::numeric)/count(distinct match_id) - sum(xg_against::numeric)/count(distinct match_id), 2) xg_diff_per_match
from soccer.team_schedules
group by 1,2,3
order by 13 desc;
