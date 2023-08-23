CREATE OR REPLACE FUNCTION soccer.full_staging_updates()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
begin
	perform soccer.update_player_match_reports();
	perform soccer.opponent_season_stats();

end;

$function$
;

CREATE OR REPLACE FUNCTION soccer.metric_competition_lookup(competition_name text, competition_squad text, comp_season text)
 RETURNS TABLE(player_name character varying, squad_name character varying, comp_goals integer, comp_rank integer)
 LANGUAGE plpgsql
AS $function$
 begin
 return query
with full_data as (
select
	player,
	squad,
	sum(goals) goals,
	rank() over(order by sum(goals) desc) competition_rank
from
	soccer.match_stats
where
	competition = competition_name and season = comp_season
group by
	1,2
order by
	3 desc
)
select * from full_data where squad = competition_squad;
 end;
 $function$
;

CREATE OR REPLACE FUNCTION soccer.opponent_season_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
begin
	truncate table soccer.st_opponent_season_reports;

	insert into soccer.st_opponent_season_reports(
	select
	sq.id squad_id,
	c.id competition_id,
	sch.season,
	count(distinct st.match_id) matches,
	sum(st.goals) opponent_goals,
	sum(st.xg) opponent_xg,
	sum(st.passes_completed) opponent_passes_completed,
	sum(st.passes_attempted) opponent_passes_attempted,
	sum(st.progressive_passes) oppenent_progressive_passes
	from
	soccer.match_report_ids mri
	join soccer.st_player_match_reports st
	on st.id = mri.id
	join soccer.squads sq
	on sq.id  = mri.opponent_id
	join soccer.schedules sch
	on sch.id = mri.match_id
	join soccer.competitions c
	on sch.competition_id = c.id
	group by 1,2,3
	);
end;
$function$
;

CREATE OR REPLACE FUNCTION soccer.player_stats(input_player character varying)
 RETURNS TABLE(soccer_player character varying, minutes integer)
 LANGUAGE plpgsql
AS $function$
 begin
return query
 select
	player,
	goals
 from soccer.match_stats
 where player = input_player
 ;
 end;
 $function$
;

CREATE OR REPLACE FUNCTION soccer.team_season_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
begin
	truncate table soccer.st_team_season_reports;

	insert into soccer.st_team_season_reports(
	select
	sq.id squad_id,
	c.id competition_id,
	sch.season,
	count(distinct st.match_id) matches,
	sum(st.goals) goals,
	sum(st.xg) xg,
	sum(st.passes_completed) passes_completed,
	sum(st.passes_attempted) passes_attempted,
	sum(st.progressive_passes) progressive_passes
	from
	soccer.match_report_ids mri
	join soccer.st_player_match_reports st
	on st.id = mri.id
	join soccer.squads sq
	on sq.id  = mri.squad_id
	join soccer.schedules sch
	on sch.id = mri.match_id
	join soccer.competitions c
	on sch.competition_id = c.id
	group by 1,2,3
	);
end;

$function$
;

CREATE OR REPLACE FUNCTION soccer.update_player_match_reports()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
 begin
 truncate table soccer.st_player_match_reports;
 insert into soccer.st_player_match_reports (
 SELECT
    su.id,
    su.minutes,
    su.goals,
    su.assists,
    su.pk_goals,
    su.pk_attempts,
    su.shots,
    su.shots_on_target,
    su.yellow_cards,
    su.red_cards,
    su.touches,
    su.tackles,
    su.interceptions,
    su.blocks,
    su.xg,
    su.npxg,
    su.xag,
    su.shot_creating_actions,
    su.goal_creating_actions,
    su.passes_completed,
    su.passes_attempted,
    su.progressive_passes,
    su.carries,
    su.progressive_carries,
    su.take_ons_attempted,
    su.take_ons_succeeded,
    pa.total_pass_distance,
    pa.total_progressive_pass_distance,
    pa.short_passes_completed,
    pa.short_passes_attempted,
    pa.medium_passes_completed,
    pa.medium_passes_attempted,
    pa.long_passes_completed,
    pa.long_passes_attempted,
    pa.xa,
    pa.key_passes,
    pa.passes_into_final_third,
    pa.crosses_into_penalty_area,
    pt.passes_live,
    pt.passes_dead_ball,
    pt.passes_free_kick,
    pt.passes_through_balls,
    pt.passes_switches,
    pt.passes_throw_ins,
    pt.passes_corner_kicks,
    pt.corner_kicks_inswinging,
    pt.corner_kicks_outswinging,
    pt.corner_kicks_straight,
    pt.passes_offside,
    pt.passes_blocked,
    po.touches_def_penalty_area,
    po.touches_def_third,
    po.touches_mid_third,
    po.touches_att_third,
    po.touches_att_penalty_area,
    po.take_ons_tackled,
    po.total_carries_distance,
    po.total_progressive_carries_distance,
    po.carries_into_final_third,
    po.carries_into_penalty_area,
    po.carries_miscontrolled,
    po.carries_disposessed,
    po.passes_recieved,
    po.progressive_passes_recieved,
    de.tackles_att,
    de.tackles_won,
    de.tackles_def_third,
    de.tackles_mid_third,
    de.tackles_att_third,
    de.challenges_won,
    de.challenges_lost,
    de.challenges_att,
    de.shot_blocks,
    de.pass_blocks,
    de.clearances,
    de.errors_lead_to_shot,
    mi.second_yellow_cards,
    mi.fouls,
    mi.fouled,
    mi.offsides,
    mi.crosses,
    mi.pks_won,
    mi.pks_converted,
    mi.own_goals,
    mi.ball_recoveries,
    mi.aerial_duels_won,
    mi.aerial_duels_lost,
    mri.match_id
   FROM soccer.player_match_summary_stats su
     LEFT JOIN soccer.player_match_passing_stats pa ON pa.id::text = su.id::text
     LEFT JOIN soccer.player_match_passing_types_stats pt ON pt.id::text = su.id::text
     LEFT JOIN soccer.player_match_possession_stats po ON po.id::text = su.id::text
     LEFT JOIN soccer.player_match_defense_stats de ON de.id::text = su.id::text
     LEFT JOIN soccer.player_match_misc_stats mi ON mi.id::text = su.id::text
     left join soccer.match_report_ids mri on mri.id::text = su.id::text
 )
 ;
 end;
 $function$
;
