-- soccer.match_defense_stats source

CREATE OR REPLACE VIEW soccer.match_defense_stats
AS SELECT p.player,
    sq.squad,
    squ.squad AS opponent,
    summary.minutes,
    mss.id AS player_match_id,
    mss.tackles_att,
    mss.tackles_won,
    mss.tackles_def_third,
    mss.tackles_mid_third,
    mss.tackles_att_third,
    mss.challenges_won,
    mss.challenges_lost,
    mss.challenges_att,
    mss.blocks,
    mss.shot_blocks,
    mss.pass_blocks,
    mss.interceptions,
    mss.clearances,
    mss.errors_lead_to_shot
   FROM soccer.player_match_defense_stats mss
     LEFT JOIN soccer.match_report_ids mri ON mss.id::text = mri.id::text
     LEFT JOIN soccer.players p ON p.id = mri.player_id::text
     LEFT JOIN soccer.squads sq ON sq.id = mri.squad_id::text
     LEFT JOIN soccer.squads squ ON squ.id = mri.opponent_id::text
     LEFT JOIN soccer.player_match_summary_stats summary ON summary.id::text = mss.id::text;


-- soccer.match_misc_stats source

CREATE OR REPLACE VIEW soccer.match_misc_stats
AS SELECT p.player,
    sq.squad,
    squ.squad AS opponent,
    summary.minutes,
    mss.id AS player_match_id,
    mss.yellow_cards,
    mss.red_cards,
    mss.second_yellow_cards,
    mss.fouls,
    mss.fouled,
    mss.offsides,
    mss.crosses,
    mss.interceptions,
    mss.tackles_won,
    mss.pks_won,
    mss.pks_converted,
    mss.own_goals,
    mss.ball_recoveries,
    mss.aerial_duels_won,
    mss.aerial_duels_lost
   FROM soccer.player_match_misc_stats mss
     LEFT JOIN soccer.match_report_ids mri ON mss.id::text = mri.id::text
     LEFT JOIN soccer.players p ON p.id = mri.player_id::text
     LEFT JOIN soccer.squads sq ON sq.id = mri.squad_id::text
     LEFT JOIN soccer.squads squ ON squ.id = mri.opponent_id::text
     LEFT JOIN soccer.player_match_summary_stats summary ON summary.id::text = mss.id::text;


-- soccer.match_passing_type_stats source

CREATE OR REPLACE VIEW soccer.match_passing_type_stats
AS SELECT p.player,
    sq.squad,
    squ.squad AS opponent,
    summary.minutes,
    mss.id AS player_match_id,
    mss.passes_attempted,
    mss.passes_completed,
    mss.passes_live,
    mss.passes_dead_ball,
    mss.passes_free_kick,
    mss.passes_through_balls,
    mss.passes_switches,
    mss.passes_throw_ins,
    mss.passes_corner_kicks,
    mss.corner_kicks_inswinging,
    mss.corner_kicks_outswinging,
    mss.corner_kicks_straight,
    mss.passes_offside,
    mss.passes_blocked
   FROM soccer.player_match_passing_types_stats mss
     LEFT JOIN soccer.match_report_ids mri ON mss.id::text = mri.id::text
     LEFT JOIN soccer.players p ON p.id = mri.player_id::text
     LEFT JOIN soccer.squads sq ON sq.id = mri.squad_id::text
     LEFT JOIN soccer.squads squ ON squ.id = mri.opponent_id::text
     LEFT JOIN soccer.player_match_summary_stats summary ON summary.id::text = mss.id::text;


-- soccer.match_possession_stats source

CREATE OR REPLACE VIEW soccer.match_possession_stats
AS SELECT p.player,
    sq.squad,
    squ.squad AS opponent,
    summary.minutes,
    mss.id AS player_match_id,
    mss.touches,
    mss.touches_def_penalty_area,
    mss.touches_def_third,
    mss.touches_mid_third,
    mss.touches_att_third,
    mss.touches_att_penalty_area,
    mss.take_ons_tackled,
    mss.total_carries_distance,
    mss.total_progressive_carries_distance,
    mss.carries_into_final_third,
    mss.carries_into_penalty_area,
    mss.carries_miscontrolled,
    mss.carries_disposessed,
    mss.passes_recieved,
    mss.progressive_passes_recieved
   FROM soccer.player_match_possession_stats mss
     LEFT JOIN soccer.match_report_ids mri ON mss.id::text = mri.id::text
     LEFT JOIN soccer.players p ON p.id = mri.player_id::text
     LEFT JOIN soccer.squads sq ON sq.id = mri.squad_id::text
     LEFT JOIN soccer.squads squ ON squ.id = mri.opponent_id::text
     LEFT JOIN soccer.player_match_summary_stats summary ON summary.id::text = mss.id::text;


-- soccer.match_stats source

CREATE OR REPLACE VIEW soccer.match_stats
AS SELECT p.player,
    sq.squad,
    sch.match_date,
    co.competition,
    sch.season,
    sch.venue,
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
    sch.id AS match_id
   FROM soccer.player_match_summary_stats su
     LEFT JOIN soccer.match_report_ids mr ON mr.id::text = su.id::text
     LEFT JOIN soccer.players p ON p.id = mr.player_id::text
     LEFT JOIN soccer.squads sq ON sq.id = mr.squad_id::text
     LEFT JOIN soccer.schedules sch ON sch.id::text = mr.match_id::text
     LEFT JOIN soccer.competitions co ON co.id = sch.competition_id::text
     LEFT JOIN soccer.player_match_passing_stats pa ON pa.id::text = su.id::text
     LEFT JOIN soccer.player_match_passing_types_stats pt ON pt.id::text = su.id::text
     LEFT JOIN soccer.player_match_possession_stats po ON po.id::text = su.id::text
     LEFT JOIN soccer.player_match_defense_stats de ON de.id::text = su.id::text
     LEFT JOIN soccer.player_match_misc_stats mi ON mi.id::text = su.id::text;


-- soccer.match_summary_stats source

CREATE OR REPLACE VIEW soccer.match_summary_stats
AS SELECT p.player,
    sq.squad,
    squ.squad AS opponent,
    mss.id AS player_match_id,
    mss.minutes,
    mss.goals,
    mss.assists,
    mss.pk_goals,
    mss.pk_attempts,
    mss.shots,
    mss.shots_on_target,
    mss.yellow_cards,
    mss.red_cards,
    mss.touches,
    mss.tackles,
    mss.interceptions,
    mss.blocks,
    mss.xg,
    mss.npxg,
    mss.xag,
    mss.shot_creating_actions,
    mss.goal_creating_actions,
    mss.passes_completed,
    mss.passes_attempted,
    mss.progressive_passes,
    mss.carries,
    mss.progressive_carries,
    mss.take_ons_attempted,
    mss.take_ons_succeeded
   FROM soccer.player_match_summary_stats mss
     LEFT JOIN soccer.match_report_ids mri ON mss.id::text = mri.id::text
     LEFT JOIN soccer.players p ON p.id = mri.player_id::text
     LEFT JOIN soccer.squads sq ON sq.id = mri.squad_id::text
     LEFT JOIN soccer.squads squ ON squ.id = mri.opponent_id::text;


-- soccer.player_appearances source

CREATE OR REPLACE VIEW soccer.player_appearances
AS SELECT mri.id AS player_match_id,
    p.player,
    sq.squad,
    sc.match_date
   FROM soccer.match_report_ids mri
     LEFT JOIN soccer.players p ON p.id = mri.player_id::text
     LEFT JOIN soccer.squads sq ON sq.id = mri.squad_id::text
     LEFT JOIN soccer.schedules sc ON sc.id::text = mri.match_id::text;


-- soccer.player_competition_ranks source

CREATE OR REPLACE VIEW soccer.player_competition_ranks
AS SELECT match_stats.player,
    match_stats.squad,
    match_stats.season,
    match_stats.competition,
    sum(match_stats.goals) AS goals,
    rank() OVER (PARTITION BY match_stats.competition, match_stats.season ORDER BY (sum(match_stats.goals)) DESC) AS goals_rank,
    sum(match_stats.xg) AS xg,
    rank() OVER (PARTITION BY match_stats.competition, match_stats.season ORDER BY (sum(match_stats.xg)) DESC) AS xg_rank,
    sum(match_stats.assists) AS assists,
    rank() OVER (PARTITION BY match_stats.competition, match_stats.season ORDER BY (sum(match_stats.assists)) DESC) AS assists_rank
   FROM soccer.match_stats
  GROUP BY match_stats.player, match_stats.squad, match_stats.season, match_stats.competition;
