-- soccer.competitions definition

-- Drop table

-- DROP TABLE soccer.competitions;

CREATE TABLE soccer.competitions (
	id text NOT NULL,
	competition varchar(100) NOT NULL,
	gender varchar(10) NOT NULL,
	CONSTRAINT competition_id_constraint PRIMARY KEY (id)
);


-- soccer.match_report_ids definition

-- Drop table

-- DROP TABLE soccer.match_report_ids;

CREATE TABLE soccer.match_report_ids (
	id varchar(50) NOT NULL,
	player_id varchar(30) NULL,
	squad_id varchar(30) NULL,
	match_id varchar(20) NULL,
	opponent_id varchar(20) NULL,
	shirtnumber int4 NULL,
	age varchar(10) NULL,
	"position" varchar(20) NULL,
	CONSTRAINT match_report_ids_pkey PRIMARY KEY (id)
);


-- soccer.match_shot_creation_data definition

-- Drop table

-- DROP TABLE soccer.match_shot_creation_data;

CREATE TABLE soccer.match_shot_creation_data (
	shot_id varchar(50) NOT NULL,
	match_id varchar(20) NULL,
	shot_player_match_id varchar(50) NULL,
	sca_1_player_match_id varchar(50) NULL,
	sca_2_player_match_id varchar(50) NULL,
	squad_id varchar(20) NULL,
	"minute" int4 NULL,
	stoppage_minute int4 NULL,
	xg float8 NULL,
	psxg float8 NULL,
	outcome varchar(20) NULL,
	distance int4 NULL,
	body_part varchar(20) NULL,
	sca_1_event varchar(20) NULL,
	sca_2_event varchar(20) NULL,
	on_target bool NULL,
	is_free_kick bool NULL,
	is_deflected bool NULL,
	is_volley bool NULL,
	CONSTRAINT match_shot_creation_data_pkey PRIMARY KEY (shot_id)
);


-- soccer.per_90_stats definition

-- Drop table

-- DROP TABLE soccer.per_90_stats;

CREATE TABLE soccer.per_90_stats (
	player varchar(255) NULL,
	goals numeric NULL
);


-- soccer.player_match_defense_stats definition

-- Drop table

-- DROP TABLE soccer.player_match_defense_stats;

CREATE TABLE soccer.player_match_defense_stats (
	id varchar(100) NOT NULL,
	tackles_att int4 NULL,
	tackles_won int4 NULL,
	tackles_def_third int4 NULL,
	tackles_mid_third int4 NULL,
	tackles_att_third int4 NULL,
	challenges_won int4 NULL,
	challenges_lost int4 NULL,
	challenges_att int4 NULL,
	blocks int4 NULL,
	shot_blocks int4 NULL,
	pass_blocks int4 NULL,
	interceptions int4 NULL,
	clearances int4 NULL,
	errors_lead_to_shot int4 NULL,
	CONSTRAINT player_match_defense_stats_pkey PRIMARY KEY (id)
);


-- soccer.player_match_misc_stats definition

-- Drop table

-- DROP TABLE soccer.player_match_misc_stats;

CREATE TABLE soccer.player_match_misc_stats (
	id varchar(100) NOT NULL,
	yellow_cards int4 NULL,
	red_cards int4 NULL,
	second_yellow_cards int4 NULL,
	fouls int4 NULL,
	fouled int4 NULL,
	offsides int4 NULL,
	crosses int4 NULL,
	interceptions int4 NULL,
	tackles_won int4 NULL,
	pks_won int4 NULL,
	pks_converted int4 NULL,
	own_goals int4 NULL,
	ball_recoveries int4 NULL,
	aerial_duels_won int4 NULL,
	aerial_duels_lost int4 NULL,
	CONSTRAINT player_match_misc_stats_pkey PRIMARY KEY (id)
);


-- soccer.player_match_passing_stats definition

-- Drop table

-- DROP TABLE soccer.player_match_passing_stats;

CREATE TABLE soccer.player_match_passing_stats (
	id varchar(100) NOT NULL,
	passes_completed int4 NULL,
	passes_attempted int4 NULL,
	total_pass_distance int4 NULL,
	total_progressive_pass_distance int4 NULL,
	short_passes_completed int4 NULL,
	short_passes_attempted int4 NULL,
	medium_passes_completed int4 NULL,
	medium_passes_attempted int4 NULL,
	long_passes_completed int4 NULL,
	long_passes_attempted int4 NULL,
	assists int4 NULL,
	xag float8 NULL,
	xa float8 NULL,
	key_passes int4 NULL,
	passes_into_final_third int4 NULL,
	crosses_into_penalty_area int4 NULL,
	progressive_passes int4 NULL,
	CONSTRAINT player_match_passing_stats_pkey PRIMARY KEY (id)
);


-- soccer.player_match_passing_types_stats definition

-- Drop table

-- DROP TABLE soccer.player_match_passing_types_stats;

CREATE TABLE soccer.player_match_passing_types_stats (
	id varchar(100) NOT NULL,
	passes_attempted int4 NULL,
	passes_completed int4 NULL,
	passes_live int4 NULL,
	passes_dead_ball int4 NULL,
	passes_free_kick int4 NULL,
	passes_through_balls int4 NULL,
	passes_switches int4 NULL,
	passes_throw_ins int4 NULL,
	passes_corner_kicks int4 NULL,
	corner_kicks_inswinging int4 NULL,
	corner_kicks_outswinging int4 NULL,
	corner_kicks_straight int4 NULL,
	passes_offside int4 NULL,
	passes_blocked int4 NULL,
	CONSTRAINT player_match_passing_type_stats_pkey PRIMARY KEY (id)
);


-- soccer.player_match_possession_stats definition

-- Drop table

-- DROP TABLE soccer.player_match_possession_stats;

CREATE TABLE soccer.player_match_possession_stats (
	id varchar(100) NOT NULL,
	touches int4 NULL,
	touches_def_penalty_area int4 NULL,
	touches_def_third int4 NULL,
	touches_mid_third int4 NULL,
	touches_att_third int4 NULL,
	touches_att_penalty_area int4 NULL,
	take_ons_tackled int4 NULL,
	total_carries_distance int4 NULL,
	total_progressive_carries_distance int4 NULL,
	carries_into_final_third int4 NULL,
	carries_into_penalty_area int4 NULL,
	carries_miscontrolled int4 NULL,
	carries_disposessed int4 NULL,
	passes_recieved int4 NULL,
	progressive_passes_recieved int4 NULL,
	CONSTRAINT player_match_possession_stats_pkey PRIMARY KEY (id)
);


-- soccer.player_match_summary_stats definition

-- Drop table

-- DROP TABLE soccer.player_match_summary_stats;

CREATE TABLE soccer.player_match_summary_stats (
	id varchar(100) NOT NULL,
	minutes int4 NULL,
	goals int4 NULL,
	assists int4 NULL,
	pk_goals int4 NULL,
	pk_attempts int4 NULL,
	shots int4 NULL,
	shots_on_target int4 NULL,
	yellow_cards int4 NULL,
	red_cards int4 NULL,
	touches int4 NULL,
	tackles int4 NULL,
	interceptions int4 NULL,
	blocks int4 NULL,
	xg float8 NULL,
	npxg float8 NULL,
	xag float8 NULL,
	shot_creating_actions int4 NULL,
	goal_creating_actions int4 NULL,
	passes_completed int4 NULL,
	passes_attempted int4 NULL,
	progressive_passes int4 NULL,
	carries int4 NULL,
	progressive_carries int4 NULL,
	take_ons_attempted int4 NULL,
	take_ons_succeeded int4 NULL,
	CONSTRAINT player_match_summary_stats_pkey PRIMARY KEY (id)
);


-- soccer.players definition

-- Drop table

-- DROP TABLE soccer.players;

CREATE TABLE soccer.players (
	id text NOT NULL,
	player varchar(255) NOT NULL,
	CONSTRAINT unique_player_id PRIMARY KEY (id)
);


-- soccer.schedules definition

-- Drop table

-- DROP TABLE soccer.schedules;

CREATE TABLE soccer.schedules (
	id varchar(10) NOT NULL,
	competition_id varchar(10) NULL,
	home_team_id varchar(20) NULL,
	away_team_id varchar(20) NULL,
	wk varchar(5) NULL,
	match_date date NULL,
	venue varchar(50) NULL,
	home_goals varchar(10) NULL,
	away_goals varchar(10) NULL,
	home_xg float8 NULL,
	away_xg float8 NULL,
	attendance int4 NULL,
	comp_round text NULL,
	notes text NULL,
	season varchar(20) NULL,
	CONSTRAINT match_id_constraint PRIMARY KEY (id)
);


-- soccer.shot_creating_actions definition

-- Drop table

-- DROP TABLE soccer.shot_creating_actions;

CREATE TABLE soccer.shot_creating_actions (
	sca_id varchar(100) NOT NULL,
	shot_id varchar(100) NULL,
	player_match_id varchar(100) NULL,
	sca_event varchar(20) NULL,
	squad_id varchar(20) NULL,
	event_order int4 NULL,
	"minute" int4 NULL,
	stoppage_minute int4 NULL,
	shooter varchar(50) NULL,
	outcome varchar(50) NULL,
	sca_fouled bool NULL,
	sca_interception bool NULL,
	sca_pass_live bool NULL,
	sca_shot bool NULL,
	sca_tackle bool NULL,
	sca_take_on bool NULL,
	CONSTRAINT shot_creating_actions_pkey PRIMARY KEY (sca_id)
);


-- soccer.shots definition

-- Drop table

-- DROP TABLE soccer.shots;

CREATE TABLE soccer.shots (
	shot_id varchar(100) NOT NULL,
	shot_player_match_id varchar(100) NULL,
	"minute" int4 NULL,
	stoppage_minute int4 NULL,
	squad_id varchar(20) NULL,
	xg float8 NULL,
	psxg float8 NULL,
	outcome varchar(20) NULL,
	on_target bool NULL,
	is_free_kick bool NULL,
	is_deflected bool NULL,
	is_volley bool NULL,
	CONSTRAINT shots_pkey PRIMARY KEY (shot_id)
);


-- soccer.squads definition

-- Drop table

-- DROP TABLE soccer.squads;

CREATE TABLE soccer.squads (
	id text NOT NULL,
	squad varchar(255) NOT NULL,
	gender varchar(10) NOT NULL,
	CONSTRAINT squad_id_constraint PRIMARY KEY (id)
);


-- soccer.st_opponent_season_reports definition

-- Drop table

-- DROP TABLE soccer.st_opponent_season_reports;

CREATE TABLE soccer.st_opponent_season_reports (
	squad_id text NULL,
	competition_id text NULL,
	season varchar(20) NULL,
	matches int8 NULL,
	opponent_goals int8 NULL,
	opponent_xg float8 NULL,
	opponent_passes_completed int8 NULL,
	opponent_passes_attempted int8 NULL,
	oppenent_progressive_passes int8 NULL
);


-- soccer.st_player_match_reports definition

-- Drop table

-- DROP TABLE soccer.st_player_match_reports;

CREATE TABLE soccer.st_player_match_reports (
	id varchar(100) NULL,
	minutes int4 NULL,
	goals int4 NULL,
	assists int4 NULL,
	pk_goals int4 NULL,
	pk_attempts int4 NULL,
	shots int4 NULL,
	shots_on_target int4 NULL,
	yellow_cards int4 NULL,
	red_cards int4 NULL,
	touches int4 NULL,
	tackles int4 NULL,
	interceptions int4 NULL,
	blocks int4 NULL,
	xg float8 NULL,
	npxg float8 NULL,
	xag float8 NULL,
	shot_creating_actions int4 NULL,
	goal_creating_actions int4 NULL,
	passes_completed int4 NULL,
	passes_attempted int4 NULL,
	progressive_passes int4 NULL,
	carries int4 NULL,
	progressive_carries int4 NULL,
	take_ons_attempted int4 NULL,
	take_ons_succeeded int4 NULL,
	total_pass_distance int4 NULL,
	total_progressive_pass_distance int4 NULL,
	short_passes_completed int4 NULL,
	short_passes_attempted int4 NULL,
	medium_passes_completed int4 NULL,
	medium_passes_attempted int4 NULL,
	long_passes_completed int4 NULL,
	long_passes_attempted int4 NULL,
	xa float8 NULL,
	key_passes int4 NULL,
	passes_into_final_third int4 NULL,
	crosses_into_penalty_area int4 NULL,
	passes_live int4 NULL,
	passes_dead_ball int4 NULL,
	passes_free_kick int4 NULL,
	passes_through_balls int4 NULL,
	passes_switches int4 NULL,
	passes_throw_ins int4 NULL,
	passes_corner_kicks int4 NULL,
	corner_kicks_inswinging int4 NULL,
	corner_kicks_outswinging int4 NULL,
	corner_kicks_straight int4 NULL,
	passes_offside int4 NULL,
	passes_blocked int4 NULL,
	touches_def_penalty_area int4 NULL,
	touches_def_third int4 NULL,
	touches_mid_third int4 NULL,
	touches_att_third int4 NULL,
	touches_att_penalty_area int4 NULL,
	take_ons_tackled int4 NULL,
	total_carries_distance int4 NULL,
	total_progressive_carries_distance int4 NULL,
	carries_into_final_third int4 NULL,
	carries_into_penalty_area int4 NULL,
	carries_miscontrolled int4 NULL,
	carries_disposessed int4 NULL,
	passes_recieved int4 NULL,
	progressive_passes_recieved int4 NULL,
	tackles_att int4 NULL,
	tackles_won int4 NULL,
	tackles_def_third int4 NULL,
	tackles_mid_third int4 NULL,
	tackles_att_third int4 NULL,
	challenges_won int4 NULL,
	challenges_lost int4 NULL,
	challenges_att int4 NULL,
	shot_blocks int4 NULL,
	pass_blocks int4 NULL,
	clearances int4 NULL,
	errors_lead_to_shot int4 NULL,
	second_yellow_cards int4 NULL,
	fouls int4 NULL,
	fouled int4 NULL,
	offsides int4 NULL,
	crosses int4 NULL,
	pks_won int4 NULL,
	pks_converted int4 NULL,
	own_goals int4 NULL,
	ball_recoveries int4 NULL,
	aerial_duels_won int4 NULL,
	aerial_duels_lost int4 NULL,
	match_id varchar(20) NULL
);


-- soccer.st_team_season_reports definition

-- Drop table

-- DROP TABLE soccer.st_team_season_reports;

CREATE TABLE soccer.st_team_season_reports (
	squad_id text NULL,
	competition_id text NULL,
	season varchar(20) NULL,
	matches int8 NULL,
	goals int8 NULL,
	xg float8 NULL,
	passes_completed int8 NULL,
	passes_attempted int8 NULL,
	progressive_passes int8 NULL
);
