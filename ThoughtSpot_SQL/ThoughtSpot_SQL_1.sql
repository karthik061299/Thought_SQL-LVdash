/*
  HPX Retention_CSS_Analytics Metric View
*/
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.hpx_retention_good_metric_view AS
SELECT
  event_date AS `event_date`,
  session_id AS `session_id`,
  sys_serial_number AS `sys_serial_number`,
  event_meta_detail_date_time AS `event_meta_detail_date_time`,
  first_device_event_date_time AS `first_device_event_date_time`,
  session_first_event_date_time AS `session_first_event_date_time`,
  last_device_event_date_time AS `last_device_event_date_time`,
  session_last_event_date_time AS `session_last_event_date_time`,
  device_event_sequence AS `device_event_sequence`,
  session_time AS `session_time`,
  session_type AS `session_type`,
  first_event AS `first_event`,
  seconds_since_first_use AS `seconds_since_first_use`,
  seconds_since_previous_session AS `seconds_since_previous_session`,
  hours_since_first_use AS `hours_since_first_use`,
  hours_since_previous_session AS `hours_since_previous_session`,
  days_since_first_use AS `days_since_first_use`,
  days_since_previous_session AS `days_since_previous_session`,
  weeks_since_first_use AS `weeks_since_first_use`,
  weeks_since_previous_session AS `weeks_since_previous_session`,
  months_since_first_use AS `months_since_first_use`,
  months_since_previous_session AS `months_since_previous_session`,
  sys_sku AS `sys_sku`,
  business_model AS `business_model`,
  family AS `family`,
  model_name AS `model_name`,
  device_type AS `device_type`,
  geo_country_code AS `geo_country_code`,
  pc_type AS `pc_type`,
  PCaaS AS `PCaaS`,
  HPOne AS `HPOne`,
  action AS `action`,
  action_aux_params AS `action_aux_params`,
  activity AS `activity`,
  screen_path AS `screen_path`,
  screen_mode AS `screen_mode`,
  control_name AS `control_name`,
  COUNT(DISTINCT sys_serial_number) AS `active_devices`,
  COUNT(DISTINCT CASE WHEN first_event > 0 THEN sys_serial_number END) AS `new_device`
FROM team_css_analytics_prod.hpx_analytics.hpx_retention_good
GROUP BY
  event_date, session_id, sys_serial_number, event_meta_detail_date_time, first_device_event_date_time, session_first_event_date_time, last_device_event_date_time, session_last_event_date_time, device_event_sequence, session_time, session_type, first_event, seconds_since_first_use, seconds_since_previous_session, hours_since_first_use, hours_since_previous_session, days_since_first_use, days_since_previous_session, weeks_since_first_use, weeks_since_previous_session, months_since_first_use, months_since_previous_session, sys_sku, business_model, family, model_name, device_type, geo_country_code, pc_type, PCaaS, HPOne, action, action_aux_params, activity, screen_path, screen_mode, control_name;
 
/*
  HPX_CURR Metric View
*/
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.hpx_curr_metric_view AS
SELECT
  week_active AS `week_active`,
  previous_week_users_t14t20 AS `previous_week_users_t14t20`,
  last_week_users_t7t13 AS `last_week_users_t7t13`,
  this_week_users_t0t6 AS `this_week_users_t0t6`,
  CURR AS `curr`,
  hpid_signed_in AS `hpid_signed_in`,
  app_version AS `app_version`,
  os_platform AS `os_platform`,
  country_name AS `country_name`
FROM team_css_analytics_prod.hpx_analytics.hpx_curr;
 
/*
  hpx_events_model Metric View
*/
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.hpx_events_model_iide_metric_view AS
SELECT
  sys_sn_hash AS `sys_sn_hash`,
  app_package_deployed_uuid AS `app_package_deployed_uuid`,
  session_id AS `session_id`,
  clicks AS `clicks`,
  support_feature_category AS `support_feature_category`,
  first_event_ct AS `first_event_ct`,
  hours_since_first_use AS `hours_since_first_use`,
  days_since_first_use AS `days_since_first_use`,
  weeks_since_first_use AS `weeks_since_first_use`,
  months_since_first_use AS `months_since_first_use`,
  first_session_id AS `first_session_id`,
  app_version AS `app_version`,
  activity AS `activity`,
  action AS `action`,
  event_type AS `event_type`,
  first_app_version AS `first_app_version`,
  first_use_registration AS `first_use_registration`,
  first_use_registration_server_response AS `first_use_registration_server_response`,
  event_id AS `event_id`,
  event_name AS `event_name`,
  event_detail_event_category AS `event_detail_event_category`,
  event_source_table AS `event_source_table`,
  geo_region_name AS `geo_region_name`,
  geo_country_code AS `geo_country_code`,
  geo_ip_connection AS `geo_ip_connection`,
  sequence_number AS `sequence_number`,
  event_date_time AS `event_date_time`,
  first_event_date_time AS `first_event_date_time`,
  session_start_date_time AS `session_start_date_time`,
  session_end_date_time AS `session_end_date_time`,
  session_duration AS `session_duration`,
  born_on_date AS `born_on_date`,
  os_country_region AS `os_country_region`,
  os_language AS `os_language`,
  os_platform AS `os_platform`,
  os_version AS `os_version`,
  os_screen_resolution AS `os_screen_resolution`,
  device_type AS `device_type`,
  model_name AS `model_name`,
  model_number AS `model_number`,
  geo_postal_code AS `geo_postal_code`,
  geo_city AS `geo_city`,
  launch_type AS `launch_type`,
  pc_type AS `pc_type`,
  screen AS `screen`,
  action_aux_params AS `action_aux_params`,
  tile_position AS `tile_position`,
  event_par8_cleaned AS `event_par8_cleaned`,
  control AS `control`,
  ui_action AS `ui_action`,
  is_signed_in AS `is_signed_in`,
  previous_control AS `previous_control`,
  next_control AS `next_control`,
  home_tile AS `home_tile`,
  unique_screen_per_session AS `unique_screen_per_session`,
  unique_event_per_session AS `unique_event_per_session`,
  session_type AS `session_type`,
  previous_screen AS `previous_screen`,
  next_screen AS `next_screen`,
  data_purpose_benefits_hp_analytics AS `data_purpose_benefits_hp_analytics`,
  COUNT(DISTINCT sys_sn_hash) AS `active_devices_1`,
  SUM(first_event_ct) AS `new_devices_1`,
  COUNT(DISTINCT session_id) AS `sessions_1`,
  COUNT(event_id) AS `total_events_1`,
  COUNT(DISTINCT unique_event_per_session) AS `unique_events_1`,
  COUNT(DISTINCT unique_screen_per_session) AS `unique_screen_views_1`
FROM team_css_analytics_prod.hpx_analytics.hpx_events_model_iide
GROUP BY
  sys_sn_hash, app_package_deployed_uuid, session_id, clicks, support_feature_category, first_event_ct, hours_since_first_use, days_since_first_use, weeks_since_first_use, months_since_first_use, first_session_id, app_version, activity, action, event_type, first_app_version, first_use_registration, first_use_registration_server_response, event_id, event_name, event_detail_event_category, event_source_table, geo_region_name, geo_country_code, geo_ip_connection, sequence_number, event_date_time, first_event_date_time, session_start_date_time, session_end_date_time, session_duration, born_on_date, os_country_region, os_language, os_platform, os_version, os_screen_resolution, device_type, model_name, model_number, geo_postal_code, geo_city, launch_type, pc_type, screen, action_aux_params, tile_position, event_par8_cleaned, control, ui_action, is_signed_in, previous_control, next_control, home_tile, unique_screen_per_session, unique_event_per_session, session_type, previous_screen, next_screen, data_purpose_benefits_hp_analytics;
 
/*
  hpx_events_model_homepage_summary Metric View
*/
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.hpx_events_model_homepage_summary_metric_view AS
SELECT
  home_tile AS `home_tile`,
  unique_tile_views AS `unique_tile_views`,
  unique_tile_clicks AS `unique_tile_clicks`,
  unique_interstitial_views AS `unique_interstitial_views`,
  unique_interstitial_primary_button_clicks AS `unique_interstitial_primary_button_clicks`,
  unique_interstitial_secondary_button_clicks AS `unique_interstitial_secondary_button_clicks`,
  home_tile_clicked_vs_homepage_displayed AS `home_tile_clicked_vs_homepage_displayed`,
  primary_button_clicked_vs_tile_displayed AS `primary_button_clicked_vs_tile_displayed`,
  secondary_button_clicked_vs_tile_displayed AS `secondary_button_clicked_vs_tile_displayed`,
  primary_button_clicked_vs_interstitial_displayed AS `primary_button_clicked_vs_interstitial_displayed`,
  secondary_button_clicked_vs_interstitial_displayed AS `secondary_button_clicked_vs_interstitial_displayed`,
  pc_type AS `pc_type`,
  screen AS `screen`,
  app_version AS `app_version`,
  event_date_time AS `event_date_time`,
  geo_country_code AS `geo_country_code`
FROM team_css_analytics_prod.hpx_analytics.hpx_events_model_homepage_summary;
 
/*
  hpx_first_use_behavior_flow Metric View
*/
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.hpx_first_use_behavior_flow_metric_view AS
SELECT
  Date AS `date`,
  geo_country_code AS `geo_country_code`,
  pc_Type AS `pc_type`,
  sys_sku AS `sys_sku`,
  model_name AS `model_name`,
  App_Version AS `app_version`,
  Screen_1 AS `screen_1`,
  Screen_2 AS `screen_2`,
  Screen_3 AS `screen_3`,
  Screen_4 AS `screen_4`,
  Screen_5 AS `screen_5`,
  Devices AS `devices`
FROM team_css_analytics_prod.hpx_analytics.hpx_first_use_behavior_flow;
