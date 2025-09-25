CREATE OR REPLACE VIEW workspace.default.mv_hpx_first_use_behavior_flow
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_first_use_behavior_flow

dimensions:
  - name: Date
    expr: Date
  - name: Geo Country Code
    expr: geo_country_code
  - name: Pc Type
    expr: pc_Type
  - name: Sys Sku
    expr: sys_sku
  - name: Model Name
    expr: model_name
  - name: App Version
    expr: App_Version
  - name: Screen 1
    expr: Screen_1
  - name: Screen 2
    expr: Screen_2
  - name: Screen 3
    expr: Screen_3
  - name: Screen 4
    expr: Screen_4
  - name: Screen 5
    expr: Screen_5

measures:
  - name: Devices
    expr: SUM(Devices)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_events_model_homepage_summary
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_events_model_homepage_summary

dimensions:
  - name: Home Tile
    expr: home_tile
  - name: Pc Type
    expr: pc_type
  - name: Screen
    expr: screen
  - name: App Version
    expr: app_version
  - name: Event Date Time
    expr: event_date_time
  - name: Geo Country Code
    expr: geo_country_code

measures:
  - name: Unique Tile Views
    expr: SUM(unique_tile_views)
  - name: Unique Tile Clicks
    expr: SUM(unique_tile_clicks)
  - name: Unique Interstitial Views
    expr: SUM(unique_interstitial_views)
  - name: Unique Interstitial Primary Button Clicks
    expr: SUM(unique_interstitial_primary_button_clicks)
  - name: Unique Interstitial Secondary Button Clicks
    expr: SUM(unique_interstitial_secondary_button_clicks)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_retention_good
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_retention_good

dimensions:
  - name: Event Date
    expr: event_date
  - name: Session Id
    expr: session_id
  - name: Sys Serial Number
    expr: sys_serial_number
  - name: Event Meta Detail Date Time
    expr: event_meta_detail_date_time
  - name: First Device Event Date Time
    expr: first_device_event_date_time
  - name: Session First Event Date Time
    expr: session_first_event_date_time
  - name: Last Device Event Date Time
    expr: last_device_event_date_time
  - name: Session Last Event Date Time
    expr: session_last_event_date_time
  - name: Session Type
    expr: session_type
  - name: Sys Sku
    expr: sys_sku
  - name: Business Model
    expr: business_model
  - name: Family
    expr: family
  - name: Model Name
    expr: model_name
  - name: Device Type
    expr: device_type
  - name: Geo Country Code
    expr: geo_country_code
  - name: Pc Type
    expr: pc_type
  - name: Action
    expr: action
  - name: Activity
    expr: activity

measures:
  - name: Active Devices
    expr: SUM(active_devices)
  - name: New Device
    expr: SUM(new_device)
  - name: Device Event Sequence
    expr: SUM(device_event_sequence)
  - name: Session Time
    expr: SUM(session_time)
  - name: First Event
    expr: SUM(first_event)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_curr
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_curr

dimensions:
  - name: Week Active
    expr: week_active

measures:
  - name: Previous Week Users T14t20
    expr: SUM(previous_week_users_t14t20)
  - name: Last Week Users T7t13
    expr: SUM(last_week_users_t7t13)
  - name: This Week Users T0t6
    expr: SUM(this_week_users_t0t6)
  - name: Curr
    expr: SUM(CURR)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_events_model
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_events_model_iide

dimensions:
  - name: Session Id
    expr: session_id
  - name: Support Feature Category
    expr: support_feature_category
  - name: First Session Id
    expr: first_session_id
  - name: App Version
    expr: app_version
  - name: First App Version
    expr: first_app_version
  - name: First Use Registration
    expr: first_use_registration
  - name: Event Id
    expr: event_id
  - name: Event Source Table
    expr: event_source_table
  - name: Geo Region Name
    expr: geo_region_name
  - name: Geo Country Code
    expr: geo_country_code
  - name: Geo Ip Connection
    expr: geo_ip_connection
  - name: Event Date Time
    expr: event_date_time
  - name: First Event Date Time
    expr: first_event_date_time
  - name: Session Start Date Time
    expr: session_start_date_time
  - name: Session End Date Time
    expr: session_end_date_time
  - name: Born On Date
    expr: born_on_date
  - name: Os Country Region
    expr: os_country_region
  - name: Os Language
    expr: os_language
  - name: Os Version
    expr: os_version
  - name: Os Screen Resolution
    expr: os_screen_resolution
  - name: Device Type
    expr: device_type
  - name: Model Name
    expr: model_name
  - name: Model Number
    expr: model_number
  - name: Geo Postal Code
    expr: geo_postal_code
  - name: Geo City
    expr: geo_city
  - name: Launch Type
    expr: launch_type
  - name: Screen
    expr: screen
  - name: Action Aux Params
    expr: action_aux_params
  - name: Tile Position
    expr: tile_position
  - name: Event Par8 Cleaned
    expr: event_par8_cleaned
  - name: Control
    expr: control
  - name: Ui Action
    expr: ui_action
  - name: Is Signed In
    expr: is_signed_in
  - name: Previous Control
    expr: previous_control
  - name: Next Control
    expr: next_control
  - name: Home Tile
    expr: home_tile
  - name: Unique Screen Per Session
    expr: unique_screen_per_session
  - name: Unique Event Per Session
    expr: unique_event_per_session
  - name: Session Type
    expr: session_type
  - name: Previous Screen
    expr: previous_screen
  - name: Next Screen
    expr: next_screen
  - name: Event Detail Event Category
    expr: event_detail_event_category
  - name: Event Type
    expr: event_type
  - name: Event Name
    expr: event_name
  - name: Action
    expr: action
  - name: Activity
    expr: activity
  - name: Os Platform
    expr: os_platform
  - name: Pc Type
    expr: pc_type

measures:
  - name: Active Devices
    expr: SUM(active_devices)
  - name: Cumulative New Devices
    expr: SUM(cumulative_new_devices)
  - name: New Devices
    expr: SUM(new_devices)
  - name: Screen Views
    expr: SUM(screen_views)
  - name: Sessions
    expr: SUM(sessions)
  - name: Total Events
    expr: SUM(total_events)
  - name: Unique Events
    expr: SUM(unique_events)
  - name: Unique Screen Views
    expr: SUM(unique_screen_views)
$$;