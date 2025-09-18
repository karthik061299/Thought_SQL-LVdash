# HPX Analytics Metric Views
# Generated from ThoughtSpot artifacts

# 1. HPX Events Model Metric View
CREATE OR REPLACE VIEW hive_metastore.default.mv_hpx_events_model
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: hive_metastore.default.hpx_events_model_iide

filter: event_date_time >= DATE '2023-05-01'

dimensions:
  - name: Event Date Time
    expr: event_date_time
  - name: Daily Event Date
    expr: DATE_TRUNC('DAY', event_date_time)
  - name: Monthly Event Date
    expr: DATE_TRUNC('MONTH', event_date_time)
  - name: Session Id
    expr: session_id
  - name: Support Feature Category
    expr: support_feature_category
  - name: First Session Id
    expr: first_session_id
  - name: App Version
    expr: app_version
  - name: Activity
    expr: activity
  - name: Action
    expr: action
  - name: Event Type
    expr: event_type
  - name: Event Name
    expr: event_name
  - name: Event Detail Event Category
    expr: event_detail_event_category
  - name: Geo Country Code
    expr: geo_country_code
  - name: First Event Date Time
    expr: first_event_date_time
  - name: Os Platform
    expr: os_platform
  - name: Device Type
    expr: device_type
  - name: Model Name
    expr: model_name
  - name: Launch Type
    expr: launch_type
  - name: Pc Type
    expr: pc_type
  - name: Screen
    expr: screen
  - name: Control
    expr: control
  - name: Ui Action
    expr: ui_action
  - name: Is Signed In
    expr: is_signed_in
  - name: Home Tile
    expr: home_tile
  - name: Session Type
    expr: session_type
  - name: Hours Since First Use
    expr: hours_since_first_use
  - name: Days Since First Use
    expr: days_since_first_use
  - name: Weeks Since First Use
    expr: weeks_since_first_use
  - name: Months Since First Use
    expr: months_since_first_use

measures:
  - name: Active Devices
    expr: COUNT(DISTINCT sys_sn_hash)
  - name: New Devices
    expr: SUM(first_event_ct)
  - name: Screen Views
    expr: SUM(CASE WHEN ui_action = 'ScreenDisplayed' THEN 1 ELSE 0 END)
  - name: Sessions
    expr: COUNT(DISTINCT session_id)
  - name: Total Events
    expr: COUNT(event_id)
  - name: Unique Events
    expr: COUNT(DISTINCT unique_event_per_session)
  - name: Unique Screen Views
    expr: COUNT(DISTINCT unique_screen_per_session)
  - name: Total Clicks
    expr: SUM(clicks)
  - name: Sequence Number
    expr: SUM(sequence_number)
  - name: Session Duration
    expr: AVG(session_duration)
$$;

# 2. HPX CURR Metric View
CREATE OR REPLACE VIEW hive_metastore.default.mv_hpx_curr
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: hive_metastore.default.hpx_curr

filter: week_active >= DATE '2023-05-01'

dimensions:
  - name: Week Active
    expr: week_active
  - name: Weekly Week Active
    expr: DATE_TRUNC('WEEK', week_active)
  - name: Hpid Signed In
    expr: hpid_signed_in
  - name: App Version
    expr: app_version
  - name: Os Platform
    expr: os_platform
  - name: Country Name
    expr: country_name

measures:
  - name: Total Previous Week Users T14t20
    expr: SUM(previous_week_users_t14t20)
  - name: Total Last Week Users T7t13
    expr: SUM(last_week_users_t7t13)
  - name: Total This Week Users T0t6
    expr: SUM(this_week_users_t0t6)
  - name: Curr
    expr: AVG(CURR)
$$;

# 3. HPX Retention Metric View
CREATE OR REPLACE VIEW hive_metastore.default.mv_hpx_retention
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: hive_metastore.default.hpx_retention_good

filter: first_device_event_date_time >= DATE '2023-05-01'

dimensions:
  - name: Event Date
    expr: event_date
  - name: Session Id
    expr: session_id
  - name: Sys Serial Number
    expr: sys_serial_number
  - name: First Device Event Date Time
    expr: first_device_event_date_time
  - name: Monthly First Device Event Date Time
    expr: DATE_TRUNC('MONTH', first_device_event_date_time)
  - name: Session Type
    expr: session_type
  - name: Hours Since First Use
    expr: hours_since_first_use
  - name: Days Since First Use
    expr: days_since_first_use
  - name: Weeks Since First Use
    expr: weeks_since_first_use
  - name: Months Since First Use
    expr: months_since_first_use
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
    expr: COUNT(DISTINCT sys_serial_number)
  - name: New Device
    expr: COUNT(DISTINCT first_event)
  - name: Device Event Sequence
    expr: SUM(device_event_sequence)
  - name: Session Time
    expr: SUM(session_time)
  - name: First Event
    expr: SUM(first_event)
$$;

# 4. HPX First Use Behavior Flow Metric View
CREATE OR REPLACE VIEW hive_metastore.default.mv_hpx_first_use_behavior_flow
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: hive_metastore.default.hpx_first_use_behavior_flow

filter: Date >= DATE '2023-05-01'

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
  - name: Total Devices
    expr: SUM(Devices)
$$;

# 5. HPX Events Model Homepage Summary Metric View
CREATE OR REPLACE VIEW hive_metastore.default.mv_hpx_homepage_summary
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: hive_metastore.default.hpx_events_model_homepage_summary

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
  - name: Total Unique Tile Views
    expr: SUM(unique_tile_views)
  - name: Total Unique Tile Clicks
    expr: SUM(unique_tile_clicks)
  - name: Total Unique Interstitial Views
    expr: SUM(unique_interstitial_views)
  - name: Total Unique Interstitial Primary Button Clicks
    expr: SUM(unique_interstitial_primary_button_clicks)
  - name: Total Unique Interstitial Secondary Button Clicks
    expr: SUM(unique_interstitial_secondary_button_clicks)
  - name: Total Home Tile Clicked Vs Homepage Displayed
    expr: SUM(home_tile_clicked_vs_homepage_displayed)
  - name: Total Primary Button Clicked Vs Tile Displayed
    expr: SUM(primary_button_clicked_vs_tile_displayed)
  - name: Total Secondary Button Clicked Vs Tile Displayed
    expr: SUM(secondary_button_clicked_vs_tile_displayed)
  - name: Total Primary Button Clicked Vs Interstitial Displayed
    expr: SUM(primary_button_clicked_vs_interstitial_displayed)
  - name: Total Secondary Button Clicked Vs Interstitial Displayed
    expr: SUM(secondary_button_clicked_vs_interstitial_displayed)
$$;