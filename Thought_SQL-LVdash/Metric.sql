CREATE OR REPLACE VIEW workspace.default.mv_hpx_first_use_behavior_flow
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_first_use_behavior_flow

filter: Date >= DATE '2023-05-01' AND Screen_2 != 'welcome_S2' AND Screen_3 != 'welcome_S3' AND Screen_4 != 'welcome_S4' AND Screen_5 != 'welcome_S5'

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
    expr: COALESCE(SUM(Devices), 0)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_events_model_homepage_summary
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_events_model_homepage_summary

filter: Event Date Time >= DATE '2023-05-01'

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
    expr: COALESCE(SUM(unique_tile_views), 0)
  - name: Unique Tile Clicks
    expr: COALESCE(SUM(unique_tile_clicks), 0)
  - name: Unique Interstitial Views
    expr: COALESCE(SUM(unique_interstitial_views), 0)
  - name: Unique Interstitial Primary Button Clicks
    expr: COALESCE(SUM(unique_interstitial_primary_button_clicks), 0)
  - name: Unique Interstitial Secondary Button Clicks
    expr: COALESCE(SUM(unique_interstitial_secondary_button_clicks), 0)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_retention_good
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_retention_good

filter: Event Date >= DATE '2023-05-01'

dimensions:
  - name: Event Date
    expr: event_date
  - name: Session Id
    expr: session_id
  - name: Sys Serial Number
    expr: sys_serial_number
  - name: First Device Event Date Time
    expr: first_device_event_date_time
  - name: Session Type
    expr: session_type
  - name: Geo Country Code
    expr: geo_country_code
  - name: Pc Type
    expr: pc_type

measures:
  - name: Active Devices
    expr: COALESCE(SUM(active_devices), 0)
  - name: New Device
    expr: COALESCE(SUM(new_device), 0)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_curr
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: team_css_analytics_prod.hpx_analytics.hpx_curr

filter: Week Active >= DATE '2023-05-01'

dimensions:
  - name: Week Active
    expr: week_active

measures:
  - name: Previous Week Users T14t20
    expr: COALESCE(SUM(previous_week_users_t14t20), 0)
  - name: Last Week Users T7t13
    expr: COALESCE(SUM(last_week_users_t7t13), 0)
  - name: This Week Users T0t6
    expr: COALESCE(SUM(this_week_users_t0t6), 0)
  - name: CURR
    expr: COALESCE(SUM(CURR), 0)
$$;