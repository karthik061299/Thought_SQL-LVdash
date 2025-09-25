CREATE OR REPLACE VIEW workspace.default.mv_hpx_events_model_v1
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: workspace.default.hpx_events_model_iide

dimensions:
  - name: event_date_time
    expr: event_date_time
  - name: geo_country_code
    expr: geo_country_code
  - name: app_version
    expr: app_version

measures:
  - name: active_devices
    expr: COUNT(DISTINCT sys_sn_hash)
  - name: new_devices
    expr: SUM(first_event_ct)
  - name: unique_screen_views
    expr: COUNT(DISTINCT unique_screen_per_session)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_retention_good_v1
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: workspace.default.hpx_retention_good

dimensions:
  - name: event_date
    expr: event_date
  - name: geo_country_code
    expr: geo_country_code
  - name: pc_type
    expr: pc_type

measures:
  - name: active_devices
    expr: COUNT(DISTINCT sys_serial_number)
  - name: new_device
    expr: COUNT(DISTINCT first_event)
$$;

CREATE OR REPLACE VIEW workspace.default.mv_hpx_curr_v1
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: workspace.default.hpx_curr

dimensions:
  - name: week_active
    expr: week_active

measures:
  - name: previous_week_users_t14t20
    expr: SUM(previous_week_users_t14t20)
  - name: last_week_users_t7t13
    expr: SUM(last_week_users_t7t13)
  - name: this_week_users_t0t6
    expr: SUM(this_week_users_t0t6)
$$;