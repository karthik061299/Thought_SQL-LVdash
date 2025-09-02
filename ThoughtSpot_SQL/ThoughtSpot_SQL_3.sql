-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from ThoughtSpot TML files - Version 3
-- Simplified for Databricks compatibility
-- =====================================================

-- Test basic connectivity first
SELECT COUNT(*) as row_count FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card LIMIT 10;

-- Base metric view for PaaS Tracking Card data
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_base_metrics AS
SELECT 
    session_id,
    app_package_deployed_uuid,
    os_platform,
    app_name,
    app_version,
    session_start_date_time,
    geo_country_code,
    is_hpid_signed_in,
    DATE(session_start_date_time) AS session_date,
    is_viewed_aip_tracking_card,
    is_clicked_aip_order_accordian,
    is_clicked_order_confirmation,
    is_clicked_order_processing,
    is_clicked_track_delivery,
    is_clicked_complete_setup,
    total_printer_count,
    total_device_count
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Simple aggregated view
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_simple_agg AS
SELECT 
    os_platform,
    session_date,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_apps,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_tracking_card,
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS clicked_expand,
    SUM(total_printer_count) AS total_printers,
    SUM(total_device_count) AS total_devices
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_base_metrics
GROUP BY os_platform, session_date;

-- Monthly summary view
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_summary AS
SELECT 
    os_platform,
    DATE_TRUNC('month', session_date) AS month_year,
    SUM(unique_apps) AS monthly_unique_apps,
    SUM(viewed_tracking_card) AS monthly_views,
    SUM(clicked_expand) AS monthly_clicks,
    SUM(total_printers) AS monthly_printers,
    SUM(total_devices) AS monthly_devices
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_simple_agg
GROUP BY os_platform, DATE_TRUNC('month', session_date);

-- =====================================================
-- End of ThoughtSpot SQL Metric Views - Version 3
-- =====================================================