-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Assets
-- Version 3 - Simplified Test Version
-- =====================================================

-- Test basic table access first
SELECT COUNT(*) as row_count 
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card 
LIMIT 10;

-- Basic Metric View: PaaS Tracking Card Analytics
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    os_platform,
    app_name,
    app_version,
    geo_country_code,
    DATE(session_start_date_time) AS date_field,
    session_start_date_time,
    is_hpid_signed_in,
    is_viewed_aip_tracking_card,
    is_clicked_aip_order_accordian,
    total_printer_count,
    total_device_count
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Simple Calculated Measures View
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_simple_measures AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS month_field,
    os_platform,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_paas_tracking_card,
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS clicked_expand,
    SUM(total_printer_count) AS total_printer_count_sum,
    SUM(total_device_count) AS total_device_count_sum
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform;

-- Test the views
SELECT * FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_simple_measures LIMIT 5;