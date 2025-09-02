-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card - Version 4
-- Simplified for Databricks SQL compatibility
-- =====================================================

-- Test basic connectivity first
SELECT 'ThoughtSpot SQL Views Creation Started' AS status;

-- Simple base view to test table access
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_test AS
SELECT 
    session_id,
    app_package_deployed_uuid,
    os_platform,
    app_name,
    session_start_date_time,
    is_viewed_aip_tracking_card,
    total_printer_count
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
LIMIT 100;

-- Basic metrics view with simple aggregations
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_simple_metrics AS
SELECT 
    os_platform,
    DATE(session_start_date_time) AS session_date,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_apps,
    SUM(CASE WHEN is_viewed_aip_tracking_card = true THEN 1 ELSE 0 END) AS tracking_card_views,
    SUM(CASE WHEN is_clicked_aip_order_accordian = true THEN 1 ELSE 0 END) AS expand_clicks,
    SUM(total_printer_count) AS total_printers
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    os_platform,
    DATE(session_start_date_time);

-- Monthly aggregation view
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly AS
SELECT 
    os_platform,
    DATE_TRUNC('month', session_start_date_time) AS month_date,
    COUNT(*) AS monthly_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS monthly_unique_apps,
    SUM(CASE WHEN is_viewed_aip_tracking_card = true THEN 1 ELSE 0 END) AS monthly_tracking_card_views,
    SUM(CASE WHEN is_clicked_aip_order_accordian = true THEN 1 ELSE 0 END) AS monthly_expand_clicks
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    os_platform,
    DATE_TRUNC('month', session_start_date_time);

SELECT 'ThoughtSpot SQL Views Creation Completed Successfully' AS status;

-- =====================================================
-- End of Simplified ThoughtSpot SQL Metric Views
-- =====================================================