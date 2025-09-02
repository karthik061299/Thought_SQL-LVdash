-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Version 3: Simplified approach with basic table access
-- =====================================================

-- First, let's test basic table access
SELECT 'Table access test' as test_message;

-- Basic count query
SELECT COUNT(*) as total_records 
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Sample data query
SELECT 
    session_id,
    app_package_deployed_uuid,
    os_platform,
    session_start_date_time,
    is_viewed_aip_tracking_card
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card 
LIMIT 10;

-- Simple aggregation by month and platform
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    os_platform,
    COUNT(*) as record_count,
    COUNT(DISTINCT session_id) as unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) as unique_apps
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    os_platform
ORDER BY month_date DESC, os_platform
LIMIT 20;

-- Basic metric: Viewed PaaS Tracking Card
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    os_platform,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_paas_tracking_card_count
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    os_platform
ORDER BY month_date DESC, os_platform
LIMIT 20;

-- Create a simple view
CREATE OR REPLACE VIEW paas_tracking_basic_metrics AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    os_platform,
    geo_country_code,
    COUNT(*) as total_records,
    COUNT(DISTINCT session_id) as unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) as unique_apps,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_tracking_card_count,
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS clicked_expand_count
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    os_platform,
    geo_country_code;

-- Test the view
SELECT * FROM paas_tracking_basic_metrics 
ORDER BY month_date DESC, os_platform 
LIMIT 10;

SELECT 'SQL execution completed successfully' as completion_message;