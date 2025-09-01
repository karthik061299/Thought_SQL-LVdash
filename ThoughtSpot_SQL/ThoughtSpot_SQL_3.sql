-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 3.0 - Basic Compatibility Test
-- =====================================================

-- Test basic table access
SELECT COUNT(*) as row_count 
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card 
LIMIT 10;

-- Basic metric view for PaaS Tracking Card analytics
CREATE OR REPLACE VIEW paas_tracking_card_basic_metrics AS
SELECT 
    session_id,
    app_package_deployed_uuid,
    os_platform,
    app_name,
    app_version,
    session_start_date_time,
    DATE(session_start_date_time) AS session_date,
    geo_country_code,
    is_hpid_signed_in,
    
    -- Boolean flags
    is_viewed_aip_tracking_card,
    is_clicked_aip_order_accordian,
    is_clicked_order_confirmation,
    is_clicked_order_processing,
    is_clicked_track_delivery,
    is_clicked_complete_setup,
    
    -- Count measures
    total_printer_count,
    total_device_count,
    total_accessory_count,
    total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Simple aggregated view
CREATE OR REPLACE VIEW paas_tracking_card_summary AS
SELECT 
    os_platform,
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_apps,
    
    SUM(CASE WHEN is_viewed_aip_tracking_card = true THEN 1 ELSE 0 END) AS viewed_tracking_card_count,
    SUM(CASE WHEN is_clicked_aip_order_accordian = true THEN 1 ELSE 0 END) AS clicked_expand_count,
    SUM(CASE WHEN is_clicked_order_confirmation = true THEN 1 ELSE 0 END) AS clicked_confirmation_count,
    
    SUM(total_printer_count) AS total_printers,
    SUM(total_device_count) AS total_devices
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    os_platform,
    DATE_TRUNC('MONTH', session_start_date_time);

-- =====================================================
-- End of Basic ThoughtSpot SQL Metric Views
-- =====================================================