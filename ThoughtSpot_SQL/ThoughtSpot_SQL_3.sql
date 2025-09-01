-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks - Test Version
-- Generated from PaaS Tracking Card TML Files
-- Version: 3.0 - Simplified for testing
-- =====================================================

-- Simple test query to validate connection
SELECT 'ThoughtSpot SQL Test' AS test_message;

-- Base metric view for PaaS Tracking Card analytics
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Base Attributes
    session_id,
    app_package_deployed_uuid,
    os_platform,
    app_name,
    app_package_id,
    app_version,
    session_start_date_time,
    DATE(session_start_date_time) AS date_field,
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    geo_country_code,
    is_hpid_signed_in,
    device_app_package_deployed_uuid,
    
    -- Boolean Attributes
    is_viewed_aip_tracking_card,
    is_viewed_aip_tracking_card_order_confirmed,
    is_viewed_aip_tracking_card_order_processing,
    is_viewed_aip_tracking_card_order_shipped,
    is_viewed_aip_tracking_card_order_delivered,
    is_clicked_aip_order_accordian,
    is_clicked_order_confirmation,
    is_clicked_order_processing,
    is_clicked_track_delivery,
    is_clicked_complete_setup,
    is_clicked_order_confirmation_pill,
    is_clicked_order_processing_pill,
    is_clicked_order_shipped_pill,
    is_clicked_order_delivered_pill,
    is_aip_setup_complete,
    is_oobe_support_session,
    
    -- Count Measures
    COALESCE(total_printer_count, 0) AS total_printer_count,
    COALESCE(total_device_count, 0) AS total_device_count,
    COALESCE(total_accessory_count, 0) AS total_accessory_count,
    COALESCE(total_pc_count, 0) AS total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;