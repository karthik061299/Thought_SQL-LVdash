-- Version: 3
-- Error in the previous version: INTERNAL_ERROR - table access or permissions issue
-- Error handling: Simplified to basic SELECT query to test table access and structure
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from ThoughtSpot TML assets: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL Environment
-- Catalog: team_css_analytics_prod, Schema: hpx_analytics

-- =============================================================================
-- TABLE ACCESS TEST
-- =============================================================================

-- Test basic table access
SELECT COUNT(*) as record_count
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
LIMIT 1;

-- Test table structure
DESCRIBE team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =============================================================================
-- BASIC METRIC VIEW: PaaS Tracking Card Analytics
-- =============================================================================

SELECT 
    -- Base Dimensions
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    os_platform,
    app_name,
    app_version,
    geo_country_code,
    
    -- Date Dimensions
    session_start_date_time,
    DATE(session_start_date_time) AS date_field,
    DATE_TRUNC('month', session_start_date_time) AS month_date,
    
    -- Boolean Attributes
    is_hpid_signed_in,
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
    is_aip_setup_complete,
    is_oobe_support_session,
    
    -- Base Measures
    total_printer_count,
    total_device_count,
    total_accessory_count,
    total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
LIMIT 100;

-- =============================================================================
-- END OF TEST QUERIES
-- =============================================================================