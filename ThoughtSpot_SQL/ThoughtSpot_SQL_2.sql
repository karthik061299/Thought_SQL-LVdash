-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 2.0 - Simplified for Databricks Compatibility
-- =====================================================

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
    DATE(session_start_date_time) AS session_date,
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    geo_country_code,
    is_hpid_signed_in,
    device_app_package_deployed_uuid,
    associated_device_session_id,
    aip_device_uuid,
    is_associated_device,
    
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
    total_printer_count,
    total_device_count,
    total_accessory_count,
    total_pc_count,
    max_total_printer_count,
    max_total_device_count,
    max_total_accessory_count,
    max_total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- Aggregated Metrics View with ThoughtSpot Formulas
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_aggregated AS
SELECT 
    month_date,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    
    -- ThoughtSpot Formula: Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) 
    END AS viewed_paas_tracking_card,
    
    -- ThoughtSpot Formula: Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) 
    END AS clicked_expand,
    
    -- ThoughtSpot Formula: Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) 
    END AS clicked_order_confirmation,
    
    -- ThoughtSpot Formula: Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) 
    END AS clicked_order_processing,
    
    -- ThoughtSpot Formula: Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) 
    END AS clicked_track_delivery,
    
    -- ThoughtSpot Formula: Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) 
    END AS clicked_complete_setup,
    
    -- ThoughtSpot Formula: Confirmed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) 
    END AS confirmed,
    
    -- ThoughtSpot Formula: Processed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) 
    END AS processed,
    
    -- ThoughtSpot Formula: Shipped
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) 
    END AS shipped,
    
    -- ThoughtSpot Formula: Delivered
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) 
    END AS delivered,
    
    -- ThoughtSpot Formula: Onboarded
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN (is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true) THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN (is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true) THEN app_package_deployed_uuid END) 
    END AS onboarded,
    
    -- ThoughtSpot Formula: Support Cases
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN (is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true) THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN (is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true) THEN app_package_deployed_uuid END) 
    END AS support_cases,
    
    -- Pill Click Measures
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) 
    END AS order_confirmed_pill,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) 
    END AS processing_pill,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) 
    END AS shipped_pill,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) 
    END AS delivered_pill,
    
    -- Count Aggregations
    SUM(total_printer_count) AS sum_total_printer_count,
    SUM(total_device_count) AS sum_total_device_count,
    SUM(total_accessory_count) AS sum_total_accessory_count,
    SUM(total_pc_count) AS sum_total_pc_count,
    SUM(max_total_printer_count) AS sum_max_total_printer_count,
    SUM(max_total_device_count) AS sum_max_total_device_count,
    SUM(max_total_accessory_count) AS sum_max_total_accessory_count,
    SUM(max_total_pc_count) AS sum_max_total_pc_count,
    
    -- Session Counts
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_app_packages
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
GROUP BY 
    month_date,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in;

-- =====================================================
-- Dashboard-Ready View for Lakeview Integration
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard AS
SELECT 
    month_date,
    os_platform,
    app_version,
    geo_country_code,
    CASE WHEN is_hpid_signed_in = true THEN 'Signed In' ELSE 'Not Signed In' END AS hpid_status,
    
    -- Core Metrics for Visualization (matching ThoughtSpot liveboard)
    viewed_paas_tracking_card AS paas_tracking_card_views,
    clicked_expand AS clicks_on_expand,
    clicked_order_confirmation AS order_confirmation_clicks,
    clicked_order_processing AS processing_clicks,
    clicked_track_delivery AS track_delivery_clicks,
    clicked_complete_setup AS complete_setup_clicks,
    
    -- Status Metrics
    confirmed AS confirmed_status,
    processed AS processed_status,
    shipped AS shipped_status,
    delivered AS delivered_status,
    onboarded AS onboarded_users,
    support_cases,
    
    -- Pill Click Metrics
    order_confirmed_pill AS confirmed_pill_clicks,
    processing_pill AS processing_pill_clicks,
    shipped_pill AS shipped_pill_clicks,
    delivered_pill AS delivered_pill_clicks,
    
    -- Device Counts
    sum_total_printer_count AS total_printers,
    sum_total_device_count AS total_devices,
    sum_total_accessory_count AS total_accessories,
    sum_total_pc_count AS total_pcs,
    
    -- Session Metrics
    unique_sessions,
    unique_app_packages
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_aggregated
ORDER BY month_date DESC, os_platform;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views
-- =====================================================