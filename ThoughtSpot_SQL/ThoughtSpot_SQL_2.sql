-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Assets
-- Version 2 - Corrected Syntax
-- =====================================================

-- Main Metric View: PaaS Tracking Card Analytics
CREATE OR REPLACE VIEW `team_css_analytics_prod`.`hpx_analytics`.`paas_tracking_card_metrics` AS
SELECT 
    -- Base Dimensions
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    os_platform,
    app_name,
    app_package_id,
    app_version,
    geo_country_code,
    aip_device_uuid,
    associated_device_session_id,
    
    -- Date Dimensions
    DATE(session_start_date_time) AS date_field,
    DATE_TRUNC('month', session_start_date_time) AS month_field,
    DATE_TRUNC('year', session_start_date_time) AS year_field,
    session_start_date_time,
    
    -- Boolean Attributes
    is_hpid_signed_in,
    is_associated_device,
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
    is_clicked_aip_order_accordian_order_confirmed,
    is_clicked_aip_order_accordian_order_processing,
    is_clicked_aip_order_accordian_order_shipped,
    is_clicked_aip_order_accord,
    is_aip_setup_start,
    is_ows_start,
    is_oobe_complete,
    is_aip_setup_complete,
    is_clicked_support,
    is_oobe_support_session,
    
    -- Base Measures
    total_printer_count,
    total_device_count,
    total_accessory_count,
    total_pc_count,
    max_total_printer_count,
    max_total_device_count,
    max_total_accessory_count,
    max_total_pc_count
    
FROM `team_css_analytics_prod`.`hpx_analytics`.`paas_tracking_card`;

-- =====================================================
-- Calculated Measures View: ThoughtSpot Formula Conversions
-- =====================================================

CREATE OR REPLACE VIEW `team_css_analytics_prod`.`hpx_analytics`.`paas_tracking_card_calculated_measures` AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS month_field,
    os_platform,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    
    -- Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END)
    END AS viewed_paas_tracking_card,
    
    -- Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END)
    END AS clicked_expand,
    
    -- Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END)
    END AS clicked_order_confirmation,
    
    -- Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END)
    END AS clicked_order_processing,
    
    -- Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END)
    END AS clicked_track_delivery,
    
    -- Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END)
    END AS clicked_complete_setup,
    
    -- Confirmed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END)
    END AS confirmed,
    
    -- Delivered
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END)
    END AS delivered,
    
    -- Processed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END)
    END AS processed,
    
    -- Shipped
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END)
    END AS shipped,
    
    -- Onboarded (Delivered AND Setup Complete)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END)
    END AS onboarded,
    
    -- Support Cases (Delivered AND Support Session)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END)
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
    END AS delivered_pill
    
FROM `team_css_analytics_prod`.`hpx_analytics`.`paas_tracking_card`
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid;

-- =====================================================
-- Dashboard Summary View: Aggregated Metrics for Liveboards
-- =====================================================

CREATE OR REPLACE VIEW `team_css_analytics_prod`.`hpx_analytics`.`paas_tracking_card_dashboard_summary` AS
SELECT 
    month_field,
    os_platform,
    
    -- Primary Metrics
    SUM(viewed_paas_tracking_card) AS total_viewed_paas_tracking_card,
    SUM(clicked_expand) AS total_clicked_expand,
    SUM(clicked_order_confirmation) AS total_clicked_order_confirmation,
    SUM(clicked_order_processing) AS total_clicked_order_processing,
    SUM(clicked_track_delivery) AS total_clicked_track_delivery,
    SUM(clicked_complete_setup) AS total_clicked_complete_setup,
    
    -- Status Metrics
    SUM(confirmed) AS total_confirmed,
    SUM(processed) AS total_processed,
    SUM(shipped) AS total_shipped,
    SUM(delivered) AS total_delivered,
    SUM(onboarded) AS total_onboarded,
    SUM(support_cases) AS total_support_cases,
    
    -- Pill Click Metrics
    SUM(order_confirmed_pill) AS total_order_confirmed_pill,
    SUM(processing_pill) AS total_processing_pill,
    SUM(shipped_pill) AS total_shipped_pill,
    SUM(delivered_pill) AS total_delivered_pill
    
FROM `team_css_analytics_prod`.`hpx_analytics`.`paas_tracking_card_calculated_measures`
GROUP BY month_field, os_platform
ORDER BY month_field DESC, os_platform;

-- =====================================================
-- Device Count Aggregations View
-- =====================================================

CREATE OR REPLACE VIEW `team_css_analytics_prod`.`hpx_analytics`.`paas_tracking_card_device_metrics` AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS month_field,
    os_platform,
    geo_country_code,
    is_hpid_signed_in,
    
    -- Device Count Aggregations
    SUM(total_printer_count) AS total_printer_count_sum,
    SUM(total_device_count) AS total_device_count_sum,
    SUM(total_accessory_count) AS total_accessory_count_sum,
    SUM(total_pc_count) AS total_pc_count_sum,
    SUM(max_total_printer_count) AS max_total_printer_count_sum,
    SUM(max_total_device_count) AS max_total_device_count_sum,
    SUM(max_total_accessory_count) AS max_total_accessory_count_sum,
    SUM(max_total_pc_count) AS max_total_pc_count_sum,
    
    -- Session Counts
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_app_packages,
    COUNT(DISTINCT device_app_package_deployed_uuid) AS unique_device_packages
    
FROM `team_css_analytics_prod`.`hpx_analytics`.`paas_tracking_card`
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform,
    geo_country_code,
    is_hpid_signed_in
ORDER BY month_field DESC, os_platform;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views
-- =====================================================