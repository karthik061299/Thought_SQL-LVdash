-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 1.0
-- =====================================================

-- Base metric view for PaaS Tracking Card analytics
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Base Attributes
    `session_id`,
    `app_package_deployed_uuid`,
    `os_platform`,
    `app_name`,
    `app_package_id`,
    `app_version`,
    `session_start_date_time`,
    DATE(`session_start_date_time`) AS `date`,
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `device_app_package_deployed_uuid`,
    `associated_device_session_id`,
    `aip_device_uuid`,
    `is_associated_device`,
    
    -- Boolean Attributes
    `is_viewed_aip_tracking_card`,
    `is_viewed_aip_tracking_card_order_confirmed`,
    `is_viewed_aip_tracking_card_order_processing`,
    `is_viewed_aip_tracking_card_order_shipped`,
    `is_viewed_aip_tracking_card_order_delivered`,
    `is_clicked_aip_order_accordian`,
    `is_clicked_order_confirmation`,
    `is_clicked_order_processing`,
    `is_clicked_track_delivery`,
    `is_clicked_complete_setup`,
    `is_clicked_order_confirmation_pill`,
    `is_clicked_order_processing_pill`,
    `is_clicked_order_shipped_pill`,
    `is_clicked_order_delivered_pill`,
    `is_clicked_aip_order_accordian_order_confirmed`,
    `is_clicked_aip_order_accordian_order_processing`,
    `is_clicked_aip_order_accordian_order_shipped`,
    `is_clicked_aip_order_accord`,
    `is_aip_setup_start`,
    `is_ows_start`,
    `is_oobe_complete`,
    `is_aip_setup_complete`,
    `is_clicked_support`,
    `is_oobe_support_session`,
    
    -- Count Measures (Direct)
    `total_printer_count`,
    `total_device_count`,
    `total_accessory_count`,
    `total_pc_count`,
    `max_total_printer_count`,
    `max_total_device_count`,
    `max_total_accessory_count`,
    `max_total_pc_count`,
    
    -- Calculated Measures (ThoughtSpot Formulas converted to SQL)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) 
    END AS `viewed_paas_tracking_card`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_expand`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_order_confirmation`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_order_processing`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_track_delivery`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_complete_setup`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) 
    END AS `confirmed`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) 
    END AS `processed`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) 
    END AS `shipped`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) 
    END AS `delivered`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN (`is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true) THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN (`is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true) THEN `app_package_deployed_uuid` END) 
    END AS `onboarded`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN (`is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true) THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN (`is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true) THEN `app_package_deployed_uuid` END) 
    END AS `support_cases`,
    
    -- Pill Click Measures
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `order_confirmed_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `processing_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `shipped_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `delivered_pill`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    `session_id`,
    `app_package_deployed_uuid`,
    `os_platform`,
    `app_name`,
    `app_package_id`,
    `app_version`,
    `session_start_date_time`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `device_app_package_deployed_uuid`,
    `associated_device_session_id`,
    `aip_device_uuid`,
    `is_associated_device`,
    `is_viewed_aip_tracking_card`,
    `is_viewed_aip_tracking_card_order_confirmed`,
    `is_viewed_aip_tracking_card_order_processing`,
    `is_viewed_aip_tracking_card_order_shipped`,
    `is_viewed_aip_tracking_card_order_delivered`,
    `is_clicked_aip_order_accordian`,
    `is_clicked_order_confirmation`,
    `is_clicked_order_processing`,
    `is_clicked_track_delivery`,
    `is_clicked_complete_setup`,
    `is_clicked_order_confirmation_pill`,
    `is_clicked_order_processing_pill`,
    `is_clicked_order_shipped_pill`,
    `is_clicked_order_delivered_pill`,
    `is_clicked_aip_order_accordian_order_confirmed`,
    `is_clicked_aip_order_accordian_order_processing`,
    `is_clicked_aip_order_accordian_order_shipped`,
    `is_clicked_aip_order_accord`,
    `is_aip_setup_start`,
    `is_ows_start`,
    `is_oobe_complete`,
    `is_aip_setup_complete`,
    `is_clicked_support`,
    `is_oobe_support_session`,
    `total_printer_count`,
    `total_device_count`,
    `total_accessory_count`,
    `total_pc_count`,
    `max_total_printer_count`,
    `max_total_device_count`,
    `max_total_accessory_count`,
    `max_total_pc_count`;

-- =====================================================
-- Aggregated Monthly Metrics View
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    
    -- Aggregated Measures
    SUM(`viewed_paas_tracking_card`) AS `total_viewed_paas_tracking_card`,
    SUM(`clicked_expand`) AS `total_clicked_expand`,
    SUM(`clicked_order_confirmation`) AS `total_clicked_order_confirmation`,
    SUM(`clicked_order_processing`) AS `total_clicked_order_processing`,
    SUM(`clicked_track_delivery`) AS `total_clicked_track_delivery`,
    SUM(`clicked_complete_setup`) AS `total_clicked_complete_setup`,
    SUM(`confirmed`) AS `total_confirmed`,
    SUM(`processed`) AS `total_processed`,
    SUM(`shipped`) AS `total_shipped`,
    SUM(`delivered`) AS `total_delivered`,
    SUM(`onboarded`) AS `total_onboarded`,
    SUM(`support_cases`) AS `total_support_cases`,
    SUM(`order_confirmed_pill`) AS `total_order_confirmed_pill`,
    SUM(`processing_pill`) AS `total_processing_pill`,
    SUM(`shipped_pill`) AS `total_shipped_pill`,
    SUM(`delivered_pill`) AS `total_delivered_pill`,
    
    -- Count Aggregations
    SUM(`total_printer_count`) AS `sum_total_printer_count`,
    SUM(`total_device_count`) AS `sum_total_device_count`,
    SUM(`total_accessory_count`) AS `sum_total_accessory_count`,
    SUM(`total_pc_count`) AS `sum_total_pc_count`,
    SUM(`max_total_printer_count`) AS `sum_max_total_printer_count`,
    SUM(`max_total_device_count`) AS `sum_max_total_device_count`,
    SUM(`max_total_accessory_count`) AS `sum_max_total_accessory_count`,
    SUM(`max_total_pc_count`) AS `sum_max_total_pc_count`,
    
    -- Session Counts
    COUNT(DISTINCT `session_id`) AS `unique_sessions`,
    COUNT(DISTINCT `app_package_deployed_uuid`) AS `unique_app_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
GROUP BY 
    DATE_TRUNC('MONTH', `session_start_date_time`),
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`;

-- =====================================================
-- Dashboard-Ready View for Lakeview Integration
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard AS
SELECT 
    `month_date`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    CASE WHEN `is_hpid_signed_in` = true THEN 'Signed In' ELSE 'Not Signed In' END AS `hpid_status`,
    
    -- Core Metrics for Visualization
    `total_viewed_paas_tracking_card` AS `paas_tracking_card_views`,
    `total_clicked_expand` AS `clicks_on_expand`,
    `total_clicked_order_confirmation` AS `order_confirmation_clicks`,
    `total_clicked_order_processing` AS `processing_clicks`,
    `total_clicked_track_delivery` AS `track_delivery_clicks`,
    `total_clicked_complete_setup` AS `complete_setup_clicks`,
    
    -- Status Metrics
    `total_confirmed` AS `confirmed_status`,
    `total_processed` AS `processed_status`,
    `total_shipped` AS `shipped_status`,
    `total_delivered` AS `delivered_status`,
    `total_onboarded` AS `onboarded_users`,
    `total_support_cases` AS `support_cases`,
    
    -- Pill Click Metrics
    `total_order_confirmed_pill` AS `confirmed_pill_clicks`,
    `total_processing_pill` AS `processing_pill_clicks`,
    `total_shipped_pill` AS `shipped_pill_clicks`,
    `total_delivered_pill` AS `delivered_pill_clicks`,
    
    -- Device Counts
    `sum_total_printer_count` AS `total_printers`,
    `sum_total_device_count` AS `total_devices`,
    `sum_total_accessory_count` AS `total_accessories`,
    `sum_total_pc_count` AS `total_pcs`,
    
    -- Session Metrics
    `unique_sessions`,
    `unique_app_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics
ORDER BY `month_date` DESC, `os_platform`;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views
-- =====================================================