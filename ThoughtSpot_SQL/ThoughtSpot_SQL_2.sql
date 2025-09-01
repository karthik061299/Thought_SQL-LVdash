-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 2.0 - Enhanced with improved aggregations and performance optimizations
-- =====================================================

-- =====================================================
-- Base Metric View for PaaS Tracking Card Analytics
-- =====================================================

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
    DATE(`session_start_date_time`) AS `session_date`,
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    DATE_TRUNC('WEEK', `session_start_date_time`) AS `week_date`,
    DATE_TRUNC('YEAR', `session_start_date_time`) AS `year_date`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `device_app_package_deployed_uuid`,
    `associated_device_session_id`,
    `aip_device_uuid`,
    `is_associated_device`,
    
    -- Boolean Attributes for Tracking Card Views
    `is_viewed_aip_tracking_card`,
    `is_viewed_aip_tracking_card_order_confirmed`,
    `is_viewed_aip_tracking_card_order_processing`,
    `is_viewed_aip_tracking_card_order_shipped`,
    `is_viewed_aip_tracking_card_order_delivered`,
    
    -- Boolean Attributes for Click Actions
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
    `is_clicked_support`,
    
    -- Boolean Attributes for Setup and Onboarding
    `is_aip_setup_start`,
    `is_ows_start`,
    `is_oobe_complete`,
    `is_aip_setup_complete`,
    `is_oobe_support_session`,
    
    -- Count Measures (Direct from source)
    `total_printer_count`,
    `total_device_count`,
    `total_accessory_count`,
    `total_pc_count`,
    `max_total_printer_count`,
    `max_total_device_count`,
    `max_total_accessory_count`,
    `max_total_pc_count`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- Aggregated Metrics View with ThoughtSpot Formula Logic
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_aggregated_metrics AS
SELECT 
    `session_date`,
    `month_date`,
    `week_date`,
    `year_date`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    
    -- ThoughtSpot Formula: Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) 
    END AS `viewed_paas_tracking_card`,
    
    -- ThoughtSpot Formula: Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_expand`,
    
    -- ThoughtSpot Formula: Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_order_confirmation`,
    
    -- ThoughtSpot Formula: Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_order_processing`,
    
    -- ThoughtSpot Formula: Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_track_delivery`,
    
    -- ThoughtSpot Formula: Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_complete_setup`,
    
    -- ThoughtSpot Formula: Confirmed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) 
    END AS `confirmed`,
    
    -- ThoughtSpot Formula: Processed (using device_app_package_deployed_uuid)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) 
    END AS `processed`,
    
    -- ThoughtSpot Formula: Shipped
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) 
    END AS `shipped`,
    
    -- ThoughtSpot Formula: Delivered (using device_app_package_deployed_uuid)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) 
    END AS `delivered`,
    
    -- ThoughtSpot Formula: Onboarded
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN (`is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true) THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN (`is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true) THEN `app_package_deployed_uuid` END) 
    END AS `onboarded`,
    
    -- ThoughtSpot Formula: Support Cases
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN (`is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true) THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN (`is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true) THEN `app_package_deployed_uuid` END) 
    END AS `support_cases`,
    
    -- ThoughtSpot Formula: Order Confirmed - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `order_confirmed_pill`,
    
    -- ThoughtSpot Formula: Processing - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `processing_pill`,
    
    -- ThoughtSpot Formula: Shipped - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `shipped_pill`,
    
    -- ThoughtSpot Formula: Delivered - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `delivered_pill`,
    
    -- Aggregated Count Measures
    SUM(`total_printer_count`) AS `sum_total_printer_count`,
    SUM(`total_device_count`) AS `sum_total_device_count`,
    SUM(`total_accessory_count`) AS `sum_total_accessory_count`,
    SUM(`total_pc_count`) AS `sum_total_pc_count`,
    SUM(`max_total_printer_count`) AS `sum_max_total_printer_count`,
    SUM(`max_total_device_count`) AS `sum_max_total_device_count`,
    SUM(`max_total_accessory_count`) AS `sum_max_total_accessory_count`,
    SUM(`max_total_pc_count`) AS `sum_max_total_pc_count`,
    
    -- Session and Package Counts
    COUNT(DISTINCT `session_id`) AS `unique_sessions`,
    COUNT(DISTINCT `app_package_deployed_uuid`) AS `unique_app_packages`,
    COUNT(DISTINCT `device_app_package_deployed_uuid`) AS `unique_device_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
GROUP BY 
    `session_date`,
    `month_date`,
    `week_date`,
    `year_date`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`;

-- =====================================================
-- Dashboard-Ready View for Lakeview Integration
-- Optimized for ThoughtSpot Liveboard Visualizations
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard AS
SELECT 
    -- Time Dimensions
    `session_date` AS `date`,
    `month_date`,
    `week_date`,
    `year_date`,
    
    -- Categorical Dimensions
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    CASE WHEN `is_hpid_signed_in` = true THEN 'Signed In' ELSE 'Not Signed In' END AS `hpid_status`,
    
    -- Core Tracking Card Metrics (matching ThoughtSpot visualizations)
    COALESCE(`viewed_paas_tracking_card`, 0) AS `viewed_paas_tracking_card`,
    COALESCE(`clicked_expand`, 0) AS `clicked_expand`,
    
    -- Status Click Metrics (Viz_3: Clicks on Each Status)
    COALESCE(`clicked_order_confirmation`, 0) AS `clicked_order_confirmation`,
    COALESCE(`clicked_order_processing`, 0) AS `clicked_order_processing`, 
    COALESCE(`clicked_track_delivery`, 0) AS `clicked_track_delivery`,
    COALESCE(`clicked_complete_setup`, 0) AS `clicked_complete_setup`,
    
    -- Status Tracking Metrics (Viz_6: Tracking Card Status)
    COALESCE(`confirmed`, 0) AS `confirmed`,
    COALESCE(`processed`, 0) AS `processed`,
    COALESCE(`shipped`, 0) AS `shipped`,
    COALESCE(`delivered`, 0) AS `delivered`,
    
    -- Onboarding and Support Metrics (Viz_4: Delivered vs Onboarded and Support Cases)
    COALESCE(`onboarded`, 0) AS `onboarded`,
    COALESCE(`support_cases`, 0) AS `support_cases`,
    
    -- Pill Click Metrics (Viz_5: Tracking Card Status Pill Clicks)
    COALESCE(`order_confirmed_pill`, 0) AS `order_confirmed_pill`,
    COALESCE(`processing_pill`, 0) AS `processing_pill`,
    COALESCE(`shipped_pill`, 0) AS `shipped_pill`,
    COALESCE(`delivered_pill`, 0) AS `delivered_pill`,
    
    -- Device and Hardware Counts
    `sum_total_printer_count` AS `total_printer_count`,
    `sum_total_device_count` AS `total_device_count`,
    `sum_total_accessory_count` AS `total_accessory_count`,
    `sum_total_pc_count` AS `total_pc_count`,
    `sum_max_total_printer_count` AS `max_total_printer_count`,
    `sum_max_total_device_count` AS `max_total_device_count`,
    `sum_max_total_accessory_count` AS `max_total_accessory_count`,
    `sum_max_total_pc_count` AS `max_total_pc_count`,
    
    -- Session Metrics
    `unique_sessions`,
    `unique_app_packages`,
    `unique_device_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_aggregated_metrics
ORDER BY `month_date` DESC, `os_platform`, `app_version`;

-- =====================================================
-- Monthly Summary View for High-Level Reporting
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_summary AS
SELECT 
    `month_date`,
    `os_platform`,
    
    -- Monthly Totals
    SUM(`viewed_paas_tracking_card`) AS `monthly_views`,
    SUM(`clicked_expand`) AS `monthly_expand_clicks`,
    SUM(`delivered`) AS `monthly_delivered`,
    SUM(`onboarded`) AS `monthly_onboarded`,
    SUM(`support_cases`) AS `monthly_support_cases`,
    
    -- Conversion Rates
    CASE 
        WHEN SUM(`viewed_paas_tracking_card`) > 0 
        THEN ROUND((SUM(`clicked_expand`) * 100.0 / SUM(`viewed_paas_tracking_card`)), 2)
        ELSE 0 
    END AS `expand_conversion_rate_pct`,
    
    CASE 
        WHEN SUM(`delivered`) > 0 
        THEN ROUND((SUM(`onboarded`) * 100.0 / SUM(`delivered`)), 2)
        ELSE 0 
    END AS `onboarding_completion_rate_pct`,
    
    CASE 
        WHEN SUM(`delivered`) > 0 
        THEN ROUND((SUM(`support_cases`) * 100.0 / SUM(`delivered`)), 2)
        ELSE 0 
    END AS `support_case_rate_pct`,
    
    -- Session Metrics
    SUM(`unique_sessions`) AS `monthly_sessions`,
    SUM(`unique_app_packages`) AS `monthly_app_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
GROUP BY `month_date`, `os_platform`
ORDER BY `month_date` DESC, `os_platform`;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views - Version 2.0
-- =====================================================