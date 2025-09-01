-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 2.0 - Enhanced with improved aggregations and error handling
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
    DATE_TRUNC('YEAR', `session_start_date_time`) AS `year_date`,
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
    COALESCE(`total_printer_count`, 0) AS `total_printer_count`,
    COALESCE(`total_device_count`, 0) AS `total_device_count`,
    COALESCE(`total_accessory_count`, 0) AS `total_accessory_count`,
    COALESCE(`total_pc_count`, 0) AS `total_pc_count`,
    COALESCE(`max_total_printer_count`, 0) AS `max_total_printer_count`,
    COALESCE(`max_total_device_count`, 0) AS `max_total_device_count`,
    COALESCE(`max_total_accessory_count`, 0) AS `max_total_accessory_count`,
    COALESCE(`max_total_pc_count`, 0) AS `max_total_pc_count`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- Calculated Measures View (ThoughtSpot Formulas)
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics AS
SELECT 
    `month_date`,
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
    
    -- ThoughtSpot Formula: Processed
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
    
    -- ThoughtSpot Formula: Delivered
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
    
    -- Session Counts
    COUNT(DISTINCT `session_id`) AS `unique_sessions`,
    COUNT(DISTINCT `app_package_deployed_uuid`) AS `unique_app_packages`,
    COUNT(DISTINCT `device_app_package_deployed_uuid`) AS `unique_device_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
GROUP BY 
    `month_date`,
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
    
    -- Core Metrics for Visualization (matching ThoughtSpot liveboard)
    COALESCE(`viewed_paas_tracking_card`, 0) AS `paas_tracking_card_views`,
    COALESCE(`clicked_expand`, 0) AS `clicks_on_expand`,
    COALESCE(`clicked_order_confirmation`, 0) AS `order_confirmation_clicks`,
    COALESCE(`clicked_order_processing`, 0) AS `processing_clicks`,
    COALESCE(`clicked_track_delivery`, 0) AS `track_delivery_clicks`,
    COALESCE(`clicked_complete_setup`, 0) AS `complete_setup_clicks`,
    
    -- Status Metrics (matching ThoughtSpot liveboard)
    COALESCE(`confirmed`, 0) AS `confirmed_status`,
    COALESCE(`processed`, 0) AS `processed_status`,
    COALESCE(`shipped`, 0) AS `shipped_status`,
    COALESCE(`delivered`, 0) AS `delivered_status`,
    COALESCE(`onboarded`, 0) AS `onboarded_users`,
    COALESCE(`support_cases`, 0) AS `support_cases`,
    
    -- Pill Click Metrics (matching ThoughtSpot liveboard)
    COALESCE(`order_confirmed_pill`, 0) AS `confirmed_pill_clicks`,
    COALESCE(`processing_pill`, 0) AS `processing_pill_clicks`,
    COALESCE(`shipped_pill`, 0) AS `shipped_pill_clicks`,
    COALESCE(`delivered_pill`, 0) AS `delivered_pill_clicks`,
    
    -- Device Counts
    COALESCE(`sum_total_printer_count`, 0) AS `total_printers`,
    COALESCE(`sum_total_device_count`, 0) AS `total_devices`,
    COALESCE(`sum_total_accessory_count`, 0) AS `total_accessories`,
    COALESCE(`sum_total_pc_count`, 0) AS `total_pcs`,
    COALESCE(`sum_max_total_printer_count`, 0) AS `max_total_printers`,
    COALESCE(`sum_max_total_device_count`, 0) AS `max_total_devices`,
    COALESCE(`sum_max_total_accessory_count`, 0) AS `max_total_accessories`,
    COALESCE(`sum_max_total_pc_count`, 0) AS `max_total_pcs`,
    
    -- Session Metrics
    COALESCE(`unique_sessions`, 0) AS `unique_sessions`,
    COALESCE(`unique_app_packages`, 0) AS `unique_app_packages`,
    COALESCE(`unique_device_packages`, 0) AS `unique_device_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics
ORDER BY `month_date` DESC, `os_platform`;

-- =====================================================
-- Additional Views for Specific Dashboard Visualizations
-- =====================================================

-- View for Viz_1: PaaS Tracking Card Views by OS Platform and Month
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_card_views_by_platform AS
SELECT 
    `month_date`,
    `os_platform`,
    `paas_tracking_card_views` AS `viewed_paas_tracking_card`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
WHERE `paas_tracking_card_views` > 0
ORDER BY `month_date`, `os_platform`;

-- View for Viz_2: Clicks on Expand by OS Platform and Month
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_card_expand_clicks AS
SELECT 
    `month_date`,
    `os_platform`,
    `clicks_on_expand` AS `clicked_expand`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
WHERE `clicks_on_expand` > 0
ORDER BY `month_date`, `os_platform`;

-- View for Viz_3: Clicks on Each Status
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_card_status_clicks AS
SELECT 
    `month_date`,
    `order_confirmation_clicks` AS `clicked_order_confirmation`,
    `processing_clicks` AS `clicked_order_processing`,
    `track_delivery_clicks` AS `clicked_track_delivery`,
    `complete_setup_clicks` AS `clicked_complete_setup`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
ORDER BY `month_date`;

-- View for Viz_4: Delivered vs Onboarded and Support Cases
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_card_delivery_metrics AS
SELECT 
    `month_date`,
    `delivered_status` AS `delivered`,
    `onboarded_users` AS `onboarded`,
    `support_cases`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
ORDER BY `month_date`;

-- View for Viz_5: Tracking Card Status Pill Clicks
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_card_pill_clicks AS
SELECT 
    `month_date`,
    `confirmed_pill_clicks` AS `order_confirmed_pill`,
    `processing_pill_clicks` AS `processing_pill`,
    `shipped_pill_clicks` AS `shipped_pill`,
    `delivered_pill_clicks` AS `delivered_pill`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
ORDER BY `month_date`;

-- View for Viz_6: Tracking Card Status
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_card_status_metrics AS
SELECT 
    `month_date`,
    `processed_status` AS `processed`,
    `shipped_status` AS `shipped`,
    `confirmed_status` AS `confirmed`,
    `delivered_status` AS `delivered`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
ORDER BY `month_date`;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views - Version 2.0
-- =====================================================