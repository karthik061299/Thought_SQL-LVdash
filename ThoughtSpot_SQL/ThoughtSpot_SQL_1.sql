-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 1
-- =====================================================

-- Base Table View
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_base AS
SELECT 
    session_id AS `session_id`,
    app_package_deployed_uuid AS `app_package_deployed_uuid`,
    os_platform AS `os_platform`,
    app_name AS `app_name`,
    app_package_id AS `app_package_id`,
    app_version AS `app_version`,
    session_start_date_time AS `session_start_date_time`,
    geo_country_code AS `geo_country_code`,
    is_hpid_signed_in AS `is_hpid_signed_in`,
    total_printer_count AS `total_printer_count`,
    total_device_count AS `total_device_count`,
    total_accessory_count AS `total_accessory_count`,
    total_pc_count AS `total_pc_count`,
    is_viewed_aip_tracking_card AS `is_viewed_aip_tracking_card`,
    is_viewed_aip_tracking_card_order_confirmed AS `is_viewed_aip_tracking_card_order_confirmed`,
    is_viewed_aip_tracking_card_order_processing AS `is_viewed_aip_tracking_card_order_processing`,
    is_viewed_aip_tracking_card_order_shipped AS `is_viewed_aip_tracking_card_order_shipped`,
    is_viewed_aip_tracking_card_order_delivered AS `is_viewed_aip_tracking_card_order_delivered`,
    is_clicked_aip_order_accordian AS `is_clicked_aip_order_accordian`,
    is_clicked_order_confirmation AS `is_clicked_order_confirmation`,
    is_clicked_order_processing AS `is_clicked_order_processing`,
    is_clicked_track_delivery AS `is_clicked_track_delivery`,
    is_clicked_complete_setup AS `is_clicked_complete_setup`,
    is_clicked_order_confirmation_pill AS `is_clicked_order_confirmation_pill`,
    is_clicked_order_processing_pill AS `is_clicked_order_processing_pill`,
    is_clicked_order_shipped_pill AS `is_clicked_order_shipped_pill`,
    is_clicked_order_delivered_pill AS `is_clicked_order_delivered_pill`,
    device_app_package_deployed_uuid AS `device_app_package_deployed_uuid`,
    max_total_printer_count AS `max_total_printer_count`,
    max_total_device_count AS `max_total_device_count`,
    max_total_accessory_count AS `max_total_accessory_count`,
    max_total_pc_count AS `max_total_pc_count`,
    associated_device_session_id AS `associated_device_session_id`,
    aip_device_uuid AS `aip_device_uuid`,
    is_associated_device AS `is_associated_device`,
    is_clicked_aip_order_accordian_order_processing AS `is_clicked_aip_order_accordian_order_processing`,
    is_clicked_aip_order_accord AS `is_clicked_aip_order_accord`,
    is_aip_setup_start AS `is_aip_setup_start`,
    is_clicked_aip_order_accordian_order_confirmed AS `is_clicked_aip_order_accordian_order_confirmed`,
    is_ows_start AS `is_ows_start`,
    is_clicked_aip_order_accordian_order_shipped AS `is_clicked_aip_order_accordian_order_shipped`,
    is_oobe_complete AS `is_oobe_complete`,
    is_aip_setup_complete AS `is_aip_setup_complete`,
    is_clicked_support AS `is_clicked_support`,
    is_oobe_support_session AS `is_oobe_support_session`,
    DATE(session_start_date_time) AS `date`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- Metric Views with Calculated Measures
-- =====================================================

-- Main Metrics View
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_metrics AS
SELECT 
    `date`,
    DATE_TRUNC('month', `date`) AS `month`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    
    -- Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END)
    END AS `viewed_paas_tracking_card`,
    
    -- Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_expand`,
    
    -- Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_order_confirmation`,
    
    -- Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_order_processing`,
    
    -- Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_track_delivery`,
    
    -- Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_complete_setup`,
    
    -- Confirmed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END)
    END AS `confirmed`,
    
    -- Delivered
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END)
    END AS `delivered`,
    
    -- Processed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END)
    END AS `processed`,
    
    -- Shipped
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END)
    END AS `shipped`,
    
    -- Onboarded
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END)
    END AS `onboarded`,
    
    -- Support Cases
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END)
    END AS `support_cases`,
    
    -- Pill Clicks
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
    END AS `delivered_pill`,
    
    -- Aggregated Measures
    SUM(`total_printer_count`) AS `total_printer_count`,
    SUM(`total_device_count`) AS `total_device_count`,
    SUM(`total_accessory_count`) AS `total_accessory_count`,
    SUM(`total_pc_count`) AS `total_pc_count`,
    SUM(`max_total_printer_count`) AS `max_total_printer_count`,
    SUM(`max_total_device_count`) AS `max_total_device_count`,
    SUM(`max_total_accessory_count`) AS `max_total_accessory_count`,
    SUM(`max_total_pc_count`) AS `max_total_pc_count`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_base
GROUP BY 
    `date`,
    DATE_TRUNC('month', `date`),
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`;

-- =====================================================
-- Monthly Aggregated View for Dashboard Visualizations
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_monthly_metrics AS
SELECT 
    `month`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    
    SUM(`viewed_paas_tracking_card`) AS `viewed_paas_tracking_card`,
    SUM(`clicked_expand`) AS `clicked_expand`,
    SUM(`clicked_order_confirmation`) AS `clicked_order_confirmation`,
    SUM(`clicked_order_processing`) AS `clicked_order_processing`,
    SUM(`clicked_track_delivery`) AS `clicked_track_delivery`,
    SUM(`clicked_complete_setup`) AS `clicked_complete_setup`,
    SUM(`confirmed`) AS `confirmed`,
    SUM(`delivered`) AS `delivered`,
    SUM(`processed`) AS `processed`,
    SUM(`shipped`) AS `shipped`,
    SUM(`onboarded`) AS `onboarded`,
    SUM(`support_cases`) AS `support_cases`,
    SUM(`order_confirmed_pill`) AS `order_confirmed_pill`,
    SUM(`processing_pill`) AS `processing_pill`,
    SUM(`shipped_pill`) AS `shipped_pill`,
    SUM(`delivered_pill`) AS `delivered_pill`,
    SUM(`total_printer_count`) AS `total_printer_count`,
    SUM(`total_device_count`) AS `total_device_count`,
    SUM(`total_accessory_count`) AS `total_accessory_count`,
    SUM(`total_pc_count`) AS `total_pc_count`,
    SUM(`max_total_printer_count`) AS `max_total_printer_count`,
    SUM(`max_total_device_count`) AS `max_total_device_count`,
    SUM(`max_total_accessory_count`) AS `max_total_accessory_count`,
    SUM(`max_total_pc_count`) AS `max_total_pc_count`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_metrics
GROUP BY 
    `month`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`;

-- =====================================================
-- Individual Attribute Views for Filtering
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_attributes AS
SELECT DISTINCT
    `session_id`,
    `app_package_deployed_uuid`,
    `os_platform`,
    `app_name`,
    `app_package_id`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `device_app_package_deployed_uuid`,
    `associated_device_session_id`,
    `aip_device_uuid`,
    `is_associated_device`,
    `is_oobe_complete`,
    `is_aip_setup_complete`,
    `is_clicked_support`,
    `is_aip_setup_start`,
    `is_ows_start`,
    `is_oobe_support_session`,
    `date`,
    DATE_TRUNC('month', `date`) AS `month`,
    DATE_TRUNC('year', `date`) AS `year`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_base;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views
-- =====================================================