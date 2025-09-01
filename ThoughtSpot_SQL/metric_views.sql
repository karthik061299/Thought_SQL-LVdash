-- =====================================================
-- THOUGHTSPOT SQL GENERATION - PAAS TRACKING CARD METRIC VIEWS
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target Database: team_css_analytics_prod.hpx_analytics.paas_tracking_card
-- Generated for Databricks SQL compatibility
-- =====================================================

-- MAIN PAAS TRACKING CARD METRICS VIEW
-- This view contains all calculated measures from ThoughtSpot model formulas
-- Implements the exact logic from PaaS_tracking_card_hpx.model.tml

WITH paas_tracking_card_base AS (
    -- Base data structure matching the ThoughtSpot table schema
    -- In production, replace this CTE with: FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
    SELECT 
        'session_001' AS `session_id`,
        'app_001' AS `app_package_deployed_uuid`,
        'android' AS `os_platform`,
        'HP Smart' AS `app_name`,
        'com.hp.smart' AS `app_package_id`,
        '1.0.0' AS `app_version`,
        CURRENT_TIMESTAMP() AS `session_start_date_time`,
        'US' AS `geo_country_code`,
        true AS `is_hpid_signed_in`,
        5 AS `total_printer_count`,
        3 AS `total_device_count`,
        2 AS `total_accessory_count`,
        1 AS `total_pc_count`,
        'device_001' AS `device_app_package_deployed_uuid`,
        5 AS `max_total_printer_count`,
        3 AS `max_total_device_count`,
        2 AS `max_total_accessory_count`,
        1 AS `max_total_pc_count`,
        'assoc_session_001' AS `associated_device_session_id`,
        'aip_device_001' AS `aip_device_uuid`,
        true AS `is_associated_device`,
        true AS `is_viewed_aip_tracking_card`,
        true AS `is_viewed_aip_tracking_card_order_confirmed`,
        false AS `is_viewed_aip_tracking_card_order_processing`,
        false AS `is_viewed_aip_tracking_card_order_shipped`,
        false AS `is_viewed_aip_tracking_card_order_delivered`,
        true AS `is_clicked_aip_order_accordian`,
        true AS `is_clicked_order_confirmation`,
        false AS `is_clicked_order_processing`,
        false AS `is_clicked_track_delivery`,
        false AS `is_clicked_complete_setup`,
        true AS `is_clicked_order_confirmation_pill`,
        false AS `is_clicked_order_processing_pill`,
        false AS `is_clicked_order_shipped_pill`,
        false AS `is_clicked_order_delivered_pill`,
        true AS `is_clicked_aip_order_accordian_order_confirmed`,
        false AS `is_clicked_aip_order_accordian_order_processing`,
        false AS `is_clicked_aip_order_accordian_order_shipped`,
        true AS `is_clicked_aip_order_accord`,
        false AS `is_clicked_support`,
        false AS `is_aip_setup_start`,
        false AS `is_aip_setup_complete`,
        false AS `is_ows_start`,
        false AS `is_oobe_complete`,
        false AS `is_oobe_support_session`
    UNION ALL
    SELECT 
        'session_002' AS `session_id`,
        'app_002' AS `app_package_deployed_uuid`,
        'ios' AS `os_platform`,
        'HP Smart' AS `app_name`,
        'com.hp.smart' AS `app_package_id`,
        '1.1.0' AS `app_version`,
        CURRENT_TIMESTAMP() - INTERVAL 30 DAYS AS `session_start_date_time`,
        'CA' AS `geo_country_code`,
        false AS `is_hpid_signed_in`,
        3 AS `total_printer_count`,
        2 AS `total_device_count`,
        1 AS `total_accessory_count`,
        1 AS `total_pc_count`,
        'device_002' AS `device_app_package_deployed_uuid`,
        3 AS `max_total_printer_count`,
        2 AS `max_total_device_count`,
        1 AS `max_total_accessory_count`,
        1 AS `max_total_pc_count`,
        'assoc_session_002' AS `associated_device_session_id`,
        'aip_device_002' AS `aip_device_uuid`,
        false AS `is_associated_device`,
        true AS `is_viewed_aip_tracking_card`,
        true AS `is_viewed_aip_tracking_card_order_confirmed`,
        true AS `is_viewed_aip_tracking_card_order_processing`,
        true AS `is_viewed_aip_tracking_card_order_shipped`,
        true AS `is_viewed_aip_tracking_card_order_delivered`,
        true AS `is_clicked_aip_order_accordian`,
        true AS `is_clicked_order_confirmation`,
        true AS `is_clicked_order_processing`,
        true AS `is_clicked_track_delivery`,
        true AS `is_clicked_complete_setup`,
        true AS `is_clicked_order_confirmation_pill`,
        true AS `is_clicked_order_processing_pill`,
        true AS `is_clicked_order_shipped_pill`,
        true AS `is_clicked_order_delivered_pill`,
        true AS `is_clicked_aip_order_accordian_order_confirmed`,
        true AS `is_clicked_aip_order_accordian_order_processing`,
        true AS `is_clicked_aip_order_accordian_order_shipped`,
        true AS `is_clicked_aip_order_accord`,
        true AS `is_clicked_support`,
        true AS `is_aip_setup_start`,
        true AS `is_aip_setup_complete`,
        true AS `is_ows_start`,
        true AS `is_oobe_complete`,
        true AS `is_oobe_support_session`
    UNION ALL
    SELECT 
        'session_003' AS `session_id`,
        'app_003' AS `app_package_deployed_uuid`,
        'macos' AS `os_platform`,
        'HP Smart' AS `app_name`,
        'com.hp.smart' AS `app_package_id`,
        '1.2.0' AS `app_version`,
        CURRENT_TIMESTAMP() - INTERVAL 60 DAYS AS `session_start_date_time`,
        'UK' AS `geo_country_code`,
        true AS `is_hpid_signed_in`,
        2 AS `total_printer_count`,
        1 AS `total_device_count`,
        0 AS `total_accessory_count`,
        2 AS `total_pc_count`,
        'device_003' AS `device_app_package_deployed_uuid`,
        2 AS `max_total_printer_count`,
        1 AS `max_total_device_count`,
        0 AS `max_total_accessory_count`,
        2 AS `max_total_pc_count`,
        'assoc_session_003' AS `associated_device_session_id`,
        'aip_device_003' AS `aip_device_uuid`,
        true AS `is_associated_device`,
        true AS `is_viewed_aip_tracking_card`,
        false AS `is_viewed_aip_tracking_card_order_confirmed`,
        true AS `is_viewed_aip_tracking_card_order_processing`,
        false AS `is_viewed_aip_tracking_card_order_shipped`,
        false AS `is_viewed_aip_tracking_card_order_delivered`,
        false AS `is_clicked_aip_order_accordian`,
        false AS `is_clicked_order_confirmation`,
        true AS `is_clicked_order_processing`,
        false AS `is_clicked_track_delivery`,
        false AS `is_clicked_complete_setup`,
        false AS `is_clicked_order_confirmation_pill`,
        true AS `is_clicked_order_processing_pill`,
        false AS `is_clicked_order_shipped_pill`,
        false AS `is_clicked_order_delivered_pill`,
        false AS `is_clicked_aip_order_accordian_order_confirmed`,
        true AS `is_clicked_aip_order_accordian_order_processing`,
        false AS `is_clicked_aip_order_accordian_order_shipped`,
        false AS `is_clicked_aip_order_accord`,
        false AS `is_clicked_support`,
        true AS `is_aip_setup_start`,
        false AS `is_aip_setup_complete`,
        true AS `is_ows_start`,
        false AS `is_oobe_complete`,
        false AS `is_oobe_support_session`
)
SELECT 
    -- Base Dimensions
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    DATE(`session_start_date_time`) AS `date`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `app_name`,
    `app_package_id`,
    
    -- THOUGHTSPOT MODEL FORMULAS - EXACT IMPLEMENTATION
    -- Formula: Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) 
    END AS `viewed_paas_tracking_card`,
    
    -- Formula: Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_complete_setup`,
    
    -- Formula: Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_expand`,
    
    -- Formula: Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_order_confirmation`,
    
    -- Formula: Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_order_processing`,
    
    -- Formula: Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) 
    END AS `clicked_track_delivery`,
    
    -- Formula: Confirmed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) 
    END AS `confirmed`,
    
    -- Formula: Delivered
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) 
    END AS `delivered`,
    
    -- Formula: Delivered - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `delivered_pill`,
    
    -- Formula: Onboarded
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) 
    END AS `onboarded`,
    
    -- Formula: Order Confirmed - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `order_confirmed_pill`,
    
    -- Formula: Processed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) 
    END AS `processed`,
    
    -- Formula: Processing - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `processing_pill`,
    
    -- Formula: Shipped
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) 
    END AS `shipped`,
    
    -- Formula: Shipped - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) 
    END AS `shipped_pill`,
    
    -- Formula: Support Cases
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) 
    END AS `support_cases`,
    
    -- Base Aggregated Measures (SUM aggregation as per TML)
    SUM(`total_printer_count`) AS `total_printer_count_sum`,
    SUM(`total_device_count`) AS `total_device_count_sum`,
    SUM(`total_accessory_count`) AS `total_accessory_count_sum`,
    SUM(`total_pc_count`) AS `total_pc_count_sum`,
    SUM(`max_total_printer_count`) AS `max_total_printer_count_sum`,
    SUM(`max_total_device_count`) AS `max_total_device_count_sum`,
    SUM(`max_total_accessory_count`) AS `max_total_accessory_count_sum`,
    SUM(`max_total_pc_count`) AS `max_total_pc_count_sum`,
    
    -- Additional Calculated KPIs for Dashboard
    -- Conversion Rates
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) > 0 
        THEN ROUND(
            (COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) * 100.0 / 
             COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END)), 2
        )
        ELSE 0 
    END AS `expand_click_rate_percent`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) > 0 
        THEN ROUND(
            (COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) * 100.0 / 
             COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END)), 2
        )
        ELSE 0 
    END AS `onboarding_completion_rate_percent`
    
FROM paas_tracking_card_base
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`),
    DATE(`session_start_date_time`),
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `app_name`,
    `app_package_id`
ORDER BY 
    `month_date` DESC,
    `os_platform`,
    `app_version`

-- =====================================================
-- LIVEBOARD VISUALIZATION MAPPINGS
-- =====================================================
-- Viz 1: PaaS Tracking Card Views -> viewed_paas_tracking_card, os_platform, month_date
-- Viz 2: Clicks on Expand -> clicked_expand, os_platform, month_date  
-- Viz 3: Clicks on Each Status -> clicked_order_confirmation, clicked_order_processing, clicked_track_delivery, clicked_complete_setup, month_date
-- Viz 4: Delivered vs Onboarded and Support Cases -> delivered, onboarded, support_cases, month_date
-- Viz 5: Tracking Card Status Pill Clicks -> order_confirmed_pill, processing_pill, shipped_pill, delivered_pill, month_date
-- Viz 6: Tracking Card Status -> processed, shipped, confirmed, delivered, month_date
-- =====================================================
-- END OF THOUGHTSPOT SQL GENERATION
-- =====================================================