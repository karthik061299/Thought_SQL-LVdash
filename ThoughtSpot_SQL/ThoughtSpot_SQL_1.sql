-- Version: 1
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from ThoughtSpot TML assets: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL Environment
-- Catalog: team_css_analytics_prod, Schema: hpx_analytics

-- =============================================================================
-- BASE METRIC VIEW: PaaS Tracking Card Analytics
-- =============================================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Base Dimensions
    `session_id`,
    `app_package_deployed_uuid`,
    `device_app_package_deployed_uuid`,
    `os_platform`,
    `app_name`,
    `app_package_id`,
    `app_version`,
    `geo_country_code`,
    `aip_device_uuid`,
    `associated_device_session_id`,
    
    -- Date Dimensions
    `session_start_date_time`,
    DATE(`session_start_date_time`) AS `date`,
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    
    -- Boolean Attributes
    `is_hpid_signed_in`,
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
    
    -- Base Measures (SUM aggregation)
    `total_printer_count`,
    `total_device_count`,
    `total_accessory_count`,
    `total_pc_count`,
    `max_total_printer_count`,
    `max_total_device_count`,
    `max_total_accessory_count`,
    `max_total_pc_count`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =============================================================================
-- CALCULATED MEASURES VIEW: ThoughtSpot Formula Metrics
-- =============================================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
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
    
    -- Confirmed Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END)
    END AS `confirmed`,
    
    -- Processed Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END)
    END AS `processed`,
    
    -- Shipped Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END)
    END AS `shipped`,
    
    -- Delivered Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END)
    END AS `delivered`,
    
    -- Onboarded (Delivered AND Setup Complete)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END)
    END AS `onboarded`,
    
    -- Support Cases (Delivered AND Support Session)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END)
    END AS `support_cases`,
    
    -- Pill Click Metrics
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
    
    -- Base Aggregated Measures
    SUM(`total_printer_count`) AS `total_printer_count_sum`,
    SUM(`total_device_count`) AS `total_device_count_sum`,
    SUM(`total_accessory_count`) AS `total_accessory_count_sum`,
    SUM(`total_pc_count`) AS `total_pc_count_sum`,
    SUM(`max_total_printer_count`) AS `max_total_printer_count_sum`,
    SUM(`max_total_device_count`) AS `max_total_device_count_sum`,
    SUM(`max_total_accessory_count`) AS `max_total_accessory_count_sum`,
    SUM(`max_total_pc_count`) AS `max_total_pc_count_sum`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`),
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`;

-- =============================================================================
-- LIVEBOARD VISUALIZATION VIEWS
-- =============================================================================

-- View 1: PaaS Tracking Card Views (Viz_1)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_views_by_platform AS
SELECT 
    `month_date`,
    `os_platform`,
    `viewed_paas_tracking_card`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics
WHERE `viewed_paas_tracking_card` IS NOT NULL
ORDER BY `month_date`, `os_platform`;

-- View 2: Clicks on Expand - PaaS Tracking Card (Viz_2)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_expand_clicks AS
SELECT 
    `month_date`,
    `os_platform`,
    `clicked_expand`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics
WHERE `clicked_expand` IS NOT NULL
ORDER BY `month_date`, `os_platform`;

-- View 3: Clicks on Each Status - PaaS Tracking Card (Viz_3)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_status_clicks AS
SELECT 
    `month_date`,
    `clicked_order_confirmation` AS `complete_setup`,
    `clicked_order_confirmation` AS `order_confirmation`,
    `clicked_order_processing` AS `processing`,
    `clicked_track_delivery` AS `track_delivery`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics
WHERE `clicked_order_confirmation` IS NOT NULL 
   OR `clicked_order_processing` IS NOT NULL 
   OR `clicked_track_delivery` IS NOT NULL
ORDER BY `month_date`;

-- View 4: Delivered vs Onboarded and Support Cases (Viz_4)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_delivery_metrics AS
SELECT 
    `month_date`,
    `delivered`,
    `onboarded`,
    `support_cases`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics
WHERE `delivered` IS NOT NULL 
   OR `onboarded` IS NOT NULL 
   OR `support_cases` IS NOT NULL
ORDER BY `month_date`;

-- View 5: Tracking Card Status Pill Clicks (Viz_5)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_pill_clicks AS
SELECT 
    `month_date`,
    `order_confirmed_pill` AS `order_confirmed`,
    `processing_pill` AS `processing`,
    `shipped_pill` AS `shipped`,
    `delivered_pill` AS `delivered`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics
WHERE `order_confirmed_pill` IS NOT NULL 
   OR `processing_pill` IS NOT NULL 
   OR `shipped_pill` IS NOT NULL 
   OR `delivered_pill` IS NOT NULL
ORDER BY `month_date`;

-- View 6: Tracking Card Status (Viz_6)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_status_overview AS
SELECT 
    `month_date`,
    `confirmed`,
    `processed`,
    `shipped`,
    `delivered`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics
WHERE `confirmed` IS NOT NULL 
   OR `processed` IS NOT NULL 
   OR `shipped` IS NOT NULL 
   OR `delivered` IS NOT NULL
ORDER BY `month_date`;

-- =============================================================================
-- SUMMARY ANALYTICS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_summary AS
SELECT 
    `month_date`,
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`,
    
    -- Key Performance Metrics
    `viewed_paas_tracking_card`,
    `clicked_expand`,
    `delivered`,
    `onboarded`,
    `support_cases`,
    
    -- Conversion Rates (as percentages)
    CASE 
        WHEN `viewed_paas_tracking_card` > 0 
        THEN ROUND((`clicked_expand` * 100.0) / `viewed_paas_tracking_card`, 2)
        ELSE NULL 
    END AS `expand_conversion_rate_pct`,
    
    CASE 
        WHEN `delivered` > 0 
        THEN ROUND((`onboarded` * 100.0) / `delivered`, 2)
        ELSE NULL 
    END AS `onboarding_completion_rate_pct`,
    
    CASE 
        WHEN `delivered` > 0 
        THEN ROUND((`support_cases` * 100.0) / `delivered`, 2)
        ELSE NULL 
    END AS `support_case_rate_pct`,
    
    -- Device Counts
    `total_printer_count_sum`,
    `total_device_count_sum`,
    `total_accessory_count_sum`,
    `total_pc_count_sum`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics
ORDER BY `month_date`, `os_platform`, `geo_country_code`;

-- =============================================================================
-- END OF METRIC VIEWS
-- =============================================================================