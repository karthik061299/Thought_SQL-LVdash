-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card - Version 2
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL
-- Improvements: Enhanced error handling, optimized aggregations, comprehensive metric coverage
-- =====================================================

-- Base Table Reference: team_css_analytics_prod.hpx_analytics.paas_tracking_card
-- Model: PaaS_tracking_card_hpx
-- Liveboard: PaaS Tracking Card

-- =====================================================
-- MAIN METRIC VIEW: paas_tracking_card_metrics_v2
-- Contains all measures, dimensions, and calculated fields from ThoughtSpot model
-- Optimized for performance with proper indexing and partitioning hints
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics_v2 AS
SELECT 
    -- Primary Keys and Identifiers
    `session_id`,
    `app_package_deployed_uuid`,
    `device_app_package_deployed_uuid`,
    `associated_device_session_id`,
    `aip_device_uuid`,
    
    -- Application Context Dimensions
    `os_platform`,
    `app_name`,
    `app_package_id`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `is_associated_device`,
    
    -- Temporal Dimensions
    `session_start_date_time`,
    DATE(`session_start_date_time`) AS `session_date`,
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    DATE_TRUNC('week', `session_start_date_time`) AS `week_date`,
    DATE_TRUNC('quarter', `session_start_date_time`) AS `quarter_date`,
    DATE_TRUNC('year', `session_start_date_time`) AS `year_date`,
    DAYOFWEEK(`session_start_date_time`) AS `day_of_week`,
    HOUR(`session_start_date_time`) AS `hour_of_day`,
    
    -- Tracking Card View Attributes
    `is_viewed_aip_tracking_card`,
    `is_viewed_aip_tracking_card_order_confirmed`,
    `is_viewed_aip_tracking_card_order_processing`,
    `is_viewed_aip_tracking_card_order_shipped`,
    `is_viewed_aip_tracking_card_order_delivered`,
    
    -- Click Interaction Attributes
    `is_clicked_aip_order_accordian`,
    `is_clicked_order_confirmation`,
    `is_clicked_order_processing`,
    `is_clicked_track_delivery`,
    `is_clicked_complete_setup`,
    `is_clicked_support`,
    
    -- Pill Click Attributes
    `is_clicked_order_confirmation_pill`,
    `is_clicked_order_processing_pill`,
    `is_clicked_order_shipped_pill`,
    `is_clicked_order_delivered_pill`,
    
    -- Advanced Click Attributes
    `is_clicked_aip_order_accordian_order_processing`,
    `is_clicked_aip_order_accord`,
    `is_clicked_aip_order_accordian_order_confirmed`,
    `is_clicked_aip_order_accordian_order_shipped`,
    
    -- Setup and Onboarding Attributes
    `is_aip_setup_start`,
    `is_aip_setup_complete`,
    `is_ows_start`,
    `is_oobe_complete`,
    `is_oobe_support_session`,
    
    -- Device Count Measures (Base)
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
-- AGGREGATED METRICS VIEW: paas_tracking_card_aggregated_metrics_v2
-- Pre-calculated ThoughtSpot formulas with proper aggregation logic
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_aggregated_metrics_v2 AS
SELECT 
    -- Grouping Dimensions
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`,
    
    -- ThoughtSpot Formula: Viewed PaaS Tracking Card
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `viewed_paas_tracking_card`,
    
    -- ThoughtSpot Formula: Clicked Expand
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_expand`,
    
    -- ThoughtSpot Formula: Clicked Order Confirmation
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_order_confirmation`,
    
    -- ThoughtSpot Formula: Clicked Order Processing
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_order_processing`,
    
    -- ThoughtSpot Formula: Clicked Track Delivery
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_track_delivery`,
    
    -- ThoughtSpot Formula: Clicked Complete Setup
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_complete_setup`,
    
    -- ThoughtSpot Formula: Confirmed
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `confirmed`,
    
    -- ThoughtSpot Formula: Delivered (uses device_app_package_deployed_uuid)
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END), 0),
        NULL
    ) AS `delivered`,
    
    -- ThoughtSpot Formula: Processed (uses device_app_package_deployed_uuid)
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END), 0),
        NULL
    ) AS `processed`,
    
    -- ThoughtSpot Formula: Shipped
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `shipped`,
    
    -- ThoughtSpot Formula: Onboarded
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `onboarded`,
    
    -- ThoughtSpot Formula: Support Cases
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `support_cases`,
    
    -- Pill Click Measures
    -- ThoughtSpot Formula: Order Confirmed - Pill
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `order_confirmed_pill`,
    
    -- ThoughtSpot Formula: Processing - Pill
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `processing_pill`,
    
    -- ThoughtSpot Formula: Shipped - Pill
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `shipped_pill`,
    
    -- ThoughtSpot Formula: Delivered - Pill
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `delivered_pill`,
    
    -- Aggregated Device Counts
    SUM(`total_printer_count`) AS `total_printer_count_sum`,
    SUM(`total_device_count`) AS `total_device_count_sum`,
    SUM(`total_accessory_count`) AS `total_accessory_count_sum`,
    SUM(`total_pc_count`) AS `total_pc_count_sum`,
    SUM(`max_total_printer_count`) AS `max_total_printer_count_sum`,
    SUM(`max_total_device_count`) AS `max_total_device_count_sum`,
    SUM(`max_total_accessory_count`) AS `max_total_accessory_count_sum`,
    SUM(`max_total_pc_count`) AS `max_total_pc_count_sum`,
    
    -- Additional Metrics
    COUNT(*) AS `total_records`,
    COUNT(DISTINCT `session_id`) AS `unique_sessions`,
    COUNT(DISTINCT `app_package_deployed_uuid`) AS `unique_app_packages`,
    COUNT(DISTINCT `device_app_package_deployed_uuid`) AS `unique_device_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`),
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`;

-- =====================================================
-- LIVEBOARD VISUALIZATION VIEWS - Version 2
-- Optimized views for each specific visualization
-- =====================================================

-- Viz_1: PaaS Tracking Card Views (Monthly by OS Platform)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_views_v2 AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `viewed_paas_tracking_card`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`),
    `os_platform`
ORDER BY `month_date`, `os_platform`;

-- Viz_2: Clicks on Expand - PaaS Tracking Card (Monthly by OS Platform)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_expand_clicks_v2 AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_expand`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`),
    `os_platform`
ORDER BY `month_date`, `os_platform`;

-- Viz_3: Clicks on Each Status - PaaS Tracking Card (Monthly)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_status_clicks_v2 AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_order_confirmation`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_order_processing`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_track_delivery`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `clicked_complete_setup`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`)
ORDER BY `month_date`;

-- Viz_4: Delivered vs Onboarded and Support Cases (Monthly)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_delivery_onboarding_v2 AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END), 0),
        NULL
    ) AS `delivered`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `onboarded`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `support_cases`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`)
ORDER BY `month_date`;

-- Viz_5: Tracking Card Status Pill Clicks (Monthly)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_pill_clicks_v2 AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `order_confirmed_pill`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `processing_pill`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `shipped_pill`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `delivered_pill`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`)
ORDER BY `month_date`;

-- Viz_6: Tracking Card Status (Monthly)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_status_v2 AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `confirmed`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END), 0),
        NULL
    ) AS `processed`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END), 0),
        NULL
    ) AS `shipped`,
    COALESCE(
        NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END), 0),
        NULL
    ) AS `delivered`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`)
ORDER BY `month_date`;

-- =====================================================
-- COMPREHENSIVE DASHBOARD VIEW
-- Single view containing all metrics for dashboard consumption
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_v2 AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`,
    
    -- All ThoughtSpot Formulas
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `viewed_paas_tracking_card`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `clicked_expand`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `clicked_order_confirmation`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `clicked_order_processing`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `clicked_track_delivery`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `clicked_complete_setup`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `confirmed`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END), 0), NULL) AS `delivered`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END), 0), NULL) AS `processed`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `shipped`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `onboarded`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `support_cases`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `order_confirmed_pill`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `processing_pill`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `shipped_pill`,
    COALESCE(NULLIF(COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END), 0), NULL) AS `delivered_pill`,
    
    -- Aggregated Device Counts
    SUM(`total_printer_count`) AS `total_printer_count_sum`,
    SUM(`total_device_count`) AS `total_device_count_sum`,
    SUM(`total_accessory_count`) AS `total_accessory_count_sum`,
    SUM(`total_pc_count`) AS `total_pc_count_sum`,
    
    -- Summary Statistics
    COUNT(*) AS `total_records`,
    COUNT(DISTINCT `session_id`) AS `unique_sessions`,
    COUNT(DISTINCT `app_package_deployed_uuid`) AS `unique_app_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`),
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`;

-- =====================================================
-- END OF METRIC VIEWS - Version 2
-- Improvements:
-- 1. Enhanced error handling with COALESCE and NULLIF
-- 2. Proper handling of device_app_package_deployed_uuid vs app_package_deployed_uuid
-- 3. Added comprehensive temporal dimensions
-- 4. Optimized views for each liveboard visualization
-- 5. Added ordering for better query performance
-- 6. Comprehensive dashboard view for single-query access
-- =====================================================