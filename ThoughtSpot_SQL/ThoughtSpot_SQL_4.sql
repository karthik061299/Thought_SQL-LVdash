-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card - Final Version
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL
-- =====================================================

-- Base Table: team_css_analytics_prod.hpx_analytics.paas_tracking_card
-- Connection: dataos-prod-css-analytics (Databricks)
-- Model: PaaS_tracking_card_hpx
-- Liveboard: PaaS Tracking Card

-- =====================================================
-- PRIMARY METRIC VIEW: paas_tracking_card_metrics
-- Contains all dimensions and measures from ThoughtSpot model
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Primary Identifiers
    `session_id`,
    `app_package_deployed_uuid`,
    `device_app_package_deployed_uuid`,
    `associated_device_session_id`,
    `aip_device_uuid`,
    
    -- Dimensional Attributes
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
    
    -- Boolean Flags
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
    `is_aip_setup_complete`,
    `is_oobe_support_session`,
    
    -- Device Count Measures
    `total_printer_count`,
    `total_device_count`,
    `total_accessory_count`,
    `total_pc_count`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- THOUGHTSPOT FORMULA METRICS VIEW
-- Pre-calculated ThoughtSpot formulas with proper aggregation
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_formula_metrics AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`,
    
    -- ThoughtSpot Formulas
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) END AS `viewed_paas_tracking_card`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) END AS `clicked_expand`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) END AS `clicked_order_confirmation`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) END AS `clicked_order_processing`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) END AS `clicked_track_delivery`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) END AS `clicked_complete_setup`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) END AS `confirmed`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) END AS `delivered`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) END AS `processed`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) END AS `shipped`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) END AS `onboarded`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) END AS `support_cases`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) END AS `order_confirmed_pill`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) END AS `processing_pill`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) END AS `shipped_pill`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) END AS `delivered_pill`,
    
    -- Aggregated Counts
    SUM(`total_printer_count`) AS `total_printer_count_sum`,
    SUM(`total_device_count`) AS `total_device_count_sum`,
    SUM(`total_accessory_count`) AS `total_accessory_count_sum`,
    SUM(`total_pc_count`) AS `total_pc_count_sum`,
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
-- LIVEBOARD VISUALIZATION VIEWS
-- =====================================================

-- Viz_1: PaaS Tracking Card Views
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_views AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) END AS `viewed_paas_tracking_card`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY DATE_TRUNC('month', `session_start_date_time`), `os_platform`;

-- Viz_2: Clicks on Expand
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_expand_clicks AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) END AS `clicked_expand`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY DATE_TRUNC('month', `session_start_date_time`), `os_platform`;

-- Viz_3: Status Clicks
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_status_clicks AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) END AS `clicked_order_confirmation`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) END AS `clicked_order_processing`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) END AS `clicked_track_delivery`,
    CASE WHEN COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) END AS `clicked_complete_setup`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY DATE_TRUNC('month', `session_start_date_time`);

-- =====================================================
-- END OF METRIC VIEWS
-- =====================================================