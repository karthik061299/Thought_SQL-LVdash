-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 4.0 - Production Ready
-- =====================================================
-- 
-- This SQL file converts ThoughtSpot TML assets into Databricks-compatible
-- metric views that preserve all semantic layer definitions, relationships,
-- and calculated measures from the original ThoughtSpot model.
--
-- Source Files:
-- - paas_tracking_card.table.tml
-- - PaaS_tracking_card_hpx.model.tml  
-- - PaaS Tracking Card.liveboard.tml
-- - dataos-prod-css-analytics.connection.tml
-- - Manifest.yaml
--
-- Target: team_css_analytics_prod.hpx_analytics catalog/schema
-- =====================================================

-- Base metric view that mirrors ThoughtSpot table structure
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_base AS
SELECT 
    -- Primary identifiers
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    associated_device_session_id,
    aip_device_uuid,
    
    -- Application context
    os_platform,
    app_name,
    app_package_id,
    app_version,
    
    -- Temporal dimensions
    session_start_date_time,
    DATE(session_start_date_time) AS session_date,
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    DATE_TRUNC('YEAR', session_start_date_time) AS year_date,
    
    -- Geographic and user context
    geo_country_code,
    is_hpid_signed_in,
    is_associated_device,
    
    -- Tracking card view events
    is_viewed_aip_tracking_card,
    is_viewed_aip_tracking_card_order_confirmed,
    is_viewed_aip_tracking_card_order_processing,
    is_viewed_aip_tracking_card_order_shipped,
    is_viewed_aip_tracking_card_order_delivered,
    
    -- Click interaction events
    is_clicked_aip_order_accordian,
    is_clicked_order_confirmation,
    is_clicked_order_processing,
    is_clicked_track_delivery,
    is_clicked_complete_setup,
    
    -- Status pill click events
    is_clicked_order_confirmation_pill,
    is_clicked_order_processing_pill,
    is_clicked_order_shipped_pill,
    is_clicked_order_delivered_pill,
    
    -- Additional interaction events
    is_clicked_aip_order_accordian_order_confirmed,
    is_clicked_aip_order_accordian_order_processing,
    is_clicked_aip_order_accordian_order_shipped,
    is_clicked_aip_order_accord,
    is_clicked_support,
    
    -- Setup and onboarding status
    is_aip_setup_start,
    is_aip_setup_complete,
    is_ows_start,
    is_oobe_complete,
    is_oobe_support_session,
    
    -- Device count measures
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
-- ThoughtSpot Formula Metrics View
-- Implements all calculated measures from PaaS_tracking_card_hpx.model.tml
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_formulas AS
SELECT 
    month_date,
    session_date,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    
    -- ThoughtSpot Formula: "Viewed PaaS Tracking Card"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END), 
        0
    ) AS viewed_paas_tracking_card,
    
    -- ThoughtSpot Formula: "Clicked Expand"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END), 
        0
    ) AS clicked_expand,
    
    -- ThoughtSpot Formula: "Clicked Order Confirmation"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END), 
        0
    ) AS clicked_order_confirmation,
    
    -- ThoughtSpot Formula: "Clicked Order Processing"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END), 
        0
    ) AS clicked_order_processing,
    
    -- ThoughtSpot Formula: "Clicked Track Delivery"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END), 
        0
    ) AS clicked_track_delivery,
    
    -- ThoughtSpot Formula: "Clicked Complete Setup"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END), 
        0
    ) AS clicked_complete_setup,
    
    -- ThoughtSpot Formula: "Confirmed"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END), 
        0
    ) AS confirmed,
    
    -- ThoughtSpot Formula: "Processed"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END), 
        0
    ) AS processed,
    
    -- ThoughtSpot Formula: "Shipped"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END), 
        0
    ) AS shipped,
    
    -- ThoughtSpot Formula: "Delivered"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END), 
        0
    ) AS delivered,
    
    -- ThoughtSpot Formula: "Onboarded"
    NULLIF(
        COUNT(DISTINCT CASE 
            WHEN is_viewed_aip_tracking_card_order_delivered = true 
                AND is_aip_setup_complete = true 
            THEN app_package_deployed_uuid 
        END), 
        0
    ) AS onboarded,
    
    -- ThoughtSpot Formula: "Support Cases"
    NULLIF(
        COUNT(DISTINCT CASE 
            WHEN is_viewed_aip_tracking_card_order_delivered = true 
                AND is_oobe_support_session = true 
            THEN app_package_deployed_uuid 
        END), 
        0
    ) AS support_cases,
    
    -- ThoughtSpot Formula: "Order Confirmed - Pill"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END), 
        0
    ) AS order_confirmed_pill,
    
    -- ThoughtSpot Formula: "Processing - Pill"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END), 
        0
    ) AS processing_pill,
    
    -- ThoughtSpot Formula: "Shipped - Pill"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END), 
        0
    ) AS shipped_pill,
    
    -- ThoughtSpot Formula: "Delivered - Pill"
    NULLIF(
        COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END), 
        0
    ) AS delivered_pill,
    
    -- Aggregated count measures
    SUM(total_printer_count) AS sum_total_printer_count,
    SUM(total_device_count) AS sum_total_device_count,
    SUM(total_accessory_count) AS sum_total_accessory_count,
    SUM(total_pc_count) AS sum_total_pc_count,
    SUM(max_total_printer_count) AS sum_max_total_printer_count,
    SUM(max_total_device_count) AS sum_max_total_device_count,
    SUM(max_total_accessory_count) AS sum_max_total_accessory_count,
    SUM(max_total_pc_count) AS sum_max_total_pc_count,
    
    -- Session and app metrics
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_app_packages,
    COUNT(DISTINCT device_app_package_deployed_uuid) AS unique_device_packages
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_base
GROUP BY 
    month_date,
    session_date,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in;

-- =====================================================
-- Dashboard-Ready View for Lakeview Integration
-- Maps to ThoughtSpot Liveboard visualizations
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard AS
SELECT 
    -- Temporal dimensions for filtering and grouping
    month_date,
    session_date,
    YEAR(month_date) AS year,
    MONTH(month_date) AS month,
    
    -- Categorical dimensions
    os_platform,
    app_version,
    geo_country_code,
    CASE 
        WHEN is_hpid_signed_in = true THEN 'Signed In' 
        ELSE 'Not Signed In' 
    END AS hpid_status,
    
    -- Core engagement metrics (Viz_1: PaaS Tracking Card Views)
    viewed_paas_tracking_card AS paas_card_views,
    
    -- Expansion metrics (Viz_2: Clicks on Expand)
    clicked_expand AS expand_clicks,
    
    -- Status interaction metrics (Viz_3: Clicks on Each Status)
    clicked_order_confirmation AS confirmation_clicks,
    clicked_order_processing AS processing_clicks,
    clicked_track_delivery AS delivery_clicks,
    clicked_complete_setup AS setup_clicks,
    
    -- Delivery funnel metrics (Viz_4: Delivered vs Onboarded and Support)
    delivered AS delivered_count,
    onboarded AS onboarded_count,
    support_cases AS support_case_count,
    
    -- Pill interaction metrics (Viz_5: Tracking Card Status Pill Clicks)
    order_confirmed_pill AS confirmed_pill_clicks,
    processing_pill AS processing_pill_clicks,
    shipped_pill AS shipped_pill_clicks,
    delivered_pill AS delivered_pill_clicks,
    
    -- Status tracking metrics (Viz_6: Tracking Card Status)
    confirmed AS confirmed_count,
    processed AS processed_count,
    shipped AS shipped_count,
    
    -- Device and hardware metrics
    sum_total_printer_count AS total_printers,
    sum_total_device_count AS total_devices,
    sum_total_accessory_count AS total_accessories,
    sum_total_pc_count AS total_pcs,
    
    -- Session and engagement metrics
    unique_sessions,
    unique_app_packages,
    unique_device_packages,
    
    -- Calculated rates and ratios
    CASE 
        WHEN viewed_paas_tracking_card > 0 
        THEN ROUND((clicked_expand * 100.0) / viewed_paas_tracking_card, 2)
        ELSE NULL 
    END AS expand_rate_percent,
    
    CASE 
        WHEN delivered > 0 
        THEN ROUND((onboarded * 100.0) / delivered, 2)
        ELSE NULL 
    END AS onboarding_rate_percent,
    
    CASE 
        WHEN delivered > 0 
        THEN ROUND((support_cases * 100.0) / delivered, 2)
        ELSE NULL 
    END AS support_rate_percent
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_formulas
WHERE month_date IS NOT NULL
ORDER BY month_date DESC, os_platform;

-- =====================================================
-- Summary Statistics View
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_summary AS
SELECT 
    'Overall' AS metric_category,
    COUNT(DISTINCT month_date) AS months_of_data,
    COUNT(DISTINCT os_platform) AS platforms_count,
    SUM(unique_sessions) AS total_sessions,
    SUM(unique_app_packages) AS total_app_packages,
    SUM(paas_card_views) AS total_card_views,
    SUM(expand_clicks) AS total_expand_clicks,
    SUM(delivered_count) AS total_delivered,
    SUM(onboarded_count) AS total_onboarded,
    SUM(support_case_count) AS total_support_cases,
    
    -- Overall conversion rates
    CASE 
        WHEN SUM(paas_card_views) > 0 
        THEN ROUND((SUM(expand_clicks) * 100.0) / SUM(paas_card_views), 2)
        ELSE NULL 
    END AS overall_expand_rate_percent,
    
    CASE 
        WHEN SUM(delivered_count) > 0 
        THEN ROUND((SUM(onboarded_count) * 100.0) / SUM(delivered_count), 2)
        ELSE NULL 
    END AS overall_onboarding_rate_percent
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views
-- 
-- Views Created:
-- 1. paas_tracking_card_base - Base data with all dimensions
-- 2. paas_tracking_card_formulas - ThoughtSpot calculated measures
-- 3. paas_tracking_card_dashboard - Dashboard-ready metrics
-- 4. paas_tracking_card_summary - High-level summary statistics
--
-- These views provide complete semantic layer compatibility
-- with the original ThoughtSpot model and support all
-- visualizations defined in the liveboard TML.
-- =====================================================