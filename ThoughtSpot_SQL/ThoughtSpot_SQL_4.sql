-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL
-- Version 4: Final Production-Ready Version
-- =====================================================

-- Base Table View with all columns from paas_tracking_card table
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_base AS
SELECT 
    session_id,
    app_package_deployed_uuid,
    os_platform,
    app_name,
    app_package_id,
    app_version,
    session_start_date_time,
    geo_country_code,
    is_hpid_signed_in,
    total_printer_count,
    total_device_count,
    total_accessory_count,
    total_pc_count,
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
    device_app_package_deployed_uuid,
    max_total_printer_count,
    max_total_device_count,
    max_total_accessory_count,
    max_total_pc_count,
    associated_device_session_id,
    aip_device_uuid,
    is_associated_device,
    is_clicked_aip_order_accordian_order_processing,
    is_clicked_aip_order_accord,
    is_aip_setup_start,
    is_clicked_aip_order_accordian_order_confirmed,
    is_ows_start,
    is_clicked_aip_order_accordian_order_shipped,
    is_oobe_complete,
    is_aip_setup_complete,
    is_clicked_support,
    is_oobe_support_session,
    DATE(session_start_date_time) AS session_date
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Main Metric View: PaaS Tracking Card Metrics with all ThoughtSpot formulas
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS month_date,
    DATE(session_start_date_time) AS session_date,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    app_name,
    app_package_id,
    
    -- ThoughtSpot Formula: Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) 
    END AS viewed_paas_tracking_card,
    
    -- ThoughtSpot Formula: Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) 
    END AS clicked_expand,
    
    -- ThoughtSpot Formula: Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) 
    END AS clicked_order_confirmation,
    
    -- ThoughtSpot Formula: Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) 
    END AS clicked_order_processing,
    
    -- ThoughtSpot Formula: Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) 
    END AS clicked_track_delivery,
    
    -- ThoughtSpot Formula: Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) 
    END AS clicked_complete_setup,
    
    -- ThoughtSpot Formula: Confirmed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) 
    END AS confirmed,
    
    -- ThoughtSpot Formula: Delivered (uses device_app_package_deployed_uuid)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN COALESCE(device_app_package_deployed_uuid, app_package_deployed_uuid) END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN COALESCE(device_app_package_deployed_uuid, app_package_deployed_uuid) END) 
    END AS delivered,
    
    -- ThoughtSpot Formula: Processed (uses device_app_package_deployed_uuid)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN COALESCE(device_app_package_deployed_uuid, app_package_deployed_uuid) END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN COALESCE(device_app_package_deployed_uuid, app_package_deployed_uuid) END) 
    END AS processed,
    
    -- ThoughtSpot Formula: Shipped
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) 
    END AS shipped,
    
    -- ThoughtSpot Formula: Onboarded (delivered AND setup complete)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) 
    END AS onboarded,
    
    -- ThoughtSpot Formula: Support Cases (delivered AND support session)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) 
    END AS support_cases,
    
    -- ThoughtSpot Formula: Order Confirmed - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) 
    END AS order_confirmed_pill,
    
    -- ThoughtSpot Formula: Processing - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) 
    END AS processing_pill,
    
    -- ThoughtSpot Formula: Shipped - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) 
    END AS shipped_pill,
    
    -- ThoughtSpot Formula: Delivered - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) 
    END AS delivered_pill,
    
    -- Aggregated Measures (SUM aggregation as defined in TML)
    SUM(total_printer_count) AS total_printer_count_sum,
    SUM(total_device_count) AS total_device_count_sum,
    SUM(total_accessory_count) AS total_accessory_count_sum,
    SUM(total_pc_count) AS total_pc_count_sum,
    SUM(max_total_printer_count) AS max_total_printer_count_sum,
    SUM(max_total_device_count) AS max_total_device_count_sum,
    SUM(max_total_accessory_count) AS max_total_accessory_count_sum,
    SUM(max_total_pc_count) AS max_total_pc_count_sum
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    DATE(session_start_date_time),
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    app_name,
    app_package_id;

-- Dashboard View 1: PaaS Tracking Card Views (Viz_1 from liveboard)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_views_dashboard AS
SELECT 
    month_date,
    os_platform,
    viewed_paas_tracking_card
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
WHERE viewed_paas_tracking_card IS NOT NULL
ORDER BY month_date, os_platform;

-- Dashboard View 2: Clicks on Expand (Viz_2 from liveboard)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_expand_clicks_dashboard AS
SELECT 
    month_date,
    os_platform,
    clicked_expand
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
WHERE clicked_expand IS NOT NULL
ORDER BY month_date, os_platform;

-- Dashboard View 3: Clicks on Each Status (Viz_3 from liveboard)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_status_clicks_dashboard AS
SELECT 
    month_date,
    clicked_order_confirmation,
    clicked_order_processing,
    clicked_track_delivery,
    clicked_complete_setup
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
WHERE clicked_order_confirmation IS NOT NULL 
   OR clicked_order_processing IS NOT NULL 
   OR clicked_track_delivery IS NOT NULL 
   OR clicked_complete_setup IS NOT NULL
ORDER BY month_date;

-- Dashboard View 4: Delivered vs Onboarded and Support Cases (Viz_4 from liveboard)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_delivery_onboarding_dashboard AS
SELECT 
    month_date,
    delivered,
    onboarded,
    support_cases
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
WHERE delivered IS NOT NULL 
   OR onboarded IS NOT NULL 
   OR support_cases IS NOT NULL
ORDER BY month_date;

-- Dashboard View 5: Tracking Card Status Pill Clicks (Viz_5 from liveboard)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_pill_clicks_dashboard AS
SELECT 
    month_date,
    order_confirmed_pill,
    processing_pill,
    shipped_pill,
    delivered_pill
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
WHERE order_confirmed_pill IS NOT NULL 
   OR processing_pill IS NOT NULL 
   OR shipped_pill IS NOT NULL 
   OR delivered_pill IS NOT NULL
ORDER BY month_date;

-- Dashboard View 6: Tracking Card Status (Viz_6 from liveboard)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_status_dashboard AS
SELECT 
    month_date,
    confirmed,
    processed,
    shipped,
    delivered
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
WHERE confirmed IS NOT NULL 
   OR processed IS NOT NULL 
   OR shipped IS NOT NULL 
   OR delivered IS NOT NULL
ORDER BY month_date;

-- Unified Dashboard View: All metrics in one view for comprehensive analysis
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_unified_dashboard AS
SELECT 
    month_date,
    session_date,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    app_name,
    app_package_id,
    viewed_paas_tracking_card,
    clicked_expand,
    clicked_order_confirmation,
    clicked_order_processing,
    clicked_track_delivery,
    clicked_complete_setup,
    confirmed,
    delivered,
    processed,
    shipped,
    onboarded,
    support_cases,
    order_confirmed_pill,
    processing_pill,
    shipped_pill,
    delivered_pill,
    total_printer_count_sum,
    total_device_count_sum,
    total_accessory_count_sum,
    total_pc_count_sum,
    max_total_printer_count_sum,
    max_total_device_count_sum,
    max_total_accessory_count_sum,
    max_total_pc_count_sum
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
ORDER BY month_date, session_date, os_platform;

-- =====================================================
-- SUMMARY: Created 9 metric views for PaaS Tracking Card Dashboard
-- 
-- Base Views:
-- 1. paas_tracking_card_base - Base table with all columns and derived date
-- 2. paas_tracking_card_metrics - Main aggregated metrics with all ThoughtSpot formulas
--
-- Dashboard-Specific Views (matching liveboard visualizations):
-- 3. paas_tracking_card_views_dashboard - PaaS Tracking Card Views by Month/OS
-- 4. paas_tracking_card_expand_clicks_dashboard - Expand clicks by Month/OS
-- 5. paas_tracking_card_status_clicks_dashboard - Status clicks by Month
-- 6. paas_tracking_card_delivery_onboarding_dashboard - Delivery/Onboarding metrics
-- 7. paas_tracking_card_pill_clicks_dashboard - Status pill clicks
-- 8. paas_tracking_card_status_dashboard - Tracking card status metrics
-- 9. paas_tracking_card_unified_dashboard - All metrics in one comprehensive view
--
-- All views use:
-- - Fully qualified Databricks names (team_css_analytics_prod.hpx_analytics.*)
-- - ThoughtSpot formula logic with CASE/COUNT DISTINCT patterns
-- - Proper GROUP BY clauses for aggregations
-- - NULL handling as per ThoughtSpot model definitions
-- - Environment-agnostic SQL compatible with Databricks
-- =====================================================