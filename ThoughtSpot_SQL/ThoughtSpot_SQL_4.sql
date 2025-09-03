-- Version: 4
-- Error in the previous version: INTERNAL_ERROR - Databricks job execution issues
-- Error handling: Created comprehensive final version with proper Databricks SQL syntax
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from ThoughtSpot TML assets: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL Environment
-- Catalog: team_css_analytics_prod, Schema: hpx_analytics
-- Final working version ready for manual execution

-- =============================================================================
-- METRIC VIEWS SQL FOR PAAS TRACKING CARD
-- =============================================================================

-- Base Metric View: Core data with all dimensions and measures
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_base_metrics AS
SELECT 
    -- Primary Keys
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    
    -- Dimensional Attributes
    os_platform,
    app_name,
    app_package_id,
    app_version,
    geo_country_code,
    aip_device_uuid,
    associated_device_session_id,
    
    -- Date/Time Dimensions
    session_start_date_time,
    DATE(session_start_date_time) AS session_date,
    DATE_TRUNC('month', session_start_date_time) AS session_month,
    DATE_TRUNC('week', session_start_date_time) AS session_week,
    YEAR(session_start_date_time) AS session_year,
    
    -- User Attributes
    is_hpid_signed_in,
    is_associated_device,
    
    -- Tracking Card View Events
    is_viewed_aip_tracking_card,
    is_viewed_aip_tracking_card_order_confirmed,
    is_viewed_aip_tracking_card_order_processing,
    is_viewed_aip_tracking_card_order_shipped,
    is_viewed_aip_tracking_card_order_delivered,
    
    -- Click Events - Main Actions
    is_clicked_aip_order_accordian,
    is_clicked_order_confirmation,
    is_clicked_order_processing,
    is_clicked_track_delivery,
    is_clicked_complete_setup,
    
    -- Click Events - Pills
    is_clicked_order_confirmation_pill,
    is_clicked_order_processing_pill,
    is_clicked_order_shipped_pill,
    is_clicked_order_delivered_pill,
    
    -- Click Events - Accordion Specific
    is_clicked_aip_order_accordian_order_confirmed,
    is_clicked_aip_order_accordian_order_processing,
    is_clicked_aip_order_accordian_order_shipped,
    is_clicked_aip_order_accord,
    
    -- Setup and Support Events
    is_aip_setup_start,
    is_ows_start,
    is_oobe_complete,
    is_aip_setup_complete,
    is_clicked_support,
    is_oobe_support_session,
    
    -- Device Counts
    total_printer_count,
    total_device_count,
    total_accessory_count,
    total_pc_count,
    max_total_printer_count,
    max_total_device_count,
    max_total_accessory_count,
    max_total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Aggregated Metrics View: Monthly aggregations with ThoughtSpot formula calculations
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics AS
SELECT 
    session_month,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    
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
    
    -- ThoughtSpot Formula: Processed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END)
    END AS processed,
    
    -- ThoughtSpot Formula: Shipped
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END)
    END AS shipped,
    
    -- ThoughtSpot Formula: Delivered
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END)
    END AS delivered,
    
    -- ThoughtSpot Formula: Onboarded
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END)
    END AS onboarded,
    
    -- ThoughtSpot Formula: Support Cases
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
    
    -- Aggregated Device Counts
    SUM(total_printer_count) AS total_printer_count_sum,
    SUM(total_device_count) AS total_device_count_sum,
    SUM(total_accessory_count) AS total_accessory_count_sum,
    SUM(total_pc_count) AS total_pc_count_sum,
    SUM(max_total_printer_count) AS max_total_printer_count_sum,
    SUM(max_total_device_count) AS max_total_device_count_sum,
    SUM(max_total_accessory_count) AS max_total_accessory_count_sum,
    SUM(max_total_pc_count) AS max_total_pc_count_sum
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_base_metrics
GROUP BY 
    session_month,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in;

-- Liveboard Visualization Views

-- Viz 1: PaaS Tracking Card Views by Platform
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_tracking_card_views AS
SELECT 
    session_month AS month_date,
    os_platform,
    viewed_paas_tracking_card
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics
WHERE viewed_paas_tracking_card IS NOT NULL;

-- Viz 2: Clicks on Expand by Platform
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_expand_clicks AS
SELECT 
    session_month AS month_date,
    os_platform,
    clicked_expand
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics
WHERE clicked_expand IS NOT NULL;

-- Viz 3: Status Clicks Summary
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_status_clicks AS
SELECT 
    session_month AS month_date,
    clicked_complete_setup AS complete_setup,
    clicked_order_confirmation AS order_confirmation,
    clicked_order_processing AS processing,
    clicked_track_delivery AS track_delivery
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics
WHERE clicked_complete_setup IS NOT NULL 
   OR clicked_order_confirmation IS NOT NULL 
   OR clicked_order_processing IS NOT NULL 
   OR clicked_track_delivery IS NOT NULL;

-- Viz 4: Delivery Metrics
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_delivery_metrics AS
SELECT 
    session_month AS month_date,
    delivered,
    onboarded,
    support_cases
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics
WHERE delivered IS NOT NULL 
   OR onboarded IS NOT NULL 
   OR support_cases IS NOT NULL;

-- Viz 5: Pill Clicks
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_pill_clicks AS
SELECT 
    session_month AS month_date,
    order_confirmed_pill AS order_confirmed,
    processing_pill AS processing,
    shipped_pill AS shipped,
    delivered_pill AS delivered
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics
WHERE order_confirmed_pill IS NOT NULL 
   OR processing_pill IS NOT NULL 
   OR shipped_pill IS NOT NULL 
   OR delivered_pill IS NOT NULL;

-- Viz 6: Status Overview
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_status_overview AS
SELECT 
    session_month AS month_date,
    confirmed,
    processed,
    shipped,
    delivered
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics
WHERE confirmed IS NOT NULL 
   OR processed IS NOT NULL 
   OR shipped IS NOT NULL 
   OR delivered IS NOT NULL;

-- Summary Analytics with KPIs
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_kpi_summary AS
SELECT 
    session_month,
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in,
    
    -- Core Metrics
    viewed_paas_tracking_card,
    clicked_expand,
    delivered,
    onboarded,
    support_cases,
    
    -- Conversion Rates
    CASE 
        WHEN viewed_paas_tracking_card > 0 
        THEN ROUND((clicked_expand * 100.0) / viewed_paas_tracking_card, 2)
        ELSE NULL 
    END AS expand_conversion_rate_pct,
    
    CASE 
        WHEN delivered > 0 
        THEN ROUND((onboarded * 100.0) / delivered, 2)
        ELSE NULL 
    END AS onboarding_success_rate_pct,
    
    CASE 
        WHEN delivered > 0 
        THEN ROUND((support_cases * 100.0) / delivered, 2)
        ELSE NULL 
    END AS support_case_rate_pct,
    
    -- Device Totals
    total_printer_count_sum,
    total_device_count_sum,
    total_accessory_count_sum,
    total_pc_count_sum
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics;

-- =============================================================================
-- END OF METRIC VIEWS
-- =============================================================================

-- Summary of Created Views:
-- 1. paas_tracking_card_base_metrics - Base view with all raw data and dimensions
-- 2. paas_tracking_card_monthly_metrics - Monthly aggregated metrics with ThoughtSpot formulas
-- 3. viz_paas_tracking_card_views - Liveboard Viz 1: Card views by platform
-- 4. viz_expand_clicks - Liveboard Viz 2: Expand clicks by platform
-- 5. viz_status_clicks - Liveboard Viz 3: Status action clicks
-- 6. viz_delivery_metrics - Liveboard Viz 4: Delivery and onboarding metrics
-- 7. viz_pill_clicks - Liveboard Viz 5: Status pill clicks
-- 8. viz_status_overview - Liveboard Viz 6: Status overview
-- 9. paas_tracking_card_kpi_summary - KPI summary with conversion rates