-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 4.0 - Complete Implementation
-- Source: team_css_analytics_prod.hpx_analytics.paas_tracking_card
-- =====================================================

-- =====================================================
-- Base Metric View - Direct mapping from table.tml
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_base_metrics AS
SELECT 
    -- Primary Keys and Identifiers
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    associated_device_session_id,
    aip_device_uuid,
    
    -- Application Attributes
    os_platform,
    app_name,
    app_package_id,
    app_version,
    
    -- Temporal Attributes
    session_start_date_time,
    DATE(session_start_date_time) AS session_date,
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    DATE_TRUNC('YEAR', session_start_date_time) AS year_date,
    
    -- Geographic and User Attributes
    geo_country_code,
    is_hpid_signed_in,
    is_associated_device,
    
    -- Tracking Card View Flags
    is_viewed_aip_tracking_card,
    is_viewed_aip_tracking_card_order_confirmed,
    is_viewed_aip_tracking_card_order_processing,
    is_viewed_aip_tracking_card_order_shipped,
    is_viewed_aip_tracking_card_order_delivered,
    
    -- Click Action Flags
    is_clicked_aip_order_accordian,
    is_clicked_order_confirmation,
    is_clicked_order_processing,
    is_clicked_track_delivery,
    is_clicked_complete_setup,
    
    -- Pill Click Flags
    is_clicked_order_confirmation_pill,
    is_clicked_order_processing_pill,
    is_clicked_order_shipped_pill,
    is_clicked_order_delivered_pill,
    
    -- Additional Click Flags
    is_clicked_aip_order_accordian_order_confirmed,
    is_clicked_aip_order_accordian_order_processing,
    is_clicked_aip_order_accordian_order_shipped,
    is_clicked_aip_order_accord,
    is_clicked_support,
    
    -- Setup and Process Flags
    is_aip_setup_start,
    is_aip_setup_complete,
    is_ows_start,
    is_oobe_complete,
    is_oobe_support_session,
    
    -- Device Count Measures
    COALESCE(total_printer_count, 0) AS total_printer_count,
    COALESCE(total_device_count, 0) AS total_device_count,
    COALESCE(total_accessory_count, 0) AS total_accessory_count,
    COALESCE(total_pc_count, 0) AS total_pc_count,
    COALESCE(max_total_printer_count, 0) AS max_total_printer_count,
    COALESCE(max_total_device_count, 0) AS max_total_device_count,
    COALESCE(max_total_accessory_count, 0) AS max_total_accessory_count,
    COALESCE(max_total_pc_count, 0) AS max_total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- Calculated Measures View - ThoughtSpot model.tml formulas
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures AS
SELECT 
    month_date,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    
    -- ThoughtSpot Formula: "Viewed PaaS Tracking Card"
    -- if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) 
    END AS viewed_paas_tracking_card,
    
    -- ThoughtSpot Formula: "Clicked Expand"
    -- if ( unique_count_if ( [paas_tracking_card::is_clicked_aip_order_accordian] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_clicked_aip_order_accordian] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) 
    END AS clicked_expand,
    
    -- ThoughtSpot Formula: "Clicked Order Confirmation"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) 
    END AS clicked_order_confirmation,
    
    -- ThoughtSpot Formula: "Clicked Order Processing"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) 
    END AS clicked_order_processing,
    
    -- ThoughtSpot Formula: "Clicked Track Delivery"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) 
    END AS clicked_track_delivery,
    
    -- ThoughtSpot Formula: "Clicked Complete Setup"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) 
    END AS clicked_complete_setup,
    
    -- ThoughtSpot Formula: "Confirmed"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) 
    END AS confirmed,
    
    -- ThoughtSpot Formula: "Processed"
    -- Uses device_app_package_deployed_uuid instead of app_package_deployed_uuid
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) 
    END AS processed,
    
    -- ThoughtSpot Formula: "Shipped"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) 
    END AS shipped,
    
    -- ThoughtSpot Formula: "Delivered"
    -- Uses device_app_package_deployed_uuid instead of app_package_deployed_uuid
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) 
    END AS delivered,
    
    -- ThoughtSpot Formula: "Onboarded"
    -- if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] and [paas_tracking_card::is_aip_setup_complete] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] and [paas_tracking_card::is_aip_setup_complete] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN (is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true) THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN (is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true) THEN app_package_deployed_uuid END) 
    END AS onboarded,
    
    -- ThoughtSpot Formula: "Support Cases"
    -- if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] and [paas_tracking_card::is_oobe_support_session] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] and [paas_tracking_card::is_oobe_support_session] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN (is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true) THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN (is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true) THEN app_package_deployed_uuid END) 
    END AS support_cases,
    
    -- ThoughtSpot Formula: "Order Confirmed - Pill"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) 
    END AS order_confirmed_pill,
    
    -- ThoughtSpot Formula: "Processing - Pill"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) 
    END AS processing_pill,
    
    -- ThoughtSpot Formula: "Shipped - Pill"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) 
    END AS shipped_pill,
    
    -- ThoughtSpot Formula: "Delivered - Pill"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) 
    END AS delivered_pill,
    
    -- Aggregated Device Counts
    SUM(total_printer_count) AS sum_total_printer_count,
    SUM(total_device_count) AS sum_total_device_count,
    SUM(total_accessory_count) AS sum_total_accessory_count,
    SUM(total_pc_count) AS sum_total_pc_count,
    SUM(max_total_printer_count) AS sum_max_total_printer_count,
    SUM(max_total_device_count) AS sum_max_total_device_count,
    SUM(max_total_accessory_count) AS sum_max_total_accessory_count,
    SUM(max_total_pc_count) AS sum_max_total_pc_count,
    
    -- Session and Package Counts
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_app_packages,
    COUNT(DISTINCT device_app_package_deployed_uuid) AS unique_device_packages
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_base_metrics
GROUP BY 
    month_date,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in;

-- =====================================================
-- Dashboard View - Lakeview Compatible
-- Maps to ThoughtSpot liveboard.tml visualizations
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard AS
SELECT 
    month_date,
    os_platform,
    app_version,
    geo_country_code,
    CASE WHEN is_hpid_signed_in = true THEN 'Signed In' ELSE 'Not Signed In' END AS hpid_status,
    
    -- Viz_1: PaaS Tracking Card Views - [Viewed PaaS Tracking Card] [Os Platform] [Date] [Date].monthly
    COALESCE(viewed_paas_tracking_card, 0) AS paas_tracking_card_views,
    
    -- Viz_2: Clicks on Expand - [Clicked Expand] [Os Platform] [Date] [Date].monthly
    COALESCE(clicked_expand, 0) AS expand_clicks,
    
    -- Viz_3: Clicks on Each Status - [Clicked Order Confirmation] [Clicked Order Processing] [Clicked Track Delivery] [Clicked Complete Setup]
    COALESCE(clicked_order_confirmation, 0) AS order_confirmation_clicks,
    COALESCE(clicked_order_processing, 0) AS order_processing_clicks,
    COALESCE(clicked_track_delivery, 0) AS track_delivery_clicks,
    COALESCE(clicked_complete_setup, 0) AS complete_setup_clicks,
    
    -- Viz_4: Delivered vs Onboarded and Support Cases - [Delivered] [Onboarded] [Support Cases]
    COALESCE(delivered, 0) AS delivered_count,
    COALESCE(onboarded, 0) AS onboarded_count,
    COALESCE(support_cases, 0) AS support_cases_count,
    
    -- Viz_5: Tracking Card Status Pill Clicks - [Order Confirmed - Pill] [Processing - Pill] [Shipped - Pill] [Delivered - Pill]
    COALESCE(order_confirmed_pill, 0) AS confirmed_pill_clicks,
    COALESCE(processing_pill, 0) AS processing_pill_clicks,
    COALESCE(shipped_pill, 0) AS shipped_pill_clicks,
    COALESCE(delivered_pill, 0) AS delivered_pill_clicks,
    
    -- Viz_6: Tracking Card Status - [Processed] [Shipped] [Confirmed] [Delivered]
    COALESCE(processed, 0) AS processed_count,
    COALESCE(shipped, 0) AS shipped_count,
    COALESCE(confirmed, 0) AS confirmed_count,
    
    -- Device Metrics
    COALESCE(sum_total_printer_count, 0) AS total_printers,
    COALESCE(sum_total_device_count, 0) AS total_devices,
    COALESCE(sum_total_accessory_count, 0) AS total_accessories,
    COALESCE(sum_total_pc_count, 0) AS total_pcs,
    
    -- Session Metrics
    COALESCE(unique_sessions, 0) AS session_count,
    COALESCE(unique_app_packages, 0) AS app_package_count,
    COALESCE(unique_device_packages, 0) AS device_package_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures
ORDER BY month_date DESC, os_platform;

-- =====================================================
-- Individual Visualization Views for Lakeview Dashboards
-- =====================================================

-- Viz_1: PaaS Tracking Card Views by OS Platform and Month
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_paas_card_views AS
SELECT 
    month_date,
    os_platform,
    paas_tracking_card_views AS viewed_paas_tracking_card
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
WHERE paas_tracking_card_views > 0;

-- Viz_2: Clicks on Expand by OS Platform and Month
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_expand_clicks AS
SELECT 
    month_date,
    os_platform,
    expand_clicks AS clicked_expand
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard
WHERE expand_clicks > 0;

-- Viz_3: Clicks on Each Status
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_status_clicks AS
SELECT 
    month_date,
    order_confirmation_clicks AS clicked_order_confirmation,
    order_processing_clicks AS clicked_order_processing,
    track_delivery_clicks AS clicked_track_delivery,
    complete_setup_clicks AS clicked_complete_setup
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard;

-- Viz_4: Delivered vs Onboarded and Support Cases
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_delivery_onboarding AS
SELECT 
    month_date,
    delivered_count AS delivered,
    onboarded_count AS onboarded,
    support_cases_count AS support_cases
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard;

-- Viz_5: Tracking Card Status Pill Clicks
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_pill_clicks AS
SELECT 
    month_date,
    confirmed_pill_clicks AS order_confirmed_pill,
    processing_pill_clicks AS processing_pill,
    shipped_pill_clicks AS shipped_pill,
    delivered_pill_clicks AS delivered_pill
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard;

-- Viz_6: Tracking Card Status
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.viz_tracking_status AS
SELECT 
    month_date,
    processed_count AS processed,
    shipped_count AS shipped,
    confirmed_count AS confirmed,
    delivered_count AS delivered
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard;

-- =====================================================
-- Summary Statistics View
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_summary AS
SELECT 
    'PaaS Tracking Card Analytics' AS dashboard_name,
    COUNT(DISTINCT month_date) AS total_months,
    COUNT(DISTINCT os_platform) AS total_platforms,
    SUM(paas_tracking_card_views) AS total_card_views,
    SUM(expand_clicks) AS total_expand_clicks,
    SUM(delivered_count) AS total_delivered,
    SUM(onboarded_count) AS total_onboarded,
    SUM(support_cases_count) AS total_support_cases,
    SUM(session_count) AS total_sessions,
    MAX(month_date) AS latest_data_date,
    MIN(month_date) AS earliest_data_date
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views - Version 4.0
-- Complete implementation ready for Databricks and Lakeview
-- =====================================================