-- =====================================================
-- THOUGHTSPOT SQL METRIC VIEWS FOR DATABRICKS
-- Generated from PaaS Tracking Card TML Assets
-- Version 4 - Final Complete Implementation
-- =====================================================

-- =====================================================
-- MAIN METRIC VIEW: PaaS Tracking Card Base Analytics
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Primary Keys and Identifiers
    session_id AS `session_id`,
    app_package_deployed_uuid AS `app_package_deployed_uuid`,
    device_app_package_deployed_uuid AS `device_app_package_deployed_uuid`,
    aip_device_uuid AS `aip_device_uuid`,
    associated_device_session_id AS `associated_device_session_id`,
    
    -- Application and Platform Dimensions
    os_platform AS `os_platform`,
    app_name AS `app_name`,
    app_package_id AS `app_package_id`,
    app_version AS `app_version`,
    geo_country_code AS `geo_country_code`,
    
    -- Date and Time Dimensions
    session_start_date_time AS `session_start_date_time`,
    DATE(session_start_date_time) AS `date_field`,
    DATE_TRUNC('month', session_start_date_time) AS `month_field`,
    DATE_TRUNC('quarter', session_start_date_time) AS `quarter_field`,
    DATE_TRUNC('year', session_start_date_time) AS `year_field`,
    
    -- User and Device Status Flags
    is_hpid_signed_in AS `is_hpid_signed_in`,
    is_associated_device AS `is_associated_device`,
    
    -- Tracking Card View Events
    is_viewed_aip_tracking_card AS `is_viewed_aip_tracking_card`,
    is_viewed_aip_tracking_card_order_confirmed AS `is_viewed_aip_tracking_card_order_confirmed`,
    is_viewed_aip_tracking_card_order_processing AS `is_viewed_aip_tracking_card_order_processing`,
    is_viewed_aip_tracking_card_order_shipped AS `is_viewed_aip_tracking_card_order_shipped`,
    is_viewed_aip_tracking_card_order_delivered AS `is_viewed_aip_tracking_card_order_delivered`,
    
    -- Click Events - Main Actions
    is_clicked_aip_order_accordian AS `is_clicked_aip_order_accordian`,
    is_clicked_order_confirmation AS `is_clicked_order_confirmation`,
    is_clicked_order_processing AS `is_clicked_order_processing`,
    is_clicked_track_delivery AS `is_clicked_track_delivery`,
    is_clicked_complete_setup AS `is_clicked_complete_setup`,
    
    -- Click Events - Status Pills
    is_clicked_order_confirmation_pill AS `is_clicked_order_confirmation_pill`,
    is_clicked_order_processing_pill AS `is_clicked_order_processing_pill`,
    is_clicked_order_shipped_pill AS `is_clicked_order_shipped_pill`,
    is_clicked_order_delivered_pill AS `is_clicked_order_delivered_pill`,
    
    -- Click Events - Detailed Accordion Actions
    is_clicked_aip_order_accordian_order_confirmed AS `is_clicked_aip_order_accordian_order_confirmed`,
    is_clicked_aip_order_accordian_order_processing AS `is_clicked_aip_order_accordian_order_processing`,
    is_clicked_aip_order_accordian_order_shipped AS `is_clicked_aip_order_accordian_order_shipped`,
    is_clicked_aip_order_accord AS `is_clicked_aip_order_accord`,
    
    -- Setup and Onboarding Events
    is_aip_setup_start AS `is_aip_setup_start`,
    is_aip_setup_complete AS `is_aip_setup_complete`,
    is_ows_start AS `is_ows_start`,
    is_oobe_complete AS `is_oobe_complete`,
    
    -- Support Events
    is_clicked_support AS `is_clicked_support`,
    is_oobe_support_session AS `is_oobe_support_session`,
    
    -- Device Count Measures
    total_printer_count AS `total_printer_count`,
    total_device_count AS `total_device_count`,
    total_accessory_count AS `total_accessory_count`,
    total_pc_count AS `total_pc_count`,
    max_total_printer_count AS `max_total_printer_count`,
    max_total_device_count AS `max_total_device_count`,
    max_total_accessory_count AS `max_total_accessory_count`,
    max_total_pc_count AS `max_total_pc_count`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- THOUGHTSPOT FORMULA CONVERSIONS: Calculated Measures
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_field`,
    os_platform AS `os_platform`,
    geo_country_code AS `geo_country_code`,
    
    -- ThoughtSpot Formula: "Viewed PaaS Tracking Card"
    -- Original: if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END)
    END AS `viewed_paas_tracking_card`,
    
    -- ThoughtSpot Formula: "Clicked Expand"
    -- Original: if ( unique_count_if ( [paas_tracking_card::is_clicked_aip_order_accordian] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_clicked_aip_order_accordian] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END)
    END AS `clicked_expand`,
    
    -- ThoughtSpot Formula: "Clicked Order Confirmation"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END)
    END AS `clicked_order_confirmation`,
    
    -- ThoughtSpot Formula: "Clicked Order Processing"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END)
    END AS `clicked_order_processing`,
    
    -- ThoughtSpot Formula: "Clicked Track Delivery"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END)
    END AS `clicked_track_delivery`,
    
    -- ThoughtSpot Formula: "Clicked Complete Setup"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END)
    END AS `clicked_complete_setup`,
    
    -- ThoughtSpot Formula: "Confirmed"
    -- Original: if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_confirmed] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_confirmed] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END)
    END AS `confirmed`,
    
    -- ThoughtSpot Formula: "Delivered"
    -- Original: if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] , [paas_tracking_card::device_app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] , [paas_tracking_card::device_app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END)
    END AS `delivered`,
    
    -- ThoughtSpot Formula: "Processed"
    -- Original: if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_processing] , [paas_tracking_card::device_app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_processing] , [paas_tracking_card::device_app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END)
    END AS `processed`,
    
    -- ThoughtSpot Formula: "Shipped"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END)
    END AS `shipped`,
    
    -- ThoughtSpot Formula: "Onboarded"
    -- Original: if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] and [paas_tracking_card::is_aip_setup_complete] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] and [paas_tracking_card::is_aip_setup_complete] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END)
    END AS `onboarded`,
    
    -- ThoughtSpot Formula: "Support Cases"
    -- Original: if ( unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] and [paas_tracking_card::is_oobe_support_session] , [paas_tracking_card::app_package_deployed_uuid] ) = 0 ) then null else unique_count_if ( [paas_tracking_card::is_viewed_aip_tracking_card_order_delivered] and [paas_tracking_card::is_oobe_support_session] , [paas_tracking_card::app_package_deployed_uuid] )
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END)
    END AS `support_cases`,
    
    -- ThoughtSpot Formula: "Order Confirmed - Pill"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END)
    END AS `order_confirmed_pill`,
    
    -- ThoughtSpot Formula: "Processing - Pill"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END)
    END AS `processing_pill`,
    
    -- ThoughtSpot Formula: "Shipped - Pill"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END)
    END AS `shipped_pill`,
    
    -- ThoughtSpot Formula: "Delivered - Pill"
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END)
    END AS `delivered_pill`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform,
    geo_country_code;

-- =====================================================
-- LIVEBOARD DASHBOARD SUMMARY: Aggregated Metrics
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_summary AS
SELECT 
    month_field AS `month`,
    os_platform AS `os_platform`,
    geo_country_code AS `geo_country_code`,
    
    -- Primary Engagement Metrics (from Viz_1 and Viz_2)
    SUM(viewed_paas_tracking_card) AS `total_viewed_paas_tracking_card`,
    SUM(clicked_expand) AS `total_clicked_expand`,
    
    -- Status Action Clicks (from Viz_3)
    SUM(clicked_order_confirmation) AS `total_clicked_order_confirmation`,
    SUM(clicked_order_processing) AS `total_clicked_order_processing`,
    SUM(clicked_track_delivery) AS `total_clicked_track_delivery`,
    SUM(clicked_complete_setup) AS `total_clicked_complete_setup`,
    
    -- Order Status Progression (from Viz_6)
    SUM(confirmed) AS `total_confirmed`,
    SUM(processed) AS `total_processed`,
    SUM(shipped) AS `total_shipped`,
    SUM(delivered) AS `total_delivered`,
    
    -- Onboarding and Support (from Viz_4)
    SUM(onboarded) AS `total_onboarded`,
    SUM(support_cases) AS `total_support_cases`,
    
    -- Status Pill Interactions (from Viz_5)
    SUM(order_confirmed_pill) AS `total_order_confirmed_pill`,
    SUM(processing_pill) AS `total_processing_pill`,
    SUM(shipped_pill) AS `total_shipped_pill`,
    SUM(delivered_pill) AS `total_delivered_pill`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures
GROUP BY month_field, os_platform, geo_country_code
ORDER BY month_field DESC, os_platform;

-- =====================================================
-- DEVICE AND SESSION ANALYTICS
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_device_metrics AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_field`,
    os_platform AS `os_platform`,
    geo_country_code AS `geo_country_code`,
    is_hpid_signed_in AS `is_hpid_signed_in`,
    
    -- Device Count Aggregations (SUM aggregation as per TML)
    SUM(total_printer_count) AS `total_printer_count_sum`,
    SUM(total_device_count) AS `total_device_count_sum`,
    SUM(total_accessory_count) AS `total_accessory_count_sum`,
    SUM(total_pc_count) AS `total_pc_count_sum`,
    SUM(max_total_printer_count) AS `max_total_printer_count_sum`,
    SUM(max_total_device_count) AS `max_total_device_count_sum`,
    SUM(max_total_accessory_count) AS `max_total_accessory_count_sum`,
    SUM(max_total_pc_count) AS `max_total_pc_count_sum`,
    
    -- Session and Package Analytics
    COUNT(DISTINCT session_id) AS `unique_sessions`,
    COUNT(DISTINCT app_package_deployed_uuid) AS `unique_app_packages`,
    COUNT(DISTINCT device_app_package_deployed_uuid) AS `unique_device_packages`,
    COUNT(DISTINCT aip_device_uuid) AS `unique_aip_devices`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform,
    geo_country_code,
    is_hpid_signed_in
ORDER BY month_field DESC, os_platform;

-- =====================================================
-- FINAL COMPREHENSIVE METRIC VIEW FOR LAKEVIEW DASHBOARDS
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_final_metrics AS
SELECT 
    -- Time Dimensions
    m.month_field AS `month`,
    DATE_TRUNC('quarter', m.month_field) AS `quarter`,
    DATE_TRUNC('year', m.month_field) AS `year`,
    
    -- Platform and Geography
    m.os_platform AS `os_platform`,
    m.geo_country_code AS `geo_country_code`,
    
    -- Core Engagement Metrics
    m.viewed_paas_tracking_card AS `viewed_paas_tracking_card`,
    m.clicked_expand AS `clicked_expand`,
    
    -- Action Metrics
    m.clicked_order_confirmation AS `clicked_order_confirmation`,
    m.clicked_order_processing AS `clicked_order_processing`,
    m.clicked_track_delivery AS `clicked_track_delivery`,
    m.clicked_complete_setup AS `clicked_complete_setup`,
    
    -- Status Progression
    m.confirmed AS `confirmed`,
    m.processed AS `processed`,
    m.shipped AS `shipped`,
    m.delivered AS `delivered`,
    m.onboarded AS `onboarded`,
    m.support_cases AS `support_cases`,
    
    -- Pill Interactions
    m.order_confirmed_pill AS `order_confirmed_pill`,
    m.processing_pill AS `processing_pill`,
    m.shipped_pill AS `shipped_pill`,
    m.delivered_pill AS `delivered_pill`,
    
    -- Device Metrics
    d.total_printer_count_sum AS `total_printer_count`,
    d.total_device_count_sum AS `total_device_count`,
    d.total_accessory_count_sum AS `total_accessory_count`,
    d.total_pc_count_sum AS `total_pc_count`,
    d.unique_sessions AS `unique_sessions`,
    d.unique_app_packages AS `unique_app_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures m
LEFT JOIN team_css_analytics_prod.hpx_analytics.paas_tracking_card_device_metrics d
    ON m.month_field = d.month_field 
    AND m.os_platform = d.os_platform 
    AND m.geo_country_code = d.geo_country_code
ORDER BY m.month_field DESC, m.os_platform;

-- =====================================================
-- END OF THOUGHTSPOT SQL METRIC VIEWS
-- All ThoughtSpot TML formulas have been converted to Databricks SQL
-- Views are ready for Lakeview Dashboard integration
-- =====================================================