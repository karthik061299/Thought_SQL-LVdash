-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card - Version 3
-- Generated from ThoughtSpot TML files
-- Database: team_css_analytics_prod.hpx_analytics
-- Corrected for Databricks SQL compatibility
-- =====================================================

-- Base metric view for PaaS Tracking Card data
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_base_metrics AS
SELECT 
    -- Primary Keys and Identifiers
    session_id AS session_id_key,
    app_package_deployed_uuid AS app_package_uuid,
    device_app_package_deployed_uuid AS device_app_package_uuid,
    associated_device_session_id AS associated_session_id,
    aip_device_uuid AS aip_device_id,
    
    -- Application Context
    os_platform AS operating_system,
    app_name AS application_name,
    app_package_id AS app_package_identifier,
    app_version AS application_version,
    
    -- Geographic and User Context
    geo_country_code AS country_code,
    is_hpid_signed_in AS hpid_signed_in_flag,
    is_associated_device AS associated_device_flag,
    
    -- Temporal Dimensions
    session_start_date_time AS session_timestamp,
    DATE(session_start_date_time) AS session_date,
    DATE_TRUNC('month', session_start_date_time) AS session_month,
    DATE_TRUNC('week', session_start_date_time) AS session_week,
    DATE_TRUNC('year', session_start_date_time) AS session_year,
    
    -- Tracking Card View Events
    is_viewed_aip_tracking_card AS viewed_tracking_card,
    is_viewed_aip_tracking_card_order_confirmed AS viewed_order_confirmed,
    is_viewed_aip_tracking_card_order_processing AS viewed_order_processing,
    is_viewed_aip_tracking_card_order_shipped AS viewed_order_shipped,
    is_viewed_aip_tracking_card_order_delivered AS viewed_order_delivered,
    
    -- Click Events - Main Actions
    is_clicked_aip_order_accordian AS clicked_expand_accordion,
    is_clicked_order_confirmation AS clicked_order_confirmation_action,
    is_clicked_order_processing AS clicked_order_processing_action,
    is_clicked_track_delivery AS clicked_track_delivery_action,
    is_clicked_complete_setup AS clicked_complete_setup_action,
    
    -- Click Events - Status Pills
    is_clicked_order_confirmation_pill AS clicked_confirmation_pill,
    is_clicked_order_processing_pill AS clicked_processing_pill,
    is_clicked_order_shipped_pill AS clicked_shipped_pill,
    is_clicked_order_delivered_pill AS clicked_delivered_pill,
    
    -- Setup and Onboarding Events
    is_aip_setup_start AS aip_setup_started,
    is_aip_setup_complete AS aip_setup_completed,
    is_ows_start AS ows_started,
    is_oobe_complete AS oobe_completed,
    
    -- Support Events
    is_clicked_support AS clicked_support,
    is_oobe_support_session AS oobe_support_session,
    
    -- Device and Asset Counts
    COALESCE(total_printer_count, 0) AS printer_count,
    COALESCE(total_device_count, 0) AS device_count,
    COALESCE(total_accessory_count, 0) AS accessory_count,
    COALESCE(total_pc_count, 0) AS pc_count,
    COALESCE(max_total_printer_count, 0) AS max_printer_count,
    COALESCE(max_total_device_count, 0) AS max_device_count,
    COALESCE(max_total_accessory_count, 0) AS max_accessory_count,
    COALESCE(max_total_pc_count, 0) AS max_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Calculated measures view matching ThoughtSpot formulas
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures AS
SELECT 
    operating_system,
    application_version,
    country_code,
    hpid_signed_in_flag,
    session_date,
    session_month,
    session_week,
    session_year,
    
    -- Viewed PaaS Tracking Card (matching ThoughtSpot formula)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN viewed_tracking_card = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN viewed_tracking_card = true THEN app_package_uuid END)
    END AS viewed_paas_tracking_card_measure,
    
    -- Clicked Expand (matching ThoughtSpot formula)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_expand_accordion = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_expand_accordion = true THEN app_package_uuid END)
    END AS clicked_expand_measure,
    
    -- Clicked Order Confirmation (matching ThoughtSpot formula)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_order_confirmation_action = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_order_confirmation_action = true THEN app_package_uuid END)
    END AS clicked_order_confirmation_measure,
    
    -- Clicked Order Processing (matching ThoughtSpot formula)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_order_processing_action = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_order_processing_action = true THEN app_package_uuid END)
    END AS clicked_order_processing_measure,
    
    -- Clicked Track Delivery (matching ThoughtSpot formula)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_track_delivery_action = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_track_delivery_action = true THEN app_package_uuid END)
    END AS clicked_track_delivery_measure,
    
    -- Clicked Complete Setup (matching ThoughtSpot formula)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_complete_setup_action = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_complete_setup_action = true THEN app_package_uuid END)
    END AS clicked_complete_setup_measure,
    
    -- Confirmed Status (matching ThoughtSpot formula)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN viewed_order_confirmed = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN viewed_order_confirmed = true THEN app_package_uuid END)
    END AS confirmed_measure,
    
    -- Processed Status (matching ThoughtSpot formula - uses device_app_package_uuid)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN viewed_order_processing = true THEN device_app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN viewed_order_processing = true THEN device_app_package_uuid END)
    END AS processed_measure,
    
    -- Shipped Status (matching ThoughtSpot formula)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN viewed_order_shipped = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN viewed_order_shipped = true THEN app_package_uuid END)
    END AS shipped_measure,
    
    -- Delivered Status (matching ThoughtSpot formula - uses device_app_package_uuid)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN viewed_order_delivered = true THEN device_app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN viewed_order_delivered = true THEN device_app_package_uuid END)
    END AS delivered_measure,
    
    -- Onboarded (matching ThoughtSpot formula - Delivered AND Setup Complete)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN viewed_order_delivered = true AND aip_setup_completed = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN viewed_order_delivered = true AND aip_setup_completed = true THEN app_package_uuid END)
    END AS onboarded_measure,
    
    -- Support Cases (matching ThoughtSpot formula - Delivered AND Support Session)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN viewed_order_delivered = true AND oobe_support_session = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN viewed_order_delivered = true AND oobe_support_session = true THEN app_package_uuid END)
    END AS support_cases_measure,
    
    -- Pill Click Measures (matching ThoughtSpot formulas)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_confirmation_pill = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_confirmation_pill = true THEN app_package_uuid END)
    END AS order_confirmed_pill_measure,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_processing_pill = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_processing_pill = true THEN app_package_uuid END)
    END AS processing_pill_measure,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_shipped_pill = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_shipped_pill = true THEN app_package_uuid END)
    END AS shipped_pill_measure,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN clicked_delivered_pill = true THEN app_package_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN clicked_delivered_pill = true THEN app_package_uuid END)
    END AS delivered_pill_measure,
    
    -- Device Count Aggregations
    SUM(printer_count) AS total_printer_count_sum,
    SUM(device_count) AS total_device_count_sum,
    SUM(accessory_count) AS total_accessory_count_sum,
    SUM(pc_count) AS total_pc_count_sum,
    SUM(max_printer_count) AS max_total_printer_count_sum,
    SUM(max_device_count) AS max_total_device_count_sum,
    SUM(max_accessory_count) AS max_total_accessory_count_sum,
    SUM(max_pc_count) AS max_total_pc_count_sum
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_base_metrics
GROUP BY 
    operating_system,
    application_version,
    country_code,
    hpid_signed_in_flag,
    session_date,
    session_month,
    session_week,
    session_year;

-- Dashboard-ready metric view for Lakeview integration
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_ready AS
SELECT 
    -- Dimension columns with consistent naming
    operating_system AS os_platform_dimension,
    application_version AS app_version_dimension,
    country_code AS geo_country_code_dimension,
    hpid_signed_in_flag AS is_hpid_signed_in_dimension,
    session_date AS date_dimension,
    session_month AS month_date_dimension,
    session_week AS week_date_dimension,
    session_year AS year_date_dimension,
    
    -- Core Metrics (matching ThoughtSpot liveboard visualizations)
    COALESCE(viewed_paas_tracking_card_measure, 0) AS viewed_paas_tracking_card,
    COALESCE(clicked_expand_measure, 0) AS clicked_expand,
    COALESCE(clicked_order_confirmation_measure, 0) AS clicked_order_confirmation,
    COALESCE(clicked_order_processing_measure, 0) AS clicked_order_processing,
    COALESCE(clicked_track_delivery_measure, 0) AS clicked_track_delivery,
    COALESCE(clicked_complete_setup_measure, 0) AS clicked_complete_setup,
    
    -- Status Tracking Metrics
    COALESCE(confirmed_measure, 0) AS confirmed,
    COALESCE(processed_measure, 0) AS processed,
    COALESCE(shipped_measure, 0) AS shipped,
    COALESCE(delivered_measure, 0) AS delivered,
    COALESCE(onboarded_measure, 0) AS onboarded,
    COALESCE(support_cases_measure, 0) AS support_cases,
    
    -- Pill Click Metrics
    COALESCE(order_confirmed_pill_measure, 0) AS order_confirmed_pill,
    COALESCE(processing_pill_measure, 0) AS processing_pill,
    COALESCE(shipped_pill_measure, 0) AS shipped_pill,
    COALESCE(delivered_pill_measure, 0) AS delivered_pill,
    
    -- Device Count Metrics
    COALESCE(total_printer_count_sum, 0) AS total_printer_count,
    COALESCE(total_device_count_sum, 0) AS total_device_count,
    COALESCE(total_accessory_count_sum, 0) AS total_accessory_count,
    COALESCE(total_pc_count_sum, 0) AS total_pc_count,
    COALESCE(max_total_printer_count_sum, 0) AS max_total_printer_count,
    COALESCE(max_total_device_count_sum, 0) AS max_total_device_count,
    COALESCE(max_total_accessory_count_sum, 0) AS max_total_accessory_count,
    COALESCE(max_total_pc_count_sum, 0) AS max_total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures;

-- KPI Summary view for executive dashboards
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_kpi_summary AS
SELECT 
    month_date_dimension AS reporting_month,
    os_platform_dimension AS platform,
    
    -- Primary KPIs
    SUM(viewed_paas_tracking_card) AS total_tracking_card_views,
    SUM(clicked_expand) AS total_expand_interactions,
    SUM(delivered) AS total_delivered_orders,
    SUM(onboarded) AS total_onboarded_users,
    SUM(support_cases) AS total_support_requests,
    
    -- Engagement Metrics
    SUM(clicked_order_confirmation + clicked_order_processing + clicked_track_delivery + clicked_complete_setup) AS total_action_clicks,
    SUM(order_confirmed_pill + processing_pill + shipped_pill + delivered_pill) AS total_pill_clicks,
    
    -- Conversion Rates
    CASE 
        WHEN SUM(viewed_paas_tracking_card) > 0 
        THEN ROUND((SUM(clicked_expand) * 100.0 / SUM(viewed_paas_tracking_card)), 2)
        ELSE 0.0 
    END AS expand_conversion_rate_percent,
    
    CASE 
        WHEN SUM(delivered) > 0 
        THEN ROUND((SUM(onboarded) * 100.0 / SUM(delivered)), 2)
        ELSE 0.0 
    END AS onboarding_success_rate_percent,
    
    CASE 
        WHEN SUM(delivered) > 0 
        THEN ROUND((SUM(support_cases) * 100.0 / SUM(delivered)), 2)
        ELSE 0.0 
    END AS support_request_rate_percent,
    
    -- Device Metrics Summary
    SUM(total_device_count) AS total_managed_devices,
    SUM(total_printer_count) AS total_managed_printers,
    AVG(max_total_device_count) AS avg_max_devices_per_session
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_ready
GROUP BY 
    month_date_dimension,
    os_platform_dimension
ORDER BY 
    reporting_month DESC,
    platform;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views - Version 3
-- Corrected for Databricks SQL compatibility
-- =====================================================