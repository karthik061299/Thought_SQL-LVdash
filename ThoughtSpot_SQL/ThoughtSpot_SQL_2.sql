-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL Environment
-- Version 2: Fixed syntax and table references
-- =====================================================

-- Test basic table access first
SELECT COUNT(*) as record_count 
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card 
LIMIT 1;

-- =====================================================
-- METRIC VIEW 1: PaaS Tracking Card Base Metrics
-- =====================================================
CREATE OR REPLACE VIEW paas_tracking_card_base_metrics AS
SELECT 
    -- Dimension Fields
    session_id,
    app_package_deployed_uuid,
    os_platform,
    app_name,
    app_package_id,
    app_version,
    session_start_date_time,
    DATE(session_start_date_time) AS date_field,
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    geo_country_code,
    is_hpid_signed_in,
    device_app_package_deployed_uuid,
    associated_device_session_id,
    aip_device_uuid,
    is_associated_device,
    
    -- Boolean Attribute Fields
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
    
    -- Measure Fields (Counts)
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
-- METRIC VIEW 2: Calculated Measures from Model Formulas
-- =====================================================
CREATE OR REPLACE VIEW paas_tracking_card_calculated_metrics AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in,
    
    -- Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END)
    END AS viewed_paas_tracking_card,
    
    -- Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END)
    END AS clicked_expand,
    
    -- Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END)
    END AS clicked_order_confirmation,
    
    -- Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END)
    END AS clicked_order_processing,
    
    -- Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END)
    END AS clicked_track_delivery,
    
    -- Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END)
    END AS clicked_complete_setup,
    
    -- Confirmed Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END)
    END AS confirmed,
    
    -- Processed Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END)
    END AS processed,
    
    -- Shipped Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END)
    END AS shipped,
    
    -- Delivered Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END)
    END AS delivered,
    
    -- Onboarded (Delivered AND Setup Complete)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END)
    END AS onboarded,
    
    -- Support Cases (Delivered AND Support Session)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END)
    END AS support_cases,
    
    -- Aggregated Counts
    SUM(total_printer_count) AS total_printer_count_sum,
    SUM(total_device_count) AS total_device_count_sum,
    SUM(total_accessory_count) AS total_accessory_count_sum,
    SUM(total_pc_count) AS total_pc_count_sum
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in;

-- =====================================================
-- SUMMARY METRIC VIEW: All Key Metrics Combined
-- =====================================================
CREATE OR REPLACE VIEW paas_tracking_card_summary_metrics AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    DATE(session_start_date_time) AS date_field,
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in,
    
    -- Core Engagement Metrics
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_paas_tracking_card_count,
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS clicked_expand_count,
    
    -- Status View Metrics
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) AS confirmed_count,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) AS processed_count,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) AS shipped_count,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS delivered_count,
    
    -- Action Click Metrics
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) AS clicked_order_confirmation_count,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) AS clicked_order_processing_count,
    COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) AS clicked_track_delivery_count,
    COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) AS clicked_complete_setup_count,
    
    -- Outcome Metrics
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) AS onboarded_count,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) AS support_cases_count,
    
    -- Device Count Aggregations
    SUM(total_printer_count) AS total_printer_count_sum,
    SUM(total_device_count) AS total_device_count_sum,
    SUM(total_accessory_count) AS total_accessory_count_sum,
    SUM(total_pc_count) AS total_pc_count_sum,
    
    -- Session Counts
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_apps,
    COUNT(*) AS total_records
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    DATE(session_start_date_time),
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in;

-- Test the views
SELECT * FROM paas_tracking_card_base_metrics LIMIT 5;
SELECT * FROM paas_tracking_card_calculated_metrics LIMIT 5;
SELECT * FROM paas_tracking_card_summary_metrics LIMIT 5;

-- =====================================================
-- END OF METRIC VIEWS
-- =====================================================