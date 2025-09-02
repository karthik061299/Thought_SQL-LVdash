-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL Environment
-- Version 4: Complete metric views implementation
-- =====================================================

-- Source Table: team_css_analytics_prod.hpx_analytics.paas_tracking_card
-- Connection: dataos-prod-css-analytics (Databricks)

-- =====================================================
-- METRIC VIEW 1: Base Dimensions and Attributes
-- =====================================================
CREATE OR REPLACE VIEW paas_tracking_card_dimensions AS
SELECT DISTINCT
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    os_platform,
    app_name,
    app_package_id,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    session_start_date_time,
    DATE(session_start_date_time) AS session_date,
    DATE_TRUNC('MONTH', session_start_date_time) AS session_month,
    DATE_TRUNC('YEAR', session_start_date_time) AS session_year,
    associated_device_session_id,
    aip_device_uuid,
    is_associated_device
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- METRIC VIEW 2: ThoughtSpot Model Formula Metrics
-- =====================================================
CREATE OR REPLACE VIEW paas_tracking_card_formula_metrics AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in,
    
    -- Formula: Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END)
    END AS viewed_paas_tracking_card,
    
    -- Formula: Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END)
    END AS clicked_expand,
    
    -- Formula: Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END)
    END AS clicked_order_confirmation,
    
    -- Formula: Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END)
    END AS clicked_order_processing,
    
    -- Formula: Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END)
    END AS clicked_track_delivery,
    
    -- Formula: Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END)
    END AS clicked_complete_setup,
    
    -- Formula: Confirmed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END)
    END AS confirmed,
    
    -- Formula: Processed
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END)
    END AS processed,
    
    -- Formula: Shipped
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END)
    END AS shipped,
    
    -- Formula: Delivered
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END)
    END AS delivered,
    
    -- Formula: Onboarded
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END)
    END AS onboarded,
    
    -- Formula: Support Cases
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END)
    END AS support_cases,
    
    -- Formula: Order Confirmed - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END)
    END AS order_confirmed_pill,
    
    -- Formula: Processing - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END)
    END AS processing_pill,
    
    -- Formula: Shipped - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END)
    END AS shipped_pill,
    
    -- Formula: Delivered - Pill
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END)
    END AS delivered_pill
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in;

-- =====================================================
-- METRIC VIEW 3: Liveboard Viz 1 - PaaS Tracking Card Views
-- =====================================================
CREATE OR REPLACE VIEW viz_paas_tracking_card_views AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    os_platform,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_paas_tracking_card
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    os_platform;

-- =====================================================
-- METRIC VIEW 4: Liveboard Viz 2 - Clicks on Expand
-- =====================================================
CREATE OR REPLACE VIEW viz_clicks_on_expand AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    os_platform,
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS clicked_expand
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    os_platform;

-- =====================================================
-- METRIC VIEW 5: Liveboard Viz 3 - Clicks on Each Status
-- =====================================================
CREATE OR REPLACE VIEW viz_clicks_on_each_status AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) AS clicked_order_confirmation,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) AS clicked_order_processing,
    COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) AS clicked_track_delivery,
    COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) AS clicked_complete_setup
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time);

-- =====================================================
-- METRIC VIEW 6: Liveboard Viz 4 - Delivered vs Onboarded and Support Cases
-- =====================================================
CREATE OR REPLACE VIEW viz_delivered_onboarded_support AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS delivered,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) AS onboarded,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) AS support_cases
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time);

-- =====================================================
-- METRIC VIEW 7: Liveboard Viz 5 - Status Pill Clicks
-- =====================================================
CREATE OR REPLACE VIEW viz_status_pill_clicks AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) AS order_confirmed_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) AS processing_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) AS shipped_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) AS delivered_pill
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time);

-- =====================================================
-- METRIC VIEW 8: Liveboard Viz 6 - Tracking Card Status
-- =====================================================
CREATE OR REPLACE VIEW viz_tracking_card_status AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) AS confirmed,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) AS processed,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) AS shipped,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS delivered
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time);

-- =====================================================
-- METRIC VIEW 9: Device and Count Aggregations
-- =====================================================
CREATE OR REPLACE VIEW paas_tracking_device_metrics AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    os_platform,
    geo_country_code,
    SUM(total_printer_count) AS total_printer_count_sum,
    SUM(total_device_count) AS total_device_count_sum,
    SUM(total_accessory_count) AS total_accessory_count_sum,
    SUM(total_pc_count) AS total_pc_count_sum,
    SUM(max_total_printer_count) AS max_total_printer_count_sum,
    SUM(max_total_device_count) AS max_total_device_count_sum,
    SUM(max_total_accessory_count) AS max_total_accessory_count_sum,
    SUM(max_total_pc_count) AS max_total_pc_count_sum,
    AVG(total_printer_count) AS avg_printer_count,
    AVG(total_device_count) AS avg_device_count,
    AVG(total_accessory_count) AS avg_accessory_count,
    AVG(total_pc_count) AS avg_pc_count
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', session_start_date_time),
    os_platform,
    geo_country_code;

-- =====================================================
-- MASTER METRIC VIEW: Complete Dashboard Metrics
-- =====================================================
CREATE OR REPLACE VIEW paas_tracking_card_master_metrics AS
SELECT 
    DATE_TRUNC('MONTH', session_start_date_time) AS month_date,
    DATE(session_start_date_time) AS session_date,
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in,
    
    -- Session and App Counts
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT app_package_deployed_uuid) AS unique_apps,
    COUNT(DISTINCT device_app_package_deployed_uuid) AS unique_device_apps,
    COUNT(*) AS total_records,
    
    -- Core Engagement Metrics (ThoughtSpot Formulas)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_paas_tracking_card,
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS clicked_expand,
    
    -- Status Tracking Metrics
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) AS confirmed,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) AS processed,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) AS shipped,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS delivered,
    
    -- Action Click Metrics
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) AS clicked_order_confirmation,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) AS clicked_order_processing,
    COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) AS clicked_track_delivery,
    COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) AS clicked_complete_setup,
    
    -- Pill Click Metrics
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) AS order_confirmed_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) AS processing_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) AS shipped_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) AS delivered_pill,
    
    -- Outcome Metrics
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) AS onboarded,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) AS support_cases,
    
    -- Device Count Aggregations
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
    DATE_TRUNC('MONTH', session_start_date_time),
    DATE(session_start_date_time),
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in;

-- =====================================================
-- END OF METRIC VIEWS DEFINITION
-- =====================================================

/*
SUMMARY OF CREATED VIEWS:
1. paas_tracking_card_dimensions - Base dimensional data
2. paas_tracking_card_formula_metrics - All ThoughtSpot model formulas
3. viz_paas_tracking_card_views - Liveboard Viz 1 metrics
4. viz_clicks_on_expand - Liveboard Viz 2 metrics
5. viz_clicks_on_each_status - Liveboard Viz 3 metrics
6. viz_delivered_onboarded_support - Liveboard Viz 4 metrics
7. viz_status_pill_clicks - Liveboard Viz 5 metrics
8. viz_tracking_card_status - Liveboard Viz 6 metrics
9. paas_tracking_device_metrics - Device count aggregations
10. paas_tracking_card_master_metrics - Complete dashboard metrics

All views are:
- Databricks SQL compatible
- Environment-agnostic (using fully qualified table names from TML)
- Faithful to ThoughtSpot TML logic and formulas
- Ready for AI/BI integration and JSON generation
- Optimized for dashboard rendering in Databricks Lakeview
*/