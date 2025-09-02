-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from ThoughtSpot TML files - Version 2
-- Database: team_css_analytics_prod.hpx_analytics
-- Fixed for Databricks compatibility
-- =====================================================

-- Base metric view for PaaS Tracking Card data
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Base Attributes
    session_id,
    app_package_deployed_uuid,
    os_platform,
    app_name,
    app_package_id,
    app_version,
    session_start_date_time,
    geo_country_code,
    is_hpid_signed_in,
    device_app_package_deployed_uuid,
    associated_device_session_id,
    aip_device_uuid,
    is_associated_device,
    
    -- Date Dimension
    DATE(session_start_date_time) AS date_dim,
    DATE_TRUNC('month', session_start_date_time) AS month_date_dim,
    
    -- Boolean Attributes
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
    is_clicked_aip_order_accordian_order_confirmed,
    is_clicked_aip_order_accordian_order_processing,
    is_clicked_aip_order_accordian_order_shipped,
    is_clicked_aip_order_accord,
    is_aip_setup_start,
    is_ows_start,
    is_oobe_complete,
    is_aip_setup_complete,
    is_clicked_support,
    is_oobe_support_session,
    
    -- Count Measures
    total_printer_count,
    total_device_count,
    total_accessory_count,
    total_pc_count,
    max_total_printer_count,
    max_total_device_count,
    max_total_accessory_count,
    max_total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Aggregated metrics view with calculated measures
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_aggregated_metrics AS
SELECT 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    date_dim,
    month_date_dim,
    
    -- Viewed PaaS Tracking Card Measure
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_paas_tracking_card,
    
    -- Clicked Expand Measure
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS clicked_expand,
    
    -- Clicked Order Confirmation Measure
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) AS clicked_order_confirmation,
    
    -- Clicked Order Processing Measure
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) AS clicked_order_processing,
    
    -- Clicked Track Delivery Measure
    COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) AS clicked_track_delivery,
    
    -- Clicked Complete Setup Measure
    COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) AS clicked_complete_setup,
    
    -- Confirmed Status Measure
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) AS confirmed,
    
    -- Processed Status Measure
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) AS processed,
    
    -- Shipped Status Measure
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) AS shipped,
    
    -- Delivered Status Measure
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS delivered,
    
    -- Onboarded Measure (Delivered AND Setup Complete)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) AS onboarded,
    
    -- Support Cases Measure (Delivered AND Support Session)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) AS support_cases,
    
    -- Pill Click Measures
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) AS order_confirmed_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) AS processing_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) AS shipped_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) AS delivered_pill,
    
    -- Device and Count Aggregations
    SUM(total_printer_count) AS total_printer_count_sum,
    SUM(total_device_count) AS total_device_count_sum,
    SUM(total_accessory_count) AS total_accessory_count_sum,
    SUM(total_pc_count) AS total_pc_count_sum,
    SUM(max_total_printer_count) AS max_total_printer_count_sum,
    SUM(max_total_device_count) AS max_total_device_count_sum,
    SUM(max_total_accessory_count) AS max_total_accessory_count_sum,
    SUM(max_total_pc_count) AS max_total_pc_count_sum
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics
GROUP BY 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    date_dim,
    month_date_dim;

-- Monthly aggregated view for dashboard visualizations
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics AS
SELECT 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    month_date_dim,
    
    -- Monthly Aggregated Measures
    SUM(viewed_paas_tracking_card) AS viewed_paas_tracking_card_monthly,
    SUM(clicked_expand) AS clicked_expand_monthly,
    SUM(clicked_order_confirmation) AS clicked_order_confirmation_monthly,
    SUM(clicked_order_processing) AS clicked_order_processing_monthly,
    SUM(clicked_track_delivery) AS clicked_track_delivery_monthly,
    SUM(clicked_complete_setup) AS clicked_complete_setup_monthly,
    SUM(confirmed) AS confirmed_monthly,
    SUM(processed) AS processed_monthly,
    SUM(shipped) AS shipped_monthly,
    SUM(delivered) AS delivered_monthly,
    SUM(onboarded) AS onboarded_monthly,
    SUM(support_cases) AS support_cases_monthly,
    SUM(order_confirmed_pill) AS order_confirmed_pill_monthly,
    SUM(processing_pill) AS processing_pill_monthly,
    SUM(shipped_pill) AS shipped_pill_monthly,
    SUM(delivered_pill) AS delivered_pill_monthly,
    
    -- Device Count Aggregations
    SUM(total_printer_count_sum) AS total_printer_count_monthly,
    SUM(total_device_count_sum) AS total_device_count_monthly,
    SUM(total_accessory_count_sum) AS total_accessory_count_monthly,
    SUM(total_pc_count_sum) AS total_pc_count_monthly,
    SUM(max_total_printer_count_sum) AS max_total_printer_count_monthly,
    SUM(max_total_device_count_sum) AS max_total_device_count_monthly,
    SUM(max_total_accessory_count_sum) AS max_total_accessory_count_monthly,
    SUM(max_total_pc_count_sum) AS max_total_pc_count_monthly
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_aggregated_metrics
GROUP BY 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    month_date_dim;

-- Dashboard-ready view for PaaS Tracking Card visualizations
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_metrics AS
SELECT 
    -- Dimensions
    os_platform AS os_platform_dim,
    app_version AS app_version_dim,
    geo_country_code AS geo_country_code_dim,
    is_hpid_signed_in AS is_hpid_signed_in_dim,
    month_date_dim AS month_date_dim,
    
    -- Key Performance Metrics for Dashboards
    COALESCE(viewed_paas_tracking_card_monthly, 0) AS paas_tracking_card_views,
    COALESCE(clicked_expand_monthly, 0) AS clicks_on_expand,
    COALESCE(clicked_order_confirmation_monthly, 0) AS clicks_order_confirmation,
    COALESCE(clicked_order_processing_monthly, 0) AS clicks_order_processing,
    COALESCE(clicked_track_delivery_monthly, 0) AS clicks_track_delivery,
    COALESCE(clicked_complete_setup_monthly, 0) AS clicks_complete_setup,
    
    -- Status Tracking Metrics
    COALESCE(confirmed_monthly, 0) AS status_confirmed,
    COALESCE(processed_monthly, 0) AS status_processed,
    COALESCE(shipped_monthly, 0) AS status_shipped,
    COALESCE(delivered_monthly, 0) AS status_delivered,
    COALESCE(onboarded_monthly, 0) AS status_onboarded,
    COALESCE(support_cases_monthly, 0) AS status_support_cases,
    
    -- Pill Click Metrics
    COALESCE(order_confirmed_pill_monthly, 0) AS pill_order_confirmed,
    COALESCE(processing_pill_monthly, 0) AS pill_processing,
    COALESCE(shipped_pill_monthly, 0) AS pill_shipped,
    COALESCE(delivered_pill_monthly, 0) AS pill_delivered,
    
    -- Device Count Metrics
    COALESCE(total_printer_count_monthly, 0) AS total_printers,
    COALESCE(total_device_count_monthly, 0) AS total_devices,
    COALESCE(total_accessory_count_monthly, 0) AS total_accessories,
    COALESCE(total_pc_count_monthly, 0) AS total_pcs
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_metrics;

-- Summary view for high-level KPIs
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_kpi_summary AS
SELECT 
    month_date_dim,
    
    -- Overall KPIs
    SUM(paas_tracking_card_views) AS total_paas_views,
    SUM(clicks_on_expand) AS total_expand_clicks,
    SUM(status_delivered) AS total_delivered,
    SUM(status_onboarded) AS total_onboarded,
    SUM(status_support_cases) AS total_support_cases,
    
    -- Conversion Rates
    CASE 
        WHEN SUM(paas_tracking_card_views) > 0 
        THEN ROUND((SUM(clicks_on_expand) * 100.0 / SUM(paas_tracking_card_views)), 2)
        ELSE 0 
    END AS expand_conversion_rate_pct,
    
    CASE 
        WHEN SUM(status_delivered) > 0 
        THEN ROUND((SUM(status_onboarded) * 100.0 / SUM(status_delivered)), 2)
        ELSE 0 
    END AS onboarding_completion_rate_pct,
    
    CASE 
        WHEN SUM(status_delivered) > 0 
        THEN ROUND((SUM(status_support_cases) * 100.0 / SUM(status_delivered)), 2)
        ELSE 0 
    END AS support_case_rate_pct
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_metrics
GROUP BY month_date_dim
ORDER BY month_date_dim;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views - Version 2
-- =====================================================