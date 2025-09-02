-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from ThoughtSpot TML files - Version 4 (Final)
-- Complete implementation with all measures and dimensions
-- =====================================================

-- Base view with all dimensions and measures from ThoughtSpot model
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Primary Keys and Identifiers
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    associated_device_session_id,
    aip_device_uuid,
    
    -- Dimensional Attributes
    os_platform,
    app_name,
    app_package_id,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    is_associated_device,
    
    -- Date Dimensions
    session_start_date_time,
    DATE(session_start_date_time) AS session_date,
    DATE_TRUNC('month', session_start_date_time) AS month_date,
    YEAR(session_start_date_time) AS session_year,
    MONTH(session_start_date_time) AS session_month,
    
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
    
    -- Additional Action Flags
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

-- Calculated Measures View (implementing ThoughtSpot formulas)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures AS
SELECT 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    session_date,
    month_date,
    
    -- Viewed PaaS Tracking Card (Formula: unique_count_if)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS viewed_paas_tracking_card,
    
    -- Clicked Expand (Formula: unique_count_if)
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS clicked_expand,
    
    -- Clicked Order Confirmation (Formula: unique_count_if)
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) AS clicked_order_confirmation,
    
    -- Clicked Order Processing (Formula: unique_count_if)
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) AS clicked_order_processing,
    
    -- Clicked Track Delivery (Formula: unique_count_if)
    COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) AS clicked_track_delivery,
    
    -- Clicked Complete Setup (Formula: unique_count_if)
    COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) AS clicked_complete_setup,
    
    -- Confirmed Status (Formula: unique_count_if)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) AS confirmed,
    
    -- Processed Status (Formula: unique_count_if with device_app_package_deployed_uuid)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) AS processed,
    
    -- Shipped Status (Formula: unique_count_if)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) AS shipped,
    
    -- Delivered Status (Formula: unique_count_if with device_app_package_deployed_uuid)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS delivered,
    
    -- Onboarded (Formula: delivered AND setup complete)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) AS onboarded,
    
    -- Support Cases (Formula: delivered AND support session)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) AS support_cases,
    
    -- Pill Click Measures
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) AS order_confirmed_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) AS processing_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) AS shipped_pill,
    COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) AS delivered_pill,
    
    -- Device Count Aggregations
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
    session_date,
    month_date;

-- Monthly Aggregated View for Dashboard Visualizations
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_dashboard AS
SELECT 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    month_date,
    
    -- Monthly Totals for Dashboard Charts
    SUM(viewed_paas_tracking_card) AS monthly_paas_views,
    SUM(clicked_expand) AS monthly_expand_clicks,
    SUM(clicked_order_confirmation) AS monthly_confirmation_clicks,
    SUM(clicked_order_processing) AS monthly_processing_clicks,
    SUM(clicked_track_delivery) AS monthly_delivery_clicks,
    SUM(clicked_complete_setup) AS monthly_setup_clicks,
    
    -- Status Tracking Monthly Totals
    SUM(confirmed) AS monthly_confirmed,
    SUM(processed) AS monthly_processed,
    SUM(shipped) AS monthly_shipped,
    SUM(delivered) AS monthly_delivered,
    SUM(onboarded) AS monthly_onboarded,
    SUM(support_cases) AS monthly_support_cases,
    
    -- Pill Clicks Monthly Totals
    SUM(order_confirmed_pill) AS monthly_confirmed_pill_clicks,
    SUM(processing_pill) AS monthly_processing_pill_clicks,
    SUM(shipped_pill) AS monthly_shipped_pill_clicks,
    SUM(delivered_pill) AS monthly_delivered_pill_clicks,
    
    -- Device Counts Monthly Totals
    SUM(total_printer_count_sum) AS monthly_total_printers,
    SUM(total_device_count_sum) AS monthly_total_devices,
    SUM(total_accessory_count_sum) AS monthly_total_accessories,
    SUM(total_pc_count_sum) AS monthly_total_pcs
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_measures
GROUP BY 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    month_date;

-- Final Dashboard View with Clean Column Names
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_final AS
SELECT 
    -- Dimension Columns
    os_platform AS os_platform,
    app_version AS app_version,
    geo_country_code AS geo_country_code,
    is_hpid_signed_in AS is_hpid_signed_in,
    month_date AS month_date,
    
    -- KPI Metrics (matching ThoughtSpot liveboard visualizations)
    COALESCE(monthly_paas_views, 0) AS paas_tracking_card_views,
    COALESCE(monthly_expand_clicks, 0) AS clicks_on_expand,
    COALESCE(monthly_confirmation_clicks, 0) AS clicks_order_confirmation,
    COALESCE(monthly_processing_clicks, 0) AS clicks_order_processing,
    COALESCE(monthly_delivery_clicks, 0) AS clicks_track_delivery,
    COALESCE(monthly_setup_clicks, 0) AS clicks_complete_setup,
    
    -- Status Metrics
    COALESCE(monthly_confirmed, 0) AS status_confirmed,
    COALESCE(monthly_processed, 0) AS status_processed,
    COALESCE(monthly_shipped, 0) AS status_shipped,
    COALESCE(monthly_delivered, 0) AS status_delivered,
    COALESCE(monthly_onboarded, 0) AS status_onboarded,
    COALESCE(monthly_support_cases, 0) AS status_support_cases,
    
    -- Pill Click Metrics
    COALESCE(monthly_confirmed_pill_clicks, 0) AS pill_confirmed_clicks,
    COALESCE(monthly_processing_pill_clicks, 0) AS pill_processing_clicks,
    COALESCE(monthly_shipped_pill_clicks, 0) AS pill_shipped_clicks,
    COALESCE(monthly_delivered_pill_clicks, 0) AS pill_delivered_clicks,
    
    -- Device Count Metrics
    COALESCE(monthly_total_printers, 0) AS total_printers,
    COALESCE(monthly_total_devices, 0) AS total_devices,
    COALESCE(monthly_total_accessories, 0) AS total_accessories,
    COALESCE(monthly_total_pcs, 0) AS total_pcs
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_monthly_dashboard;

-- KPI Summary View for Executive Dashboard
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_kpi_summary AS
SELECT 
    month_date,
    
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
    END AS support_case_rate_pct,
    
    -- Platform Distribution
    COUNT(DISTINCT os_platform) AS platform_count,
    COUNT(DISTINCT app_version) AS version_count,
    COUNT(DISTINCT geo_country_code) AS country_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_final
GROUP BY month_date
ORDER BY month_date;

-- =====================================================
-- End of ThoughtSpot SQL Metric Views - Version 4
-- Complete implementation ready for Databricks execution
-- =====================================================