-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card - Version 5
-- Complete implementation matching ThoughtSpot TML specifications
-- Database: team_css_analytics_prod.hpx_analytics
-- =====================================================

-- Base dimensional view with all attributes and measures
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dimensions AS
SELECT 
    -- Primary identifiers
    session_id,
    app_package_deployed_uuid,
    device_app_package_deployed_uuid,
    associated_device_session_id,
    aip_device_uuid,
    
    -- Application and platform dimensions
    os_platform,
    app_name,
    app_package_id,
    app_version,
    geo_country_code,
    
    -- User context
    is_hpid_signed_in,
    is_associated_device,
    
    -- Temporal dimensions
    session_start_date_time,
    DATE(session_start_date_time) AS session_date,
    DATE_TRUNC('month', session_start_date_time) AS month_date,
    DATE_TRUNC('week', session_start_date_time) AS week_date,
    DATE_TRUNC('year', session_start_date_time) AS year_date,
    
    -- Boolean event flags
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
    is_aip_setup_complete,
    is_ows_start,
    is_oobe_complete,
    is_clicked_support,
    is_oobe_support_session,
    
    -- Numeric measures
    total_printer_count,
    total_device_count,
    total_accessory_count,
    total_pc_count,
    max_total_printer_count,
    max_total_device_count,
    max_total_accessory_count,
    max_total_pc_count
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- ThoughtSpot calculated measures view (exact formula replication)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_thoughtspot_measures AS
SELECT 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    session_date,
    month_date,
    
    -- Viewed PaaS Tracking Card (ThoughtSpot formula: unique_count_if)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END)
    END AS viewed_paas_tracking_card,
    
    -- Clicked Expand (ThoughtSpot formula: unique_count_if)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END)
    END AS clicked_expand,
    
    -- Clicked Order Confirmation (ThoughtSpot formula: unique_count_if)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END)
    END AS clicked_order_confirmation,
    
    -- Clicked Order Processing (ThoughtSpot formula: unique_count_if)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END)
    END AS clicked_order_processing,
    
    -- Clicked Track Delivery (ThoughtSpot formula: unique_count_if)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END)
    END AS clicked_track_delivery,
    
    -- Clicked Complete Setup (ThoughtSpot formula: unique_count_if)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END)
    END AS clicked_complete_setup,
    
    -- Confirmed (ThoughtSpot formula: unique_count_if)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END)
    END AS confirmed,
    
    -- Processed (ThoughtSpot formula: unique_count_if with device_app_package_deployed_uuid)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END)
    END AS processed,
    
    -- Shipped (ThoughtSpot formula: unique_count_if)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END)
    END AS shipped,
    
    -- Delivered (ThoughtSpot formula: unique_count_if with device_app_package_deployed_uuid)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END)
    END AS delivered,
    
    -- Onboarded (ThoughtSpot formula: unique_count_if with AND condition)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END)
    END AS onboarded,
    
    -- Support Cases (ThoughtSpot formula: unique_count_if with AND condition)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END)
    END AS support_cases,
    
    -- Pill Click Measures
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END)
    END AS order_confirmed_pill,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END)
    END AS processing_pill,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END)
    END AS shipped_pill,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END)
    END AS delivered_pill,
    
    -- Device count aggregations (SUM aggregation as per ThoughtSpot)
    SUM(total_printer_count) AS total_printer_count_agg,
    SUM(total_device_count) AS total_device_count_agg,
    SUM(total_accessory_count) AS total_accessory_count_agg,
    SUM(total_pc_count) AS total_pc_count_agg,
    SUM(max_total_printer_count) AS max_total_printer_count_agg,
    SUM(max_total_device_count) AS max_total_device_count_agg,
    SUM(max_total_accessory_count) AS max_total_accessory_count_agg,
    SUM(max_total_pc_count) AS max_total_pc_count_agg
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dimensions
GROUP BY 
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    session_date,
    month_date;

-- Final dashboard-ready view for Lakeview integration
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_lakeview_ready AS
SELECT 
    -- Dimensions (matching ThoughtSpot liveboard filters)
    os_platform AS `Os Platform`,
    app_version AS `App Version`,
    geo_country_code AS `Geo Country Code`,
    is_hpid_signed_in AS `Is Hpid Signed In`,
    session_date AS `Date`,
    month_date AS `Month(Date)`,
    
    -- Core measures (matching ThoughtSpot liveboard visualizations)
    COALESCE(viewed_paas_tracking_card, 0) AS `Viewed PaaS Tracking Card`,
    COALESCE(clicked_expand, 0) AS `Clicked Expand`,
    COALESCE(clicked_order_confirmation, 0) AS `Clicked Order Confirmation`,
    COALESCE(clicked_order_processing, 0) AS `Clicked Order Processing`,
    COALESCE(clicked_track_delivery, 0) AS `Clicked Track Delivery`,
    COALESCE(clicked_complete_setup, 0) AS `Clicked Complete Setup`,
    
    -- Status measures
    COALESCE(confirmed, 0) AS `Confirmed`,
    COALESCE(processed, 0) AS `Processed`,
    COALESCE(shipped, 0) AS `Shipped`,
    COALESCE(delivered, 0) AS `Delivered`,
    COALESCE(onboarded, 0) AS `Onboarded`,
    COALESCE(support_cases, 0) AS `Support Cases`,
    
    -- Pill click measures
    COALESCE(order_confirmed_pill, 0) AS `Order Confirmed - Pill`,
    COALESCE(processing_pill, 0) AS `Processing - Pill`,
    COALESCE(shipped_pill, 0) AS `Shipped - Pill`,
    COALESCE(delivered_pill, 0) AS `Delivered - Pill`,
    
    -- Device count measures
    COALESCE(total_printer_count_agg, 0) AS `Total Printer Count`,
    COALESCE(total_device_count_agg, 0) AS `Total Device Count`,
    COALESCE(total_accessory_count_agg, 0) AS `Total Accessory Count`,
    COALESCE(total_pc_count_agg, 0) AS `Total Pc Count`,
    COALESCE(max_total_printer_count_agg, 0) AS `Max Total Printer Count`,
    COALESCE(max_total_device_count_agg, 0) AS `Max Total Device Count`,
    COALESCE(max_total_accessory_count_agg, 0) AS `Max Total Accessory Count`,
    COALESCE(max_total_pc_count_agg, 0) AS `Max Total Pc Count`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_thoughtspot_measures;

-- Summary KPI view for executive reporting
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_executive_summary AS
SELECT 
    `Month(Date)` AS reporting_period,
    `Os Platform` AS platform,
    
    -- Key performance indicators
    SUM(`Viewed PaaS Tracking Card`) AS total_paas_views,
    SUM(`Clicked Expand`) AS total_expansions,
    SUM(`Delivered`) AS total_deliveries,
    SUM(`Onboarded`) AS total_onboarded,
    SUM(`Support Cases`) AS total_support_cases,
    
    -- Conversion metrics
    CASE 
        WHEN SUM(`Viewed PaaS Tracking Card`) > 0 
        THEN ROUND((SUM(`Clicked Expand`) * 100.0 / SUM(`Viewed PaaS Tracking Card`)), 2)
        ELSE 0.0 
    END AS expansion_rate_percent,
    
    CASE 
        WHEN SUM(`Delivered`) > 0 
        THEN ROUND((SUM(`Onboarded`) * 100.0 / SUM(`Delivered`)), 2)
        ELSE 0.0 
    END AS onboarding_completion_rate_percent,
    
    CASE 
        WHEN SUM(`Delivered`) > 0 
        THEN ROUND((SUM(`Support Cases`) * 100.0 / SUM(`Delivered`)), 2)
        ELSE 0.0 
    END AS support_case_rate_percent
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_lakeview_ready
GROUP BY 
    `Month(Date)`,
    `Os Platform`
ORDER BY 
    reporting_period DESC,
    platform;

-- =====================================================
-- ThoughtSpot SQL Metric Views Creation Complete
-- All views are ready for Lakeview Dashboard integration
-- =====================================================