-- =====================================================
-- ThoughtSpot SQL Generation for PaaS Tracking Card - Complete Metric Views
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Database: team_css_analytics_prod.hpx_analytics.paas_tracking_card
-- =====================================================

-- Calculated Measures View (Based on ThoughtSpot Model Formulas)
CREATE OR REPLACE VIEW paas_tracking_card_calculated_metrics AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in,
    
    -- Viewed PaaS Tracking Card (Formula: Viewed PaaS Tracking Card)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS `viewed_paas_tracking_card`,
    
    -- Clicked Complete Setup (Formula: Clicked Complete Setup)
    COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) AS `clicked_complete_setup`,
    
    -- Clicked Expand (Formula: Clicked Expand)
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS `clicked_expand`,
    
    -- Clicked Order Confirmation (Formula: Clicked Order Confirmation)
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) AS `clicked_order_confirmation`,
    
    -- Clicked Order Processing (Formula: Clicked Order Processing)
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) AS `clicked_order_processing`,
    
    -- Clicked Track Delivery (Formula: Clicked Track Delivery)
    COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) AS `clicked_track_delivery`,
    
    -- Confirmed (Formula: Confirmed)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) AS `confirmed`,
    
    -- Delivered (Formula: Delivered)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS `delivered`,
    
    -- Delivered - Pill (Formula: Delivered - Pill)
    COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) AS `delivered_pill`,
    
    -- Onboarded (Formula: Onboarded)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) AS `onboarded`,
    
    -- Order Confirmed - Pill (Formula: Order Confirmed - Pill)
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) AS `order_confirmed_pill`,
    
    -- Processed (Formula: Processed)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) AS `processed`,
    
    -- Processing - Pill (Formula: Processing - Pill)
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) AS `processing_pill`,
    
    -- Shipped (Formula: Shipped)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) AS `shipped`,
    
    -- Shipped - Pill (Formula: Shipped - Pill)
    COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) AS `shipped_pill`,
    
    -- Support Cases (Formula: Support Cases)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) AS `support_cases`,
    
    -- Base Aggregated Measures
    SUM(total_printer_count) AS `total_printer_count_sum`,
    SUM(total_device_count) AS `total_device_count_sum`,
    SUM(total_accessory_count) AS `total_accessory_count_sum`,
    SUM(total_pc_count) AS `total_pc_count_sum`,
    SUM(max_total_printer_count) AS `max_total_printer_count_sum`,
    SUM(max_total_device_count) AS `max_total_device_count_sum`,
    SUM(max_total_accessory_count) AS `max_total_accessory_count_sum`,
    SUM(max_total_pc_count) AS `max_total_pc_count_sum`
    
FROM paas_tracking_card_sample
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform,
    app_version,
    geo_country_code,
    is_hpid_signed_in