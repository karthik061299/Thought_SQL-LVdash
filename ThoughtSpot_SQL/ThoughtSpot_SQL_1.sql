-- =====================================================
-- ThoughtSpot SQL Generation for PaaS Tracking Card
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Database: team_css_analytics_prod.hpx_analytics.paas_tracking_card
-- =====================================================

-- Main Metric View for PaaS Tracking Card Analytics
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Base Dimensions
    `session_id`,
    `app_package_deployed_uuid`,
    `os_platform`,
    `app_name`,
    `app_package_id`,
    `app_version`,
    `session_start_date_time`,
    DATE(`session_start_date_time`) AS `date`,
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `device_app_package_deployed_uuid`,
    `associated_device_session_id`,
    `aip_device_uuid`,
    `is_associated_device`,
    
    -- Base Measures (Counts)
    `total_printer_count`,
    `total_device_count`,
    `total_accessory_count`,
    `total_pc_count`,
    `max_total_printer_count`,
    `max_total_device_count`,
    `max_total_accessory_count`,
    `max_total_pc_count`,
    
    -- Boolean Attributes for Tracking Card Views
    `is_viewed_aip_tracking_card`,
    `is_viewed_aip_tracking_card_order_confirmed`,
    `is_viewed_aip_tracking_card_order_processing`,
    `is_viewed_aip_tracking_card_order_shipped`,
    `is_viewed_aip_tracking_card_order_delivered`,
    
    -- Boolean Attributes for Click Actions
    `is_clicked_aip_order_accordian`,
    `is_clicked_order_confirmation`,
    `is_clicked_order_processing`,
    `is_clicked_track_delivery`,
    `is_clicked_complete_setup`,
    `is_clicked_order_confirmation_pill`,
    `is_clicked_order_processing_pill`,
    `is_clicked_order_shipped_pill`,
    `is_clicked_order_delivered_pill`,
    `is_clicked_aip_order_accordian_order_confirmed`,
    `is_clicked_aip_order_accordian_order_processing`,
    `is_clicked_aip_order_accordian_order_shipped`,
    `is_clicked_aip_order_accord`,
    `is_clicked_support`,
    
    -- Boolean Attributes for Setup and Support
    `is_aip_setup_start`,
    `is_aip_setup_complete`,
    `is_ows_start`,
    `is_oobe_complete`,
    `is_oobe_support_session`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- Calculated Measures View (Based on ThoughtSpot Model Formulas)
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics AS
SELECT 
    DATE_TRUNC('month', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    
    -- Viewed PaaS Tracking Card
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) AS `viewed_paas_tracking_card`,
    
    -- Clicked Complete Setup
    COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) AS `clicked_complete_setup`,
    
    -- Clicked Expand
    COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) AS `clicked_expand`,
    
    -- Clicked Order Confirmation
    COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) AS `clicked_order_confirmation`,
    
    -- Clicked Order Processing
    COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) AS `clicked_order_processing`,
    
    -- Clicked Track Delivery
    COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) AS `clicked_track_delivery`,
    
    -- Confirmed (Order Status)
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) AS `confirmed`,
    
    -- Delivered (Order Status)
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) AS `delivered`,
    
    -- Delivered - Pill Clicks
    COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) AS `delivered_pill`,
    
    -- Onboarded (Delivered + Setup Complete)
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) AS `onboarded`,
    
    -- Order Confirmed - Pill Clicks
    COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) AS `order_confirmed_pill`,
    
    -- Processed (Order Status)
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) AS `processed`,
    
    -- Processing - Pill Clicks
    COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) AS `processing_pill`,
    
    -- Shipped (Order Status)
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) AS `shipped`,
    
    -- Shipped - Pill Clicks
    COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) AS `shipped_pill`,
    
    -- Support Cases (Delivered + Support Session)
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) AS `support_cases`,
    
    -- Base Aggregated Measures
    SUM(`total_printer_count`) AS `total_printer_count_sum`,
    SUM(`total_device_count`) AS `total_device_count_sum`,
    SUM(`total_accessory_count`) AS `total_accessory_count_sum`,
    SUM(`total_pc_count`) AS `total_pc_count_sum`,
    SUM(`max_total_printer_count`) AS `max_total_printer_count_sum`,
    SUM(`max_total_device_count`) AS `max_total_device_count_sum`,
    SUM(`max_total_accessory_count`) AS `max_total_accessory_count_sum`,
    SUM(`max_total_pc_count`) AS `max_total_pc_count_sum`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', `session_start_date_time`),
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`;

-- Dashboard-Ready View for Liveboard Visualizations
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_metrics AS
SELECT 
    `month_date`,
    `os_platform`,
    `app_version`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    
    -- Viz 1: PaaS Tracking Card Views
    `viewed_paas_tracking_card`,
    
    -- Viz 2: Clicks on Expand - PaaS Tracking Card
    `clicked_expand`,
    
    -- Viz 3: Clicks on Each Status - PaaS Tracking Card
    `clicked_order_confirmation`,
    `clicked_order_processing`,
    `clicked_track_delivery`,
    `clicked_complete_setup`,
    
    -- Viz 4: Delivered vs Onboarded and Support Cases
    `delivered`,
    `onboarded`,
    `support_cases`,
    
    -- Viz 5: Tracking Card Status Pill Clicks
    `order_confirmed_pill`,
    `processing_pill`,
    `shipped_pill`,
    `delivered_pill`,
    
    -- Viz 6: Tracking Card Status
    `processed`,
    `shipped`,
    `confirmed`,
    `delivered` AS `delivered_status`,
    
    -- Additional aggregated measures
    `total_printer_count_sum`,
    `total_device_count_sum`,
    `total_accessory_count_sum`,
    `total_pc_count_sum`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_calculated_metrics;

-- Summary Statistics View
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_summary AS
SELECT 
    `month_date`,
    `os_platform`,
    
    -- Key Performance Indicators
    SUM(`viewed_paas_tracking_card`) AS `total_views`,
    SUM(`clicked_expand`) AS `total_expand_clicks`,
    SUM(`delivered`) AS `total_delivered`,
    SUM(`onboarded`) AS `total_onboarded`,
    SUM(`support_cases`) AS `total_support_cases`,
    
    -- Conversion Rates (as percentages)
    CASE 
        WHEN SUM(`viewed_paas_tracking_card`) > 0 
        THEN ROUND((SUM(`clicked_expand`) * 100.0 / SUM(`viewed_paas_tracking_card`)), 2)
        ELSE 0 
    END AS `expand_click_rate_percent`,
    
    CASE 
        WHEN SUM(`delivered`) > 0 
        THEN ROUND((SUM(`onboarded`) * 100.0 / SUM(`delivered`)), 2)
        ELSE 0 
    END AS `onboarding_completion_rate_percent`,
    
    CASE 
        WHEN SUM(`delivered`) > 0 
        THEN ROUND((SUM(`support_cases`) * 100.0 / SUM(`delivered`)), 2)
        ELSE 0 
    END AS `support_case_rate_percent`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card_dashboard_metrics
GROUP BY 
    `month_date`,
    `os_platform`
ORDER BY 
    `month_date` DESC,
    `os_platform`;

-- =====================================================
-- End of ThoughtSpot SQL Generation
-- =====================================================