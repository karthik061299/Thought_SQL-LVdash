-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card - Version 2
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL
-- Fixed: Simplified initial view for testing
-- =====================================================

-- Test connection and basic table access
SELECT COUNT(*) as record_count 
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card 
LIMIT 1;

-- =====================================================
-- MAIN METRIC VIEW: paas_tracking_card_metrics
-- Contains all measures, dimensions, and calculated fields from ThoughtSpot model
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_metrics AS
SELECT 
    -- Base Dimensions
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
    
    -- Date Dimension (Formula: Date)
    DATE(session_start_date_time) AS `date_field`,
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    
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
    
    -- Base Measures (SUM aggregation)
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
-- AGGREGATED METRIC VIEW: paas_tracking_card_summary
-- Pre-aggregated metrics for dashboard performance
-- =====================================================

CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_summary AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in,
    
    -- Aggregated Measures
    SUM(total_printer_count) AS `total_printer_count_sum`,
    SUM(total_device_count) AS `total_device_count_sum`,
    SUM(total_accessory_count) AS `total_accessory_count_sum`,
    SUM(total_pc_count) AS `total_pc_count_sum`,
    SUM(max_total_printer_count) AS `max_total_printer_count_sum`,
    SUM(max_total_device_count) AS `max_total_device_count_sum`,
    SUM(max_total_accessory_count) AS `max_total_accessory_count_sum`,
    SUM(max_total_pc_count) AS `max_total_pc_count_sum`,
    
    -- Unique Count Measures (ThoughtSpot Formulas)
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS `viewed_paas_tracking_card_sum`,
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS `clicked_expand_sum`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) AS `clicked_order_confirmation_sum`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) AS `clicked_order_processing_sum`,
    COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) AS `clicked_track_delivery_sum`,
    COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) AS `clicked_complete_setup_sum`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) AS `confirmed_sum`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS `delivered_sum`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) AS `processed_sum`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) AS `shipped_sum`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) AS `onboarded_sum`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) AS `support_cases_sum`,
    
    -- Pill Click Measures
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) AS `order_confirmed_pill_sum`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) AS `processing_pill_sum`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) AS `shipped_pill_sum`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) AS `delivered_pill_sum`,
    
    -- Record Counts
    COUNT(*) AS `total_records`,
    COUNT(DISTINCT session_id) AS `unique_sessions`,
    COUNT(DISTINCT app_package_deployed_uuid) AS `unique_app_packages`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform,
    geo_country_code,
    app_version,
    is_hpid_signed_in;

-- =====================================================
-- LIVEBOARD SPECIFIC VIEWS
-- Views optimized for each visualization in the liveboard
-- =====================================================

-- View for Viz_1: PaaS Tracking Card Views
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_views_viz AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    os_platform,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card = true THEN app_package_deployed_uuid END) AS `viewed_paas_tracking_card`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform;

-- View for Viz_2: Clicks on Expand - PaaS Tracking Card
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_expand_clicks_viz AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    os_platform,
    COUNT(DISTINCT CASE WHEN is_clicked_aip_order_accordian = true THEN app_package_deployed_uuid END) AS `clicked_expand`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time),
    os_platform;

-- View for Viz_3: Clicks on Each Status - PaaS Tracking Card
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_status_clicks_viz AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation = true THEN app_package_deployed_uuid END) AS `clicked_order_confirmation`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing = true THEN app_package_deployed_uuid END) AS `clicked_order_processing`,
    COUNT(DISTINCT CASE WHEN is_clicked_track_delivery = true THEN app_package_deployed_uuid END) AS `clicked_track_delivery`,
    COUNT(DISTINCT CASE WHEN is_clicked_complete_setup = true THEN app_package_deployed_uuid END) AS `clicked_complete_setup`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time);

-- View for Viz_4: Delivered vs Onboarded and Support Cases
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_delivery_onboarding_viz AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS `delivered`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_aip_setup_complete = true THEN app_package_deployed_uuid END) AS `onboarded`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true AND is_oobe_support_session = true THEN app_package_deployed_uuid END) AS `support_cases`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time);

-- View for Viz_5: Tracking Card Status Pill Clicks
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_pill_clicks_viz AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_confirmation_pill = true THEN app_package_deployed_uuid END) AS `order_confirmed_pill`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_processing_pill = true THEN app_package_deployed_uuid END) AS `processing_pill`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_shipped_pill = true THEN app_package_deployed_uuid END) AS `shipped_pill`,
    COUNT(DISTINCT CASE WHEN is_clicked_order_delivered_pill = true THEN app_package_deployed_uuid END) AS `delivered_pill`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time);

-- View for Viz_6: Tracking Card Status
CREATE OR REPLACE VIEW team_css_analytics_prod.hpx_analytics.paas_tracking_card_status_viz AS
SELECT 
    DATE_TRUNC('month', session_start_date_time) AS `month_date`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_confirmed = true THEN app_package_deployed_uuid END) AS `confirmed`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_processing = true THEN device_app_package_deployed_uuid END) AS `processed`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_shipped = true THEN app_package_deployed_uuid END) AS `shipped`,
    COUNT(DISTINCT CASE WHEN is_viewed_aip_tracking_card_order_delivered = true THEN device_app_package_deployed_uuid END) AS `delivered`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('month', session_start_date_time);

-- =====================================================
-- END OF METRIC VIEWS
-- =====================================================