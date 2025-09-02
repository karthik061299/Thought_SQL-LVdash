-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card
-- Generated from TML files: paas_tracking_card.table.tml, PaaS_tracking_card_hpx.model.tml, PaaS Tracking Card.liveboard.tml
-- Target: Databricks SQL Environment
-- =====================================================

-- Base Table Reference
-- Source: team_css_analytics_prod.hpx_analytics.paas_tracking_card

-- =====================================================
-- METRIC VIEW 1: PaaS Tracking Card Base Metrics
-- =====================================================
CREATE OR REPLACE VIEW `paas_tracking_card_base_metrics` AS
SELECT 
    -- Dimension Fields
    `session_id`,
    `app_package_deployed_uuid`,
    `os_platform`,
    `app_name`,
    `app_package_id`,
    `app_version`,
    `session_start_date_time`,
    DATE(`session_start_date_time`) AS `date`,
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    `geo_country_code`,
    `is_hpid_signed_in`,
    `device_app_package_deployed_uuid`,
    `associated_device_session_id`,
    `aip_device_uuid`,
    `is_associated_device`,
    
    -- Boolean Attribute Fields
    `is_viewed_aip_tracking_card`,
    `is_viewed_aip_tracking_card_order_confirmed`,
    `is_viewed_aip_tracking_card_order_processing`,
    `is_viewed_aip_tracking_card_order_shipped`,
    `is_viewed_aip_tracking_card_order_delivered`,
    `is_clicked_aip_order_accordian`,
    `is_clicked_order_confirmation`,
    `is_clicked_order_processing`,
    `is_clicked_track_delivery`,
    `is_clicked_complete_setup`,
    `is_clicked_order_confirmation_pill`,
    `is_clicked_order_processing_pill`,
    `is_clicked_order_shipped_pill`,
    `is_clicked_order_delivered_pill`,
    `is_clicked_aip_order_accordian_order_processing`,
    `is_clicked_aip_order_accord`,
    `is_aip_setup_start`,
    `is_clicked_aip_order_accordian_order_confirmed`,
    `is_ows_start`,
    `is_clicked_aip_order_accordian_order_shipped`,
    `is_oobe_complete`,
    `is_aip_setup_complete`,
    `is_clicked_support`,
    `is_oobe_support_session`,
    
    -- Measure Fields (Counts)
    `total_printer_count`,
    `total_device_count`,
    `total_accessory_count`,
    `total_pc_count`,
    `max_total_printer_count`,
    `max_total_device_count`,
    `max_total_accessory_count`,
    `max_total_pc_count`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card;

-- =====================================================
-- METRIC VIEW 2: Calculated Measures from Model Formulas
-- =====================================================
CREATE OR REPLACE VIEW `paas_tracking_card_calculated_metrics` AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`,
    
    -- Viewed PaaS Tracking Card
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END)
    END AS `viewed_paas_tracking_card`,
    
    -- Clicked Expand
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_expand`,
    
    -- Clicked Order Confirmation
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_order_confirmation`,
    
    -- Clicked Order Processing
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_order_processing`,
    
    -- Clicked Track Delivery
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_track_delivery`,
    
    -- Clicked Complete Setup
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_complete_setup`,
    
    -- Confirmed Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END)
    END AS `confirmed`,
    
    -- Processed Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END)
    END AS `processed`,
    
    -- Shipped Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END)
    END AS `shipped`,
    
    -- Delivered Status
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END)
    END AS `delivered`,
    
    -- Onboarded (Delivered AND Setup Complete)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END)
    END AS `onboarded`,
    
    -- Support Cases (Delivered AND Support Session)
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END)
    END AS `support_cases`,
    
    -- Pill Click Metrics
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END)
    END AS `order_confirmed_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END)
    END AS `processing_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END)
    END AS `shipped_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END)
    END AS `delivered_pill`,
    
    -- Aggregated Counts
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
    DATE_TRUNC('MONTH', `session_start_date_time`),
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`;

-- =====================================================
-- METRIC VIEW 3: Liveboard Visualization 1 - PaaS Tracking Card Views
-- =====================================================
CREATE OR REPLACE VIEW `viz_paas_tracking_card_views` AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END)
    END AS `viewed_paas_tracking_card`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', `session_start_date_time`),
    `os_platform`;

-- =====================================================
-- METRIC VIEW 4: Liveboard Visualization 2 - Clicks on Expand
-- =====================================================
CREATE OR REPLACE VIEW `viz_clicks_on_expand` AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    `os_platform`,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_expand`
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', `session_start_date_time`),
    `os_platform`;

-- =====================================================
-- METRIC VIEW 5: Liveboard Visualization 3 - Clicks on Each Status
-- =====================================================
CREATE OR REPLACE VIEW `viz_clicks_on_each_status` AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_order_confirmation`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_order_processing`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_track_delivery`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END)
    END AS `clicked_complete_setup`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', `session_start_date_time`);

-- =====================================================
-- METRIC VIEW 6: Liveboard Visualization 4 - Delivered vs Onboarded and Support Cases
-- =====================================================
CREATE OR REPLACE VIEW `viz_delivered_onboarded_support` AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END)
    END AS `delivered`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END)
    END AS `onboarded`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END)
    END AS `support_cases`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', `session_start_date_time`);

-- =====================================================
-- METRIC VIEW 7: Liveboard Visualization 5 - Tracking Card Status Pill Clicks
-- =====================================================
CREATE OR REPLACE VIEW `viz_status_pill_clicks` AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END)
    END AS `order_confirmed_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END)
    END AS `processing_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END)
    END AS `shipped_pill`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END)
    END AS `delivered_pill`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', `session_start_date_time`);

-- =====================================================
-- METRIC VIEW 8: Liveboard Visualization 6 - Tracking Card Status
-- =====================================================
CREATE OR REPLACE VIEW `viz_tracking_card_status` AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END)
    END AS `confirmed`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END)
    END AS `processed`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END)
    END AS `shipped`,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) = 0 
        THEN NULL 
        ELSE COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END)
    END AS `delivered`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', `session_start_date_time`);

-- =====================================================
-- SUMMARY METRIC VIEW: All Key Metrics Combined
-- =====================================================
CREATE OR REPLACE VIEW `paas_tracking_card_summary_metrics` AS
SELECT 
    DATE_TRUNC('MONTH', `session_start_date_time`) AS `month_date`,
    DATE(`session_start_date_time`) AS `date`,
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`,
    
    -- Core Engagement Metrics
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card` = true THEN `app_package_deployed_uuid` END) AS `viewed_paas_tracking_card_count`,
    COUNT(DISTINCT CASE WHEN `is_clicked_aip_order_accordian` = true THEN `app_package_deployed_uuid` END) AS `clicked_expand_count`,
    
    -- Status View Metrics
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_confirmed` = true THEN `app_package_deployed_uuid` END) AS `confirmed_count`,
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_processing` = true THEN `device_app_package_deployed_uuid` END) AS `processed_count`,
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_shipped` = true THEN `app_package_deployed_uuid` END) AS `shipped_count`,
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true THEN `device_app_package_deployed_uuid` END) AS `delivered_count`,
    
    -- Action Click Metrics
    COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation` = true THEN `app_package_deployed_uuid` END) AS `clicked_order_confirmation_count`,
    COUNT(DISTINCT CASE WHEN `is_clicked_order_processing` = true THEN `app_package_deployed_uuid` END) AS `clicked_order_processing_count`,
    COUNT(DISTINCT CASE WHEN `is_clicked_track_delivery` = true THEN `app_package_deployed_uuid` END) AS `clicked_track_delivery_count`,
    COUNT(DISTINCT CASE WHEN `is_clicked_complete_setup` = true THEN `app_package_deployed_uuid` END) AS `clicked_complete_setup_count`,
    
    -- Pill Click Metrics
    COUNT(DISTINCT CASE WHEN `is_clicked_order_confirmation_pill` = true THEN `app_package_deployed_uuid` END) AS `order_confirmed_pill_count`,
    COUNT(DISTINCT CASE WHEN `is_clicked_order_processing_pill` = true THEN `app_package_deployed_uuid` END) AS `processing_pill_count`,
    COUNT(DISTINCT CASE WHEN `is_clicked_order_shipped_pill` = true THEN `app_package_deployed_uuid` END) AS `shipped_pill_count`,
    COUNT(DISTINCT CASE WHEN `is_clicked_order_delivered_pill` = true THEN `app_package_deployed_uuid` END) AS `delivered_pill_count`,
    
    -- Outcome Metrics
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_aip_setup_complete` = true THEN `app_package_deployed_uuid` END) AS `onboarded_count`,
    COUNT(DISTINCT CASE WHEN `is_viewed_aip_tracking_card_order_delivered` = true AND `is_oobe_support_session` = true THEN `app_package_deployed_uuid` END) AS `support_cases_count`,
    
    -- Device Count Aggregations
    SUM(`total_printer_count`) AS `total_printer_count_sum`,
    SUM(`total_device_count`) AS `total_device_count_sum`,
    SUM(`total_accessory_count`) AS `total_accessory_count_sum`,
    SUM(`total_pc_count`) AS `total_pc_count_sum`,
    
    -- Session Counts
    COUNT(DISTINCT `session_id`) AS `unique_sessions`,
    COUNT(DISTINCT `app_package_deployed_uuid`) AS `unique_apps`,
    COUNT(*) AS `total_records`
    
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
GROUP BY 
    DATE_TRUNC('MONTH', `session_start_date_time`),
    DATE(`session_start_date_time`),
    `os_platform`,
    `geo_country_code`,
    `app_version`,
    `is_hpid_signed_in`;

-- =====================================================
-- END OF METRIC VIEWS
-- =====================================================

-- Notes:
-- 1. All views use fully qualified table names: team_css_analytics_prod.hpx_analytics.paas_tracking_card
-- 2. All column aliases use backticks for Databricks compatibility
-- 3. Boolean logic follows ThoughtSpot TML formula patterns with CASE WHEN statements
-- 4. Date functions use Databricks SQL syntax (DATE_TRUNC, DATE)
-- 5. Unique count logic implemented with COUNT(DISTINCT CASE WHEN ... END)
-- 6. All aggregations include proper GROUP BY clauses
-- 7. NULL handling follows ThoughtSpot pattern (return NULL when count = 0)
-- 8. Views are environment-agnostic and ready for Databricks execution