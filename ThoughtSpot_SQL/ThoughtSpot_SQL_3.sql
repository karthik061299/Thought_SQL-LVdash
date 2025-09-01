-- =====================================================
-- ThoughtSpot SQL Metric Views for Databricks
-- Generated from PaaS Tracking Card TML Files
-- Version: 3.0 - Simplified for initial testing
-- =====================================================

-- Test basic connectivity and table access
SELECT COUNT(*) as row_count 
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card 
LIMIT 10;