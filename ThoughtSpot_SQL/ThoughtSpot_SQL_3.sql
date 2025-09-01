-- =====================================================
-- ThoughtSpot SQL Metric Views for PaaS Tracking Card - Version 3
-- Simplified version for testing and validation
-- Target: Databricks SQL
-- =====================================================

-- Test the base table access first
SELECT COUNT(*) as record_count
FROM team_css_analytics_prod.hpx_analytics.paas_tracking_card
LIMIT 1;