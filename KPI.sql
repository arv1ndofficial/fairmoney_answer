-- ============================================
-- FairMoney KPI Queries (OLAP Reporting)
-- ============================================

-- 1. Portfolio at Risk (PAR-30)
SELECT
  dp.country_name,
  dp.product_name,
  SUM(CASE WHEN flp.days_past_due > 30 THEN flp.total_outstanding ELSE 0 END) AS risk_amount,
  SUM(flp.total_outstanding) AS total_portfolio,
  CASE 
    WHEN SUM(flp.total_outstanding) = 0 THEN 0
    ELSE ROUND(SUM(CASE WHEN flp.days_past_due > 30 THEN flp.total_outstanding ELSE 0 END) * 100.0 / SUM(flp.total_outstanding), 2)
  END AS par30_rate
FROM reporting.fact_loan_portfolio flp
JOIN reporting.dim_product dp ON flp.product_key = dp.product_key
WHERE flp.is_current = true
GROUP BY dp.country_name, dp.product_name;

-- 2. Application Conversion Rate
SELECT
  dp.product_name,
  COUNT(*) AS total_applications,
  SUM(CASE WHEN fla.is_disbursed THEN 1 ELSE 0 END) AS disbursed,
  CASE 
    WHEN COUNT(*) = 0 THEN 0
    ELSE ROUND(SUM(CASE WHEN fla.is_disbursed THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
  END AS conversion_rate
FROM reporting.fact_loan_applications fla
JOIN reporting.dim_product dp ON fla.product_key = dp.product_key
JOIN reporting.dim_date dd ON fla.application_date_key = dd.date_key
WHERE dd.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY dp.product_name;

-- 3. Payment Success Rate by Channel
SELECT
  dpm.payment_method,
  COUNT(*) AS total_attempts,
  SUM(CASE WHEN fp.is_successful THEN 1 ELSE 0 END) AS successful,
  CASE 
    WHEN COUNT(*) = 0 THEN 0
    ELSE ROUND(SUM(CASE WHEN fp.is_successful THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
  END AS success_rate
FROM reporting.fact_payments fp
JOIN reporting.dim_payment_method dpm ON fp.payment_method_key = dpm.payment_method_key
JOIN reporting.dim_date dd ON fp.payment_date_key = dd.date_key
WHERE dd.full_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dpm.payment_method;

-- 4. Collections Recovery Rate
SELECT
  dc.country_name,
  SUM(fc.outstanding_at_assignment) AS total_overdue,
  SUM(fc.amount_recovered) AS total_recovered,
  CASE 
    WHEN SUM(fc.outstanding_at_assignment) = 0 THEN 0
    ELSE ROUND(SUM(fc.amount_recovered) * 100.0 / SUM(fc.outstanding_at_assignment), 1)
  END AS recovery_rate
FROM reporting.fact_collections fc
JOIN reporting.dim_customer dc ON fc.customer_key = dc.customer_key
JOIN reporting.dim_date dd ON fc.assignment_date_key = dd.date_key
WHERE dd.full_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY dc.country_name;

-- 5. Customer Lifetime Value (CLV)
SELECT
  dc.registration_month,
  dc.customer_segment,
  COUNT(dc.customer_key) AS cohort_size,
  AVG(flp.interest_paid - flp.total_outstanding) AS avg_clv
FROM reporting.fact_loan_portfolio flp
JOIN reporting.dim_customer dc ON flp.customer_key = dc.customer_key
JOIN reporting.dim_date dd ON flp.date_key = dd.date_key
WHERE dc.registration_year >= 2024
GROUP BY dc.registration_month, dc.customer_segment;
