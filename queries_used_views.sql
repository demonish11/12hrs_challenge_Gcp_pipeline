-- View 1: member_insights
-- This view gives transaction summary and free challenge activity per learner

CREATE OR REPLACE VIEW `nifty-matrix-464708-j2.report_datasets.learner_transaction_summary` AS
SELECT
  learnerobjectid,
  purchasetype AS product_type,
  COUNT(*) AS total_transactions,
  COUNTIF(amount_paid > 0) AS paid_transactions,
  COUNTIF(amount_paid = 0 OR amount_paid IS NULL) AS unpaid_transactions,
  SUM(amount_paid) AS total_amount_spent,
  ARRAY_AGG(DISTINCT status IGNORE NULLS) AS transaction_statuses,
  MIN(transactioncreatedat) AS first_transaction,
  MAX(transactioncreatedat) AS last_transaction
FROM `nifty-matrix-464708-j2.staging.raw_transaction`
GROUP BY learnerobjectid, product_type;



CREATE OR REPLACE VIEW `nifty-matrix-464708-j2.report_datasets.learner_free_product_actions` AS
SELECT
  cp.learnerobjectid,
  COUNT(DISTINCT cp.programobjectid) AS free_challenges_joined,
  ARRAY_AGG(DISTINCT c.title IGNORE NULLS) AS challenge_titles,
  MIN(cp.joined_date) AS first_joined_date,
  MAX(cp.joined_date) AS last_joined_date
FROM `nifty-matrix-464708-j2.staging.challenge_participants` cp
JOIN `nifty-matrix-464708-j2.staging.challenge` c
  ON cp.programobjectid = c._id
WHERE c.minamount = 0
GROUP BY cp.learnerobjectid;






-- View 2: product_contribution_summary
-- This view summarizes how product types contribute to revenue over time

CREATE OR REPLACE VIEW `nifty-matrix-464708-j2.report_datasets.product_type_contribution_monthly` AS
SELECT
  EXTRACT(YEAR FROM transactioncreatedat) AS year,
  EXTRACT(MONTH FROM transactioncreatedat) AS month,
  purchasetype AS product_type,
  
  COUNT(*) AS total_product_actions,
  COUNTIF(amount_paid > 0) AS paid_product_actions,
  COUNTIF(amount_paid = 0 OR amount_paid IS NULL) AS free_product_actions,
  
  ROUND(SAFE_DIVIDE(COUNTIF(amount_paid > 0), COUNT(*)) * 100, 2) AS paid_percentage,
  ROUND(SAFE_DIVIDE(COUNTIF(amount_paid = 0 OR amount_paid IS NULL), COUNT(*)) * 100, 2) AS free_percentage
FROM `nifty-matrix-464708-j2.staging.raw_transaction`
GROUP BY year, month, product_type
ORDER BY year, month, product_type;


CREATE OR REPLACE VIEW `nifty-matrix-464708-j2.report_datasets.product_contribution_summary` AS
SELECT
  EXTRACT(YEAR FROM transactioncreatedat) AS year,
  EXTRACT(MONTH FROM transactioncreatedat) AS month,
  purchasetype AS product_type,
  COUNT(*) AS total_transactions,
  COUNTIF(amount_paid > 0) AS paid_count,
  COUNTIF(amount_paid = 0) AS free_count,
  SUM(amount_paid) AS total_revenue,
  SAFE_DIVIDE(SUM(amount_paid), SUM(SUM(amount_paid)) OVER ()) * 100 AS revenue_percent
FROM `nifty-matrix-464708-j2.staging.raw_transaction`
GROUP BY year, month, product_type;
