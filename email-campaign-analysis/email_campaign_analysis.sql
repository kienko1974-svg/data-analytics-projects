-- Goal:
-- Analyze email sending activity by account and month.
-- Calculate the share of messages sent to each account within a month
-- and identify the first and last send dates.

WITH date_month AS (
  -- Prepare message send dates and derive month information
  SELECT
    es.id_account AS account_id,
    es.id_message,
    es.sent_date,
    s.date,
    DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS sent_day,
    DATE_TRUNC(
      DATE_ADD(s.date, INTERVAL es.sent_date DAY),
      MONTH
    ) AS sent_month
  FROM `data-analytics-mate.DA.email_sent` es
  JOIN `data-analytics-mate.DA.account_session` acs
    ON es.id_account = acs.account_id
  JOIN `data-analytics-mate.DA.session` s
    ON acs.ga_session_id = s.ga_session_id
),

agg AS (
  -- Calculate metrics per account within each month
  SELECT
    sent_month,
    account_id,

    -- Share of messages sent to this account within the month
    ROUND(
      100 * COUNT(DISTINCT id_message)
      OVER (PARTITION BY sent_month, account_id)
      /
      COUNT(DISTINCT id_message)
      OVER (PARTITION BY sent_month),
      4
    ) AS sent_msg_percent_from_this_month,

    -- First and last message send dates for the account in the month
    MIN(sent_day) OVER (PARTITION BY sent_month, account_id) AS first_sent_day,
    MAX(sent_day) OVER (PARTITION BY sent_month, account_id) AS last_sent_day
  FROM date_month
)

-- Final result: one row per account per month
SELECT DISTINCT
  sent_month,
  account_id,
  sent_msg_percent_from_this_month,
  first_sent_day,
  last_sent_day
FROM agg
ORDER BY
  sent_month,
  account_id;
