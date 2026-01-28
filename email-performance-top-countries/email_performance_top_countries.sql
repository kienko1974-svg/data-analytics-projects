-- Goal:
-- Analyze email campaign performance and account activity by country.
-- Identify top countries by number of accounts and sent emails.

WITH account_metrics AS (
  -- Account-level metrics by date, country and user attributes
  SELECT
    s.date AS date,
    sp.country AS country,
    ac.send_interval AS send_interval,
    ac.is_verified,
    ac.is_unsubscribed,
    COUNT(DISTINCT ac.id) AS account_cnt,
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg
  FROM `data-analytics-mate.DA.account` ac
  JOIN `data-analytics-mate.DA.account_session` acs
    ON ac.id = acs.account_id
  JOIN `data-analytics-mate.DA.session` s
    ON acs.ga_session_id = s.ga_session_id
  JOIN `data-analytics-mate.DA.session_params` sp
    ON s.ga_session_id = sp.ga_session_id
  GROUP BY
    s.date, sp.country, ac.send_interval, ac.is_verified, ac.is_unsubscribed
),

email_metrics AS (
  -- Email sending, opening and visiting metrics
  SELECT
    DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
    sp.country AS country,
    ac.send_interval AS send_interval,
    ac.is_verified,
    ac.is_unsubscribed,
    0 AS account_cnt,
    COUNT(DISTINCT es.id_message) AS sent_msg,
    COUNT(DISTINCT eo.id_message) AS open_msg,
    COUNT(DISTINCT ev.id_message) AS visit_msg
  FROM `data-analytics-mate.DA.email_sent` es
  JOIN `data-analytics-mate.DA.account` ac
    ON es.id_account = ac.id
  JOIN `data-analytics-mate.DA.account_session` acs
    ON ac.id = acs.account_id
  JOIN `data-analytics-mate.DA.session` s
    ON acs.ga_session_id = s.ga_session_id
  JOIN `data-analytics-mate.DA.session_params` sp
    ON s.ga_session_id = sp.ga_session_id
  LEFT JOIN `data-analytics-mate.DA.email_open` eo
    ON es.id_message = eo.id_message
  LEFT JOIN `data-analytics-mate.DA.email_visit` ev
    ON es.id_message = ev.id_message
  GROUP BY
    date, sp.country, ac.send_interval, ac.is_verified, ac.is_unsubscribed
),

-- Combine account and email metrics
union_data AS (
  SELECT * FROM account_metrics
  UNION ALL
  SELECT * FROM email_metrics
),

-- Aggregate metrics by dimensions
aggregated_data AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg
  FROM union_data
  GROUP BY
    date, country, send_interval, is_verified, is_unsubscribed
),

-- Country-level totals
country_totals AS (
  SELECT
    country,
    SUM(account_cnt) AS total_country_account_cnt,
    SUM(sent_msg) AS total_country_sent_cnt
  FROM aggregated_data
  GROUP BY country
),

-- Rank countries by accounts and sent emails
ranked_countries AS (
  SELECT *,
    RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
    RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
  FROM country_totals
),

-- Select top 10 countries
top_countries AS (
  SELECT *
  FROM ranked_countries
  WHERE
    rank_total_country_sent_cnt <= 10
    AND rank_total_country_account_cnt <= 10
)

-- Final result
SELECT
  a.date,
  a.country,
  a.send_interval,
  a.is_verified,
  a.is_unsubscribed,
  a.account_cnt,
  a.sent_msg,
  a.open_msg,
  a.visit_msg,
  t.total_country_account_cnt,
  t.total_country_sent_cnt,
  t.rank_total_country_account_cnt,
  t.rank_total_country_sent_cnt
FROM aggregated_data a
JOIN top_countries t
  ON a.country = t.country
ORDER BY
  t.rank_total_country_sent_cnt,
  a.date;
