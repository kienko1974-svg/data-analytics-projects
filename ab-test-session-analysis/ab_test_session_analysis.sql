-- Goal:
-- Prepare a unified dataset for A/B test analysis at session level.
-- The dataset includes sessions, sessions with orders, and user events
-- to analyze conversion and user behavior across test groups.

WITH session_info AS (
   -- Base session-level data with A/B test attributes
   SELECT
       s.date,
       s.ga_session_id,
       sp.continent,
       sp.operating_system,
       sp.browser,
       sp.language,
       ab.test,
       ab.test_group
   FROM `DA.session` AS s
   JOIN `DA.ab_test` AS ab
     ON s.ga_session_id = ab.ga_session_id
   JOIN `DA.session_params` AS sp
     ON s.ga_session_id = sp.ga_session_id
),

session_with_orders AS (
  -- Sessions that resulted in at least one order (conversion proxy)
  SELECT
      si.date,
      si.ga_session_id,
      si.continent,
      si.operating_system,
      si.browser,
      si.language,
      si.test,
      si.test_group,
      COUNT(DISTINCT o.ga_session_id) AS value
  FROM session_info AS si
  LEFT JOIN `DA.order` AS o
    ON o.ga_session_id = si.ga_session_id
  GROUP BY
      si.date, si.ga_session_id, si.continent,
      si.operating_system, si.browser, si.language,
      si.test, si.test_group
),

all_sessions AS (
  -- All sessions (value = 1 per session, used as denominator)
  SELECT
      si.date,
      si.ga_session_id,
      si.continent,
      si.operating_system,
      si.browser,
      si.language,
      si.test,
      si.test_group,
      COUNT(DISTINCT si.ga_session_id) AS value
  FROM session_info AS si
  GROUP BY
      si.date, si.ga_session_id, si.continent,
      si.operating_system, si.browser, si.language,
      si.test, si.test_group
),

events_data AS (
  -- User events within sessions (behavioral metrics)
  SELECT
      si.date,
      si.ga_session_id,
      si.continent,
      si.operating_system,
      si.browser,
      si.language,
      si.test,
      si.test_group,
      ep.event_name,
      COUNT(ep.ga_session_id) AS value
  FROM `DA.event_params` AS ep
  JOIN session_info si
    ON ep.ga_session_id = si.ga_session_id
  GROUP BY
      si.date, si.ga_session_id, si.continent,
      si.operating_system, si.browser, si.language,
      si.test, si.test_group, ep.event_name
)

-- Final unified dataset
SELECT *, 'session_with_orders' AS metric_name
FROM session_with_orders

UNION ALL

SELECT *, 'session' AS metric_name
FROM all_sessions

UNION ALL

SELECT
    date,
    ga_session_id,
    continent,
    operating_system,
    browser,
    language,
    test,
    test_group,
    value,
    event_name AS metric_name
FROM events_data;
