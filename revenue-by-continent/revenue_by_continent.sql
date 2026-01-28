-- Goal:
-- Analyze revenue distribution by continent and device type (mobile vs desktop)
-- and compare revenue with user verification metrics.

WITH revenue_usd AS (
  -- Aggregate revenue and key metrics by continent
  SELECT 
      sp.continent,
      COUNT(sp.ga_session_id) AS session_cnt,
      COUNTIF(ac.is_verified = 1) AS verified_cnt,
      COUNT(acs.account_id) AS account_cnt,
      SUM(p.price) AS revenue,
      SUM(CASE WHEN sp.device = 'mobile' THEN p.price END) AS revenue_from_mobile,
      SUM(CASE WHEN sp.device = 'desktop' THEN p.price END) AS revenue_from_desktop
  FROM `data-analytics-mate.DA.order` o
  JOIN `data-analytics-mate.DA.product` p 
      ON o.item_id = p.item_id
  JOIN `data-analytics-mate.DA.session_params` sp 
      ON o.ga_session_id = sp.ga_session_id
  LEFT JOIN `data-analytics-mate.DA.account_session` acs 
      ON sp.ga_session_id = acs.ga_session_id
  LEFT JOIN `data-analytics-mate.DA.account` ac 
      ON acs.account_id = ac.id
  GROUP BY sp.continent
),

verification AS (
  -- Prepare user verification metrics by continent
  SELECT 
      sp.continent,
      COUNT(DISTINCT acs.account_id) AS account_cnt,
      COUNTIF(ac.is_verified = 1) AS verified_cnt,
      COUNT(sp.ga_session_id) AS session_cnt
  FROM `data-analytics-mate.DA.session_params` sp
  LEFT JOIN `data-analytics-mate.DA.account_session` acs 
      ON sp.ga_session_id = acs.ga_session_id
  LEFT JOIN `data-analytics-mate.DA.account` ac 
      ON acs.account_id = ac.id
  GROUP BY sp.continent
)

-- Final result: combine revenue and user metrics
SELECT 
       r.continent,
       r.revenue,
       r.revenue_from_mobile,
       r.revenue_from_desktop,
       ROUND(100 * r.revenue / SUM(r.revenue) OVER(), 4) AS revenue_share_percent,
       v.account_cnt,
       v.verified_cnt,
       v.session_cnt
FROM revenue_usd r
LEFT JOIN verification v 
  ON r.continent = v.continent;
