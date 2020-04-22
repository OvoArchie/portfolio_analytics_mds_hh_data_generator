WITH relevant_sites AS (
  SELECT mpan
  FROM UNNEST([{}]) mpan
)

, determine_live_status AS (
  SELECT rs.*,
         sp.ssd,
         sp.sld,
         IF(sp.sld IS NULL, 1, 0) is_live
  FROM relevant_sites rs
  LEFT JOIN (
             SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY mpan ORDER BY ssd DESC) AS latest_supply_period_is_1
             FROM `portfolio-analytics-nonprod.portfolio_reconciliation.tbl_elec_utiliserve_supply_periods`
            ) sp
    ON rs.mpan = sp.mpan
   AND sp.latest_supply_period_is_1 = 1
)

SELECT ls.*,
       hh
FROM determine_live_status ls
INNER JOIN `portfolio-analytics-nonprod.half_hourly_settlement.v_hh_power_materialised` hh
  ON ls.mpan = hh.mpan
 AND DATE(hh.start_time) BETWEEN ssd AND IFNULL(sld, CURRENT_DATE())
