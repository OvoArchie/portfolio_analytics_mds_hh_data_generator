WITH parameters AS (
  SELECT TIMESTAMP('{} 00:00:00') AS start_datetime,
         TIMESTAMP('{} 23:30:00') AS end_datetime
)

, settlement_periods AS (
  SELECT settlement_periods AS settlement_period_datetime,
         DATE(settlement_periods) AS settlement_date,
         TIME(settlement_periods) AS settlement_period_time,
         ROW_NUMBER() OVER(PARTITION BY DATE(settlement_periods)) AS settlement_period
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY((SELECT start_datetime FROM parameters), (SELECT end_datetime FROM parameters), INTERVAL 30 MINUTE)) settlement_periods
)


, give_sites_all_settlement_periods AS (
-- Doing it this way round to account for if a site ever has a day or period missing completely
  SELECT mpan,
         settlement_period_datetime,
         settlement_date,
         settlement_period_time,
         settlement_period
  FROM (
        SELECT DISTINCT mpan
        FROM `portfolio-analytics-nonprod.u_archie.tbl_mds_supplies_and_hh_data`
        WHERE is_live = 1
       )
  CROSS JOIN settlement_periods
)

, input AS (
  SELECT sp.*,
         md.hh.consumption,
         CASE WHEN md.hh.start_time IS NULL THEN TRUE ELSE FALSE END AS missing_period,
         ROW_NUMBER() OVER (PARTITION BY sp.mpan, sp.settlement_period_datetime ORDER BY hh.export_time DESC) AS latest_export_time_is_1
  FROM give_sites_all_settlement_periods sp
  CROSS JOIN parameters
  LEFT JOIN `portfolio-analytics-nonprod.u_archie.tbl_mds_supplies_and_hh_data` md
    ON sp.mpan = md.mpan
   AND sp.settlement_period_datetime = md.hh.start_time
   AND md.is_live = 1
   AND DATE(hh.start_time) BETWEEN DATE(start_datetime) AND DATE(end_datetime)
)

, formatting AS (
  SELECT CURRENT_TIMESTAMP() AS report_timestamp,
         CONCAT("H", FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP())) AS header,
         CONCAT("D",
                FORMAT_DATE('%Y%m%d', settlement_date),
                'PA',
                CAST(mpan AS string),
                REPEAT(" ", 200)) AS record,
         ARRAY_AGG(CASE WHEN missing_period
                        THEN "         "
                      ELSE FORMAT("%08.4fA", consumption)  -- The magic sauce
                   END
                   order by settlement_period ASC) AS output_format,
         CAST(SUM(FLOOR(consumption)) AS INT64) AS record_sum
  FROM input
  WHERE latest_export_time_is_1 = 1
  GROUP BY header, record
)

, total_records AS (
  SELECT FORMAT("%08d", COUNT(*) + 2) AS total_records,
         FORMAT("%012d", SUM(record_sum)) AS sum_floored_consumption
  FROM formatting
)

SELECT *
FROM formatting
CROSS JOIN total_records
