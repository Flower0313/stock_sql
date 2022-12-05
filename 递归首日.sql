CREATE TABLE `ods_stock_step_one`
(
    `code`          STRING COMMENT '股票代码',
    `name`          STRING COMMENT '股票名称',
    `opening_price` decimal(10, 2) COMMENT '今日开盘',
    `closing_price` decimal(10, 2) COMMENT '今日收盘',
    `last_closing`  decimal(10, 2) COMMENT '昨日收盘',
    `highest`       decimal(10, 5) COMMENT '最高价',
    `lowest`        decimal(10, 5) COMMENT '最低价',
    `ds`            STRING COMMENT '交易日',
    `deal_amount`   decimal(20, 5) COMMENT '成交额',
    `closing_diff`  decimal(20, 5) COMMENT '差额',
    `rk`            bigint COMMENT 'rk',
    `hhv`           decimal(10, 2) COMMENT 'hhv',
    `llv`           decimal(10, 2) COMMENT 'llv',
    `rsv`           decimal(32, 10) COMMENT 'rsv',
    `sar_high`      decimal(20, 5) COMMENT 'sar_high',
    `sar_low`       decimal(20, 5) COMMENT 'sar_low',
    `tr`            decimal(20, 5) COMMENT 'tr',
    `dmp`           decimal(20, 5) COMMENT 'dmp',
    `dmm`           decimal(20, 5) COMMENT 'dmm',
    `hlc`           decimal(20, 5) COMMENT 'hlc',
    `highest9`      decimal(20, 5) COMMENT 'highest9',
    `lowest9`       decimal(20, 5) COMMENT 'lowest9',
    `hhv10`         decimal(20, 5) COMMENT 'hhv10',
    `llv10`         decimal(20, 5) COMMENT 'llv10'
) COMMENT '东方财富A股递归指标中间表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/ods/ods_stock_step_one'
    TBLPROPERTIES ('orc.compress' = 'snappy');

show tables;



truncate table ods_stock_step_one;
insert into table ods_stock_step_one partition (dt = '2022-08-04')
SELECT code
     , name
     , opening_price
     , closing_price
     , nvl(last_closing, 0)
     , highest
     , lowest
     , ds
     , deal_amount
     , nvl(closing_diff, 0)
     , rk
     , nvl(hhv, 0)
     , nvl(llv, 0)
     , nvl(rsv, 0)
     , nvl(sar_high, 0)
     , nvl(sar_low, 0)
     , tr
     , nvl(IF(hd > 0 AND hd > ld, hd, 0), 0) AS dmp
     , nvl(IF(ld > 0 AND ld > hd, ld, 0), 0) AS dmm
     , nvl(hlc, 0)
     , nvl(highest9, 0)
     , nvl(lowest9, 0)
     , nvl(hhv10, 0)
     , nvl(llv10, 0)
FROM (
         SELECT *
              , nvl(IF(hhv != llv, (closing_price - llv) * 100 / (hhv - llv), 0), 0)                   AS rsv
              , IF(rk <= 4, MAX(highest) OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ),
                   MAX(highest)
                       OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 1 PRECEDING AND CURRENT ROW )) AS sar_high
              , IF(rk <= 4, MIN(lowest) OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ),
                   MIN(lowest)
                       OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 1 PRECEDING AND CURRENT ROW )) AS sar_low
              , IF(csl > hsc, IF(csl > hsl, csl, hsl), IF(hsc > hsl, hsc, hsl))                        AS tr
              , IF(rk = 1, 0, highest - last_high)                                                     AS hd
              , IF(rk = 1, 0, last_low - lowest)                                                       AS ld
              , LAG(tmp_hlc, 1, tmp_hlc) OVER (PARTITION BY code ORDER BY ds )                         AS hlc
         FROM (
                  SELECT CODE
                       , NAME
                       , opening_price
                       , closing_price
                       , LAG(closing_price, 1, closing_price)
                             OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) )                                          AS last_closing
                       , highest
                       , lowest
                       , CONCAT(year, '-', month_day)                                                                                 AS ds
                       , deal_amount
                       , closing_price - LAG(closing_price, 1, closing_price)
                                             OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) )                          AS closing_diff
                       , ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) )                                 AS rk
                       , MAX(highest)
                             OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 8 PRECEDING AND CURRENT ROW ) AS hhv
                       , MIN(lowest)
                             OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 8 PRECEDING AND CURRENT ROW ) AS llv
                       , LAG(highest, 1, 0)
                             OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) )                                          AS last_high -- 昨日最高
                       , LAG(lowest, 1, 0)
                             OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) )                                          AS last_low  -- 昨日最低
                       , highest - lowest                                                                                             AS hsl
                       , abs(LAG(closing_price, 1, closing_price)
                                 OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) ) -
                             lowest)                                                                                                  AS csl
                       , abs(highest - LAG(closing_price, 1, closing_price)
                                           OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) ))                           AS hsc
                       , MAX(highest)
                             OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 8 PRECEDING AND CURRENT ROW ) AS highest9
                       , MIN(lowest)
                             OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 8 PRECEDING AND CURRENT ROW ) AS lowest9
                       , LAG(opening_price, 1, 0)
                             OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) )                                          AS last_opening
                       , AVG((highest + lowest + closing_price) / 3)
                             OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS tmp_hlc
                       , MAX(highest)
                             OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS hhv10
                       , MIN(lowest)
                             OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS llv10
                  FROM `ods_a_stock_history`
              ) t1
     ) t3
;


