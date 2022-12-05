use dfdb;
set hive.exec.mode.local.auto=true;

drop table ods_stock_inrecursive_index;
-- 非递归指标首日脚本
create table if not exists ods_stock_inrecursive_index
(
    `code`          STRING COMMENT '股票代码',
    `name`          STRING COMMENT '股票名称',
    `opening_price` decimal(10, 2) COMMENT '今日开盘',
    `closing_price` decimal(10, 2) COMMENT '今日收盘',
    `last_closing`  decimal(10, 2) COMMENT '昨日收盘',
    `closing_diff`  decimal(10, 2) COMMENT '今日涨额',
    `highest`       decimal(10, 5) COMMENT '最高价',
    `lowest`        decimal(10, 5) COMMENT '最低价',
    `ds`            STRING COMMENT '交易日',
    `deal_amount`   decimal(20, 5) COMMENT '成交额',
    `rk`            bigint COMMENT 'rk',
    `ma3`           decimal(20, 5) COMMENT 'ma3',
    `ma5`           decimal(20, 5) COMMENT 'ma5',
    `ma6`           decimal(20, 5) COMMENT 'ma5',
    `ma10`          decimal(20, 5) COMMENT 'ma10',
    `ma12`          decimal(20, 5) COMMENT 'ma12',
    `ma20`          decimal(20, 5) COMMENT 'ma20',
    `ma24`          decimal(20, 5) COMMENT 'ma24',
    `ma50`          decimal(20, 5) COMMENT 'ma50',
    `ma60`          decimal(20, 5) COMMENT 'ma60',
    `bbi`           decimal(20, 5) COMMENT 'bbi',
    `wr6`           decimal(20, 5) COMMENT 'wr6',
    `wr10`          decimal(20, 5) COMMENT 'wr10',
    `bias6`         decimal(20, 5) COMMENT 'bias6',
    `bias12`        decimal(20, 5) COMMENT 'bias12',
    `bias24`        decimal(20, 5) COMMENT 'bias24',
    `bias36`        decimal(20, 5) COMMENT 'bias36',
    `roc`           decimal(20, 5) COMMENT 'roc',
    `maroc`         decimal(20, 5) COMMENT 'maroc',
    `asi`           decimal(20, 5) COMMENT 'asi',
    `upper_ene`     decimal(20, 5) COMMENT 'upper_ene',
    `lower_ene`     decimal(20, 5) COMMENT 'lower_ene',
    `ene`           decimal(20, 5) COMMENT 'ene',
    `psy`           decimal(20, 5) COMMENT 'psy',
    `psyma`         decimal(20, 5) COMMENT 'psyma',
    `br`            decimal(20, 5) COMMENT 'br',
    `ar`            decimal(20, 5) COMMENT 'ar',
    `atr`           decimal(20, 5) COMMENT 'atr',
    `upperl`        decimal(20, 5) COMMENT 'upperl',
    `uppers`        decimal(20, 5) COMMENT 'uppers',
    `lowers`        decimal(20, 5) COMMENT 'lowers',
    `lowerl`        decimal(20, 5) COMMENT 'lowerl',
    `emv`           decimal(20, 5) COMMENT 'emv',
    `dpo`           decimal(20, 5) COMMENT 'dpo',
    `mtm`           decimal(20, 5) COMMENT 'mtm',
    `mtr`           decimal(20, 5) COMMENT 'mtr'
) COMMENT '东方财富A股非递归指标'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/ods/ods_stock_inrecursive_index'
    TBLPROPERTIES ('orc.compress' = 'snappy');

truncate table ods_stock_inrecursive_index;

insert into table ods_stock_inrecursive_index partition (dt = '2022-08-24')
SELECT code
     , name
     , opening_price
     , closing_price
     , last_closing
     , closing_price - last_closing                                                                    as closing_diff
     , highest
     , lowest
     , ds
     , deal_amount
     , rk
     , MA3
     , MA5
     , MA6
     , MA10
     , MA12
     , MA20
     , MA24
     , MA50
     , MA60
     , BBI
     , WR6
     , WR10
     , BIAS6
     , BIAS12
     , BIAS24
     , BIAS36
     , roc
     , maroc
     , SUM(16 * x / r * IF(aa > bb, aa, bb))
           OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW )             AS asi
     , upper_ene
     , lower_ene
     , ene
     , psy
     , psyma
     , br
     , ar
     , IF(ROW_NUMBER() OVER (partition by code ORDER BY ds ) >= 14,
          AVG(mtr) OVER (partition by code order by ds ROWS BETWEEN 13 PRECEDING AND CURRENT ROW ), 0) AS ATR
     , upperl
     , uppers
     , lowers
     , lowerl
     , emv
     , dpo
     , mtm
     , mtr
FROM (
         SELECT *
              , IF(rk >= 6, AVG(roc) OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ),
                   0)                                                                                            AS maroc
              , IF(aa > bb AND aa > cc, aa + bb / 2 + dd / 4,
                   IF(bb > cc AND bb > aa, bb + aa / 2 + dd / 4, cc + dd / 4))                                   AS r
              , closing_price - last_closing + (closing_price - opening_price) / 2 + last_closing - last_opening AS x
              , (upper + lower) / 2                                                                              AS ene
              , upper                                                                                            AS upper_ene
              , lower                                                                                            AS lower_ene
              , AVG(psy)
                    OVER (partition by code ORDER BY ds ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )               AS psyma
              , nvl(SUM(br1) OVER (partition by code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW ) * 100 /
                    SUM(br2) OVER (partition by code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW ), 0) AS br
              , nvl(SUM(ar1) OVER (partition by code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW ) * 100 /
                    SUM(ar2) OVER (partition by code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW ), 0) AS ar
              , IF(hsl > lh, IF(hsl > ll, hsl, ll), IF(lh > ll, lh, ll))                                         AS mtr
              , round(AVG(mid * volume * (highest - lowest) / if(h_l = 0, 1, h_l))
                          OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 13 PRECEDING AND CURRENT ROW ), 5)    AS emv
         FROM (
                  SELECT *
                       , IF(rk >= 24, (MA3 + MA6 + MA12 + MA24) / 4, 0)             AS BBI
                       , (highest6 - closing_price) * 100 / (highest6 - lowest6)    AS WR6
                       , (highest10 - closing_price) * 100 / (highest10 - lowest10) AS WR10
                       , (closing_price - avg6) * 100 / avg6                        AS BIAS6
                       , (closing_price - avg12) * 100 / avg12                      AS BIAS12
                       , (closing_price - avg24) * 100 / avg24                      AS BIAS24
                       , (closing_price - avg36) * 100 / avg36                      AS BIAS36
                       , (closing_price - pre_day) * 100 / pre_day                  AS roc
                       , abs(highest - last_closing)                                AS aa
                       , abs(lowest - last_closing)                                 AS bb
                       , abs(highest - last_low)                                    AS cc
                       , abs(last_closing - last_opening)                           AS dd
                       , (1 + 6 / 100) * c25                                        AS upper
                       , (1 - 6 / 100) * c25                                        AS lower
                       , SUM(IF(closing_price > last_closing, 1, 0))
                             OVER (partition by code ORDER BY ds ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) * 100 /
                         12                                                         AS psy
                       , IF(highest - last_closing > 0, highest - last_closing, 0)  AS br1
                       , IF(last_closing - lowest > 0, last_closing - lowest, 0)    AS br2
                       , highest - opening_price                                    AS ar1
                       , opening_price - lowest                                     AS ar2
                       , abs(last_closing - highest)                                AS lh
                       , abs(last_closing - lowest)                                 AS ll
                       , high30 * (1 + 15 / 100)                                    AS upperl
                       , high3 * (1 + 3 / 100)                                      AS uppers
                       , low3 * (1 - 3 / 100)                                       AS lowers
                       , low30 * (1 - 15 / 100)                                     AS lowerl
                       , 100 * (highest + lowest - last_hl) / (highest + lowest)    AS mid
                       , IF(rk > 30, closing_price - LAG(MA20, 11, MA20) OVER (PARTITION BY code ORDER BY ds ),
                            0)                                                      AS dpo
                       , closing_price - LAG(closing_price, 12, closing_price)
                                             OVER (PARTITION BY code ORDER BY ds )  AS mtm
                  FROM (
                           SELECT CODE
                                , NAME
                                , opening_price
                                , closing_price
                                , LAG(closing_price, 1, closing_price)
                                      OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) )                                           AS      last_closing
                                , highest
                                , lowest
                                , CONCAT(year, '-', month_day)                                                                                  AS      ds
                                , deal_amount
                                , ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) )                                  AS      rk
                                , LAG(highest, 1, 0)
                                      OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) )                                           AS      last_high -- 昨日最高
                                , LAG(lowest, 1, 0)
                                      OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) )                                           AS      last_low  -- 昨日最低
                                , abs(LAG(closing_price, 1, closing_price)
                                          OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) ) -
                                      lowest)                                                                                                   AS      csl
                                , abs(highest - LAG(closing_price, 1, closing_price)
                                                    OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) ))                            AS      hsc
                                , highest - lowest                                                                                              AS      hsl
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 3,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) /
                                     3,
                                     0)                                                                                                         AS      MA3
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 5,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 4 PRECEDING AND CURRENT ROW ) /
                                     5,
                                     0)                                                                                                         AS      MA5
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 6,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) /
                                     6,
                                     0)                                                                                                         AS      MA6
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 10,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) /
                                     10,
                                     0)                                                                                                         AS      MA10
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 12,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) /
                                     12,
                                     0)                                                                                                         AS      MA12
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 20,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) /
                                     20,
                                     0)                                                                                                         AS      MA20
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 24,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 23 PRECEDING AND CURRENT ROW ) /
                                     24,
                                     0)                                                                                                         AS      MA24
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 50,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 49 PRECEDING AND CURRENT ROW ) /
                                     50,
                                     0)                                                                                                         AS      MA50
                                , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ) >= 60,
                                     SUM(closing_price)
                                         OVER (PARTITION BY CODE ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 59 PRECEDING AND CURRENT ROW ) /
                                     60,
                                     0)                                                                                                         AS      MA60
                                , MAX(highest)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )  AS      highest6
                                , MIN(lowest)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )  AS      lowest6
                                , MAX(highest)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 9 PRECEDING AND CURRENT ROW )  AS      highest10
                                , MIN(lowest)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 9 PRECEDING AND CURRENT ROW )  AS      lowest10
                                , AVG(closing_price)
                                      OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )  AS      avg6
                                , AVG(closing_price)
                                      OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) AS      avg12
                                , AVG(closing_price)
                                      OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 23 PRECEDING AND CURRENT ROW ) AS      avg24
                                , AVG(closing_price)
                                      OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 35 PRECEDING AND CURRENT ROW ) AS      avg36
                                , FIRST_VALUE(closing_price)
                                              OVER (PARTITION BY code ORDER BY CONCAT(year, '-', month_day) ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) pre_day
                                , LAG(opening_price, 1, 0)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) )                                           AS      last_opening
                                , IF(ROW_NUMBER() OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ) >= 25,
                                     AVG(closing_price)
                                         OVER (partition by code order by CONCAT(YEAR, '-', month_day) ROWS BETWEEN 24 PRECEDING AND CURRENT ROW ),
                                     0)                                                                                                         AS      c25
                                , AVG(highest)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) AS      high30
                                , AVG(highest)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )  AS      high3
                                , AVG(lowest)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )  AS      low3
                                , AVG(lowest)
                                      OVER (PARTITION BY CODE ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) AS      low30
                                , AVG(deal_amount)
                                      OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 13 PRECEDING AND CURRENT ROW ) /
                                  deal_amount                                                                                                   AS      volume
                                , LAG(highest + lowest, 1, highest + lowest)
                                      OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) )                                           AS      last_hl
                                , AVG(highest - lowest)
                                      OVER (PARTITION BY code ORDER BY CONCAT(YEAR, '-', month_day) ROWS BETWEEN 13 PRECEDING AND CURRENT ROW ) AS      h_l
                           FROM `ods_a_stock_history`
                       ) t1
              ) t2
     ) t3
;


--

desc ods_a_stock_history;




















