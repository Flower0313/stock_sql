set hive.execution.engine=mr;
alter table dwd_a_stock_inrecursive_extend
drop
partition (dt = '2022-09-01');
create table if not exists dwd_a_stock_inrecursive_extend
(
    `rk`        bigint COMMENT 'rk',
    `code`      STRING COMMENT '股票代码',
    `cci`       decimal(10, 2) COMMENT 'CCI',
    `boll`      decimal(10, 2) COMMENT 'BOLL',
    `boll_up`   decimal(10, 2) COMMENT 'BOLLUP',
    `boll_down` decimal(10, 2) COMMENT 'BOLLDOWN',
    `ds`        string COMMENT '交易日'
) COMMENT '东方财富A股技术指标(扩展)'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/dwd/dwd_a_stock_inrecursive_extend'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 0、3、4、6、8
select substr(code, 1, 1), count(1) as nums
from ods_stock_inrecursive_index
group by substr(code, 1, 1);

set spark.dynamicAllocation.enabled =true;

set spark.shuffle.service.enabled=true;


-- 首次脚本执行
--CCI
SELECT t2.code
     , t2.ds
     , t2.rk
     , (t2.tp - t2.ma) / (avg(abs(t2.ma - t3.tp)) * 0.015) AS cci
FROM (
         SELECT *
              , SUM(tp) OVER (PARTITION BY CODE ORDER BY ds rows between 13 preceding and current row) / 14 AS ma
         FROM (
                  SELECT code
                       , ds
                       , (highest + closing_price + lowest) / 3 AS tp
                       , rk
                  FROM ods_stock_inrecursive_index
              ) t1
     ) t2
         LEFT JOIN (
    SELECT (highest + closing_price + lowest) / 3 AS tp
         , rk
         , code
         , closing_price
    FROM ods_stock_inrecursive_index
) t3
                   on t2.code = t3.code
                       and t2.rk <= t3.rk + 13
                       AND t2.rk >= t3.rk
                       AND t2.rk > 13
GROUP BY t2.ma
       , t2.code
       , t2.rk
       , t2.tp
       , t2.ds
;


-- BOLL
SELECT t1.rk
     , t1.ds
     , t1.code
     , t1.boll
     , boll + (2 * sqrt(AVG(power(t2.closing_price - t1.boll, 2)))) AS boll_up
     , boll - (2 * sqrt(AVG(power(t2.closing_price - t1.boll, 2)))) AS boll_down
FROM (
         SELECT rk
              , code
              , name
              , ds
              , closing_price
              , AVG(closing_price)
                    OVER (partition by code ORDER BY ds ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) AS boll
         FROM ods_stock_inrecursive_index
     ) t1
         LEFT JOIN (
    SELECT ROW_NUMBER() OVER (partition by code ORDER BY ds ) AS rk
         , closing_price
         , code
    FROM ods_stock_inrecursive_index
) t2 on t1.code = t2.code
    and t1.rk <= t2.rk + 19
    AND t1.rk >= t2.rk
    AND t1.rk > 19
GROUP BY t1.rk
       , t1.code
       , t1.boll
       , t1.ds
;



insert into table dwd_a_stock_inrecursive_extend partition (dt = '2022-08-26')
select cci.rk
     , cci.code
     , cci.cci
     , boll.boll
     , boll.boll_up
     , boll.boll_down
     , cci.ds
from (
         SELECT t2.code
              , t2.ds
              , t2.rk
              , (t2.tp - t2.ma) / (avg(abs(t2.ma - t3.tp)) * 0.015) AS cci
         FROM (
                  SELECT *
                       , SUM(tp) OVER (PARTITION BY CODE ORDER BY ds rows between 13 preceding and current row) /
                         14 AS ma
                  FROM (
                           SELECT code
                                , ds
                                , (highest + closing_price + lowest) / 3 AS tp
                                , rk
                           FROM ods_stock_inrecursive_index
                       ) t1
              ) t2
                  LEFT JOIN (
             SELECT (highest + closing_price + lowest) / 3 AS tp
                  , rk
                  , code
                  , closing_price
             FROM ods_stock_inrecursive_index
         ) t3
                            on t2.code = t3.code
                                and t2.rk <= t3.rk + 13
                                AND t2.rk >= t3.rk
                                AND t2.rk > 13
         GROUP BY t2.ma
                , t2.code
                , t2.rk
                , t2.tp
                , t2.ds
     ) cci
         inner join (
    SELECT t1.rk
         , t1.ds
         , t1.code
         , t1.boll
         , boll + (2 * sqrt(AVG(power(t2.closing_price - t1.boll, 2)))) AS boll_up
         , boll - (2 * sqrt(AVG(power(t2.closing_price - t1.boll, 2)))) AS boll_down
    FROM (
             SELECT rk
                  , code
                  , name
                  , ds
                  , closing_price
                  , AVG(closing_price)
                        OVER (partition by code ORDER BY ds ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) AS boll
             FROM ods_stock_inrecursive_index
         ) t1
             LEFT JOIN (
        SELECT ROW_NUMBER() OVER (partition by code ORDER BY ds ) AS rk
             , closing_price
             , code
        FROM ods_stock_inrecursive_index
    ) t2 on t1.code = t2.code
        and t1.rk <= t2.rk + 19
        AND t1.rk >= t2.rk
        AND t1.rk > 19
    GROUP BY t1.rk
           , t1.code
           , t1.boll
           , t1.ds
) boll on cci.code = boll.code
    and cci.rk = boll.rk;









-- 每日脚本执行
insert into table dwd_a_stock_inrecursive_extend partition (dt = '2022-09-01')
select cci.rk
     , cci.code
     , cci.cci
     , boll.boll
     , boll.boll_up
     , boll.boll_down
     , cci.ds
from (
         SELECT t2.code
              , t2.ds
              , t2.rk
              , (t2.tp - t2.ma) / (avg(abs(t2.ma - t3.tp)) * 0.015) AS cci
         FROM (
                  SELECT *
                       , SUM(tp) OVER (PARTITION BY CODE ORDER BY ds rows between 13 preceding and current row) /
                         14 AS ma
                  FROM (
                           SELECT code
                                , ds
                                , (highest + closing_price + lowest) / 3 AS tp
                                , rk
                           FROM ods_stock_inrecursive_index
                       ) t1 where ds='2022-09-01'
              ) t2
                  LEFT JOIN (
             SELECT (highest + closing_price + lowest) / 3 AS tp
                  , rk
                  , code
                  , closing_price
             FROM ods_stock_inrecursive_index
         ) t3
                            on t2.code = t3.code
                                and t2.rk <= t3.rk + 13
                                AND t2.rk >= t3.rk
                                AND t2.rk > 13
         GROUP BY t2.ma
                , t2.code
                , t2.rk
                , t2.tp
                , t2.ds
     ) cci
         inner join (
    SELECT t1.rk
         , t1.ds
         , t1.code
         , t1.boll
         , boll + (2 * sqrt(AVG(power(t2.closing_price - t1.boll, 2)))) AS boll_up
         , boll - (2 * sqrt(AVG(power(t2.closing_price - t1.boll, 2)))) AS boll_down
    FROM (
             SELECT rk
                  , code
                  , name
                  , ds
                  , closing_price
                  , AVG(closing_price)
                        OVER (partition by code ORDER BY ds ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) AS boll
             FROM ods_stock_inrecursive_index where ds='2022-09-01'
         ) t1
             LEFT JOIN (
        SELECT ROW_NUMBER() OVER (partition by code ORDER BY ds ) AS rk
             , closing_price
             , code
        FROM ods_stock_inrecursive_index
    ) t2 on t1.code = t2.code
        and t1.rk <= t2.rk + 19
        AND t1.rk >= t2.rk
        AND t1.rk > 19
    GROUP BY t1.rk
           , t1.code
           , t1.boll
           , t1.ds
) boll on cci.code = boll.code
    and cci.rk = boll.rk;

