use dfdb;

set hive.execution.engine;
drop table ods_up_to_stop;

create table if not exists ods_up_to_stop
(
    `code`          STRING COMMENT '股票代码',
    `up_down_rate`  DECIMAL(10, 2) COMMENT '涨跌幅',
    `opening_price` DECIMAL(10, 2) COMMENT '今日开盘',
    `closing_price` DECIMAL(10, 2) COMMENT '今日收盘',
    `ds`            STRING COMMENT '交易时间',
    `rk`            BIGINT COMMENT '序号'
)
    COMMENT '涨停表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/ods/ods_up_to_stop'
    TBLPROPERTIES ('orc.compress' = 'snappy');
;

-- insert into ods_up_to_stop partition (dt='2022-10-26')
SELECT allS.*,
       row_number() over (partition by code order by ds) as rk
FROM (
         SELECT code
              , up_down_rate
              , opening_price
              , closing_price
              , CONCAT(year, '-', month_day) AS ds
         FROM ods_a_stock_history
         WHERE dt = '2022-08-04'
         UNION ALL
         SELECT code
              , up_down_rate
              , opening_price
              , current_price
              , ds
         FROM ods_a_stock_detail_day
     ) allS
;
select *
from ods_a_stock_detail_day
where dt = '2022-11-07';
drop table dwd_training_data;
create table if not exists dwd_training_data
(
    `code`  STRING COMMENT 'code',
    `tags`  BIGINT COMMENT 'tags',
    `kline` STRING COMMENT 'k线'
)
    COMMENT '短线k线表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/dwd/dwd_training_data'
    TBLPROPERTIES ('orc.compress' = 'snappy');
;

----------

use dfdb;
alter table dwd_training_data
    drop
        partition (dt = '2022-11-01');


------
select code, tags, kline
from (
         select code
              , tags
              , CONCAT_WS('|', COLLECT_LIST(CAST(round(up_down_rate) AS STRING))) AS kline
              , CONCAT_WS('?', COLLECT_LIST(CAST(no AS STRING)))                  AS nos
         from (
                  SELECT c.code
                       , b.tags
                       , row_number() over (partition by c.code,tags order by c.rk) as no
                       , c.up_down_rate
                  FROM (
                           SELECT *
                                , ROW_NUMBER() OVER (PARTITION BY code ORDER BY start_rk ) AS tags
                           FROM (
                                    SELECT code
                                         , rk - 12 AS start_rk
                                         , rk      AS end_rk
                                    FROM dwd_temp
                                    where ds >= '2012-01-01'
                                ) a
                           WHERE start_rk > 0
                       ) b
                           left JOIN (
                      SELECT code
                           , up_down_rate
                           , rk
                      FROM dwd_temp
                      where ds >= '2012-01-01'
                  ) c
                                     ON b.code = c.code
                                         AND c.rk >= start_rk
                                         AND c.rk <= end_rk
              ) temp
         group by code, tags
         having count(no) = 12
     ) result
;

select round(3.2);

select *
from dwd_training_data;

drop table dwd_temp;
create table if not exists dwd_temp
(
    `code`         STRING COMMENT 'code',
    `up_down_rate` decimal(10, 3) COMMENT '涨幅',
    `ds`           STRING COMMENT '日期',
    `rk`           bigint COMMENT '序号'
)
    COMMENT '训练中间表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/dwd/dwd_temp'
    TBLPROPERTIES ('orc.compress' = 'snappy');


desc dwd_stock_detail;

insert overwrite table dwd_temp partition (dt = '9999-12-31')
select code, up_down_rate, ds, row_number() over (partition by code order by ds) as rk
from (
         select code, up_down_rate, concat(year, '-', month_day) as ds
         from ods_a_stock_history
         where dt = '2022-08-04'
           and code in (select code from ods_a_stock_detail_day where dt = '2022-12-12' and board in (2, 6))
           and up_down_rate <> 0
           and year >= 2017
         union all
         select code, up_down_rate, ds
         from ods_a_stock_detail_day
         where up_down_rate <> 0
           and current_price > 0
           and board in (2, 6)
     ) a
;



select *
from dwd_temp;
------
insert into table dwd_training_data partition (dt = '2022-11-01')
select code, tags, kline
from (
         select code
              , tags
              , CONCAT_WS(',',
                          COLLECT_LIST(CAST(if(up_down_rate > 0, 1, if(up_down_rate = 0, 0, -1)) AS STRING))) AS kline
              , CONCAT_WS('?', COLLECT_LIST(CAST(no AS STRING)))                                              AS nos
         from (
                  SELECT b.code
                       , b.tags
                       , row_number() over (partition by b.code,tags order by c.rk) as no
                       , c.up_down_rate
                  FROM (
                           SELECT *
                                , ROW_NUMBER() OVER (PARTITION BY code ORDER BY start_rk ) AS tags
                           FROM (
                                    SELECT code
                                         , rk - 6 AS start_rk
                                         , rk     AS end_rk
                                    FROM dwd_temp
                                    where code = '603209'
                                ) a
                           WHERE start_rk > 0
                       ) b
                           inner JOIN (
                      SELECT code
                           , up_down_rate
                           , rk
                      FROM dwd_temp
                      where code = '603209'
                  ) c
                                      ON b.code = c.code
                                          AND c.rk >= start_rk
                                          AND c.rk <= end_rk
              ) temp
         group by code, tags
         having count(no) = 13
     ) result
;

select code, max(rk)
from dwd_temp
group by code;

---------------------------

select SUBSTRING_INDEX(kline, '|', 12), count(1) as num
from dwd_training_data
where SUBSTRING_INDEX(kline, '|', -1) > 0
group by SUBSTRING_INDEX(kline, '|', 12)
order by num desc;


------------------------------
select stock, up, down, round(up * 100 / (up + down), 2) as ratio
from (
         select SUBSTRING_INDEX(kline, ',', 7)                     as stock,
                sum(if(SUBSTRING_INDEX(kline, ',', -1) < 0, 1, 0)) as down,
                sum(if(SUBSTRING_INDEX(kline, ',', -1) > 0, 1, 0)) as up
         from dwd_training_data
         where dt = '2022-11-01'
         group by SUBSTRING_INDEX(kline, ',', 7)
     ) t1
where up > 50
order by ratio desc
;


------------------------连续5天
SELECT b.code, b.start_rk as g, c.up_down_rate, c.rk
FROM (
         SELECT *
         FROM (
                  SELECT code
                       , rk - 4 AS start_rk
                       , rk     AS end_rk
                  FROM dwd_temp
                  where code = '603209'
              ) a
         WHERE start_rk > 0
     ) b
         inner JOIN (
    SELECT code
         , up_down_rate
         , rk
    FROM dwd_temp
    where code = '603209'
) c
                    ON b.code = c.code
                        AND c.rk >= start_rk
                        AND c.rk <= end_rk
;
--------------------- 1周交易日
drop table dwd_train;
create table if not exists dwd_train
(
    `ups`           STRING COMMENT '涨幅列表',
    `monday_result` decimal(5, 2) COMMENT '周一结果',
    `week_result`   decimal(5, 2) COMMENT '一周结果'
)
    COMMENT '训练中间表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/dwd/dwd_train'
    TBLPROPERTIES ('orc.compress' = 'snappy');



insert overwrite table dwd_train partition (dt = '9999-12-31')
select *
from (
         select CONCAT_WS(',', COLLECT_LIST(CAST(round(up_down_rate, 1) AS STRING))) as ups
              , lead(sum(if(weekday = 1, round(up_down_rate), 0)))
                     over (partition by year,code order by weekofyear)               as monday_result
              , lead(round(sum(up_down_rate)))
                     over (partition by year,code order by weekofyear)               as week_result
         from (
                  select substr(ds, 1, 4) as `year`,
                         weekday,
                         weekofyear,
                         ds
                  from ods_calendar
                  where ds >= '2018-01-01'
                    and astatus = 1
              ) cal
                  left join (
             select code, up_down_rate, concat(year, '-', month_day) as ds
             from ods_a_stock_history
             where dt = '2022-08-04'
               and code in (select code from ods_a_stock_detail_day where dt = '2022-12-12' and board in (2, 6))
               and up_down_rate <> 0
             union all
             select code, up_down_rate, ds
             from ods_a_stock_detail_day
             where up_down_rate <> 0
               and current_price > 0
               and code in (select code from ods_a_stock_detail_day where dt = '2022-12-12' and board in (2, 6))
         ) b on cal.ds = b.ds
         where b.code is not null
         group by year, weekofyear, code
         having count(1) = 5
     ) result
where monday_result is not null
;


select *
from dwd_train
limit 100;


-- python /opt/module/datax/bin/datax.py -p"-Dexportdir=/hive/warehouse/df_db/dwd/dwd_train/dt=9999-12-31" /opt/module/datax/job/export/hdfs2csv.json