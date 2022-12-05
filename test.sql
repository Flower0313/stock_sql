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
select * from ods_a_stock_detail_day where dt='2022-11-07';
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
         union all
         select code, up_down_rate, ds
         from ods_a_stock_detail_day
         where up_down_rate <> 0
     ) a
;


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
                                    where ds >= '2016-06-25'
                                ) a
                           WHERE start_rk > 0
                       ) b
                           inner JOIN (
                      SELECT code
                           , up_down_rate
                           , rk
                      FROM dwd_temp
                      where ds >= '2016-06-25'
                  ) c
                                      ON b.code = c.code
                                          AND c.rk >= start_rk
                                          AND c.rk <= end_rk
              ) temp
         group by code, tags
         having count(no) = 13
     ) result
;


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


------------------------
select distinct code
from dwd_training_data
where dt = '2022-11-01'
  and SUBSTRING_INDEX(kline, ',', 7) in ('1,-1,-1,-1,-1,1,1', '1,1,-1,1,-1,1,-1');



desc ods_a_stock_deal;

select distinct code
from ods_a_stock_deal
where dt = '2022-11-07'
  and draw >= 5000
  and buyorsale = 2
  and current_time >= '13:00:00';

select * from ods_a_stock_detail_day where dt='2022-11-07'

desc ods_a_stock_detail_day;

select count(1) from ods_stock_inrecursive_index where dt='2022-11-08';

select count(1) from ods_a_stock_detail_day where dt='2022-11-10';