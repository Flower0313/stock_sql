use dfdb;
set hive.exec.mode.local.auto=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.execution.engine;
set mapreduce.map.memory.mb=4096;


//ods层不做历史分区，只用gzip压缩即可
//日历表
//origin_db中的文件格式是gzip的，我以为这样创建后不能读取，原来还可以读取,看样子hive天然支持读取gzip格式的文件
truncate table ods_calendar;
DROP TABLE IF EXISTS ods_calendar;

-- 股市日历表
create table if not exists ods_calendar
(
    `ds`                  STRING COMMENT '日期',
    `weekday`             STRING COMMENT '周几',
    `type`                STRING COMMENT '日期类型(0工作日，1休息日，2节假日)',
    `typeDes`             STRING COMMENT '日期描述',
    `chineseZodiac`       STRING COMMENT '生肖年',
    `dayOfYear`           BIGINT COMMENT '每年多少日',
    `weekOfYear`          BIGINT COMMENT '每年多少周',
    `indexWorkDayOfMonth` BIGINT COMMENT '返回当前月的第几个工作日，否则为0',
    `Astatus`             BIGINT COMMENT 'A股开盘状态',
    `HKstatus`            BIGINT COMMENT '港股开盘状态',
    `USAstatus`           BIGINT COMMENT '美股开盘状态'
) COMMENT '股市日历表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_calendar'
;

// 查询总数必须加上limit 1,因为这是hive的优化导致的
select count(*)
from ods_a_stock_history
limit 1;

select chineseZodiac, count(1) as cnt, sum(USAstatus) as usa, sum(HKstatus) as hk, sum(Astatus) as a
from ods_calendar
group by chineseZodiac;

select *
from ods_calendar
where date_format(`ds`, 'yyyy') = 2022;

-- 还需要load语句,直接将zip压缩格式load到ods/ods_calendar目录下就行
DROP TABLE IF EXISTS ods_a_stock_history;
create table if not exists ods_a_stock_history
(
    `id`             BIGINT COMMENT '编号',
    `market`         BIGINT COMMENT '市场',
    `code`           STRING COMMENT '股票代码',
    `name`           STRING COMMENT '股票名称',
    `up_down_rate`   DECIMAL(10, 2) COMMENT '涨跌幅',
    `up_down_amount` DECIMAL(16, 2) COMMENT '涨跌额',
    `turnover_rate`  DECIMAL(10, 2) COMMENT '换手率',
    `amplitude`      DECIMAL(10, 2) COMMENT '振幅',
    `highest`        DECIMAL(10, 2) COMMENT '最高价',
    `lowest`         DECIMAL(10, 2) COMMENT '最低价',
    `opening_price`  DECIMAL(10, 2) COMMENT '今日开盘',
    `closing_price`  DECIMAL(10, 2) COMMENT '今日收盘',
    `deal_amount`    DECIMAL(20, 5) COMMENT '成交量',
    `deal_vol`       DECIMAL(32, 10) COMMENT '成交额',
    `year`           BIGINT COMMENT '年份',
    `month_day`      STRING COMMENT '月日'
)
    COMMENT '东方财富股票表(含历史)'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_history'
;


select *
from ods_a_stock_history
where dt = '2022-08-31';

load data inpath
    '/hive/warehouse/origin_db/df_a_stock_day_kline'
    into table ods_a_stock_history partition (dt = '2022-08-31');


ALTER TABLE dwd_stock_detail
    DROP PARTITION (dt = '2022-10-31');


//开启动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 首日加载
insert into table ods_a_stock_history_list partition (dt)
select market,
       code,
       name,
       up_down_rate,
       up_down_amount,
       turnover_rate,
       amplitude,
       highest,
       lowest,
       opening_price,
       closing_price,
       deal_amount,
       deal_vol,
       year,
       month_day,
       dt
from ods_a_stock_history
;



insert into table ods_a_stock_detail_day partition (dt = '2022-08-31')
select a.id,
       a.market,
       a.code,
       a.name,
       closing_price   as current_price,
       a.up_down_rate,
       b.up_down_rate5,
       b.up_down_rate10,
       a.up_down_amount,
       a.turnover_rate,
       b.pe_ratio_d,
       a.amplitude,
       b.volume_ratio,
       a.highest,
       a.lowest,
       a.opening_price,
       b.current_price as t_1_price,
       b.total_market_v,
       b.circulation_market_v,
       b.price_to_b_ratio,
       b.increase_this_year,
       b.time_to_market,
       b.outer_disk,
       b.inner_disk,
       b.roe,
       b.total_share_capital,
       b.tradable_shares,
       b.total_revenue,
       b.total_revenue_r,
       b.gross_profit_margin,
       b.total_assets,
       b.debt_ratio,
       b.industry,
       b.regional_plate,
       b.profit,
       b.PE_ratio_s,
       b.ttm,
       b.net_assets,
       a.deal_vol,
       a.deal_amount,
       b.dealTradeStae,
       b.commission,
       b.net_margin,
       b.total_profit,
       b.net_assets_per_share,
       b.net_profit,
       b.net_profit_r,
       b.unearnings_per_share,
       b.main_inflow,
       b.main_inflow_ratio,
       b.Slarge_inflow,
       b.Slarge_inflow_ratio,
       b.large_inflow,
       b.large_inflow_ratio,
       b.mid_inflow,
       b.mid_inflow_ratio,
       b.small_inflow,
       b.small_inflow_ratio,
       '2022-08-31'    as ds
from (select * from ods_a_stock_history where dt = '2022-08-31') a
         inner join (select * from ods_a_stock_detail_day where dt = '2022-08-30') b
                    on a.code = b.code
;



select *
from ods_a_stock_history
where dt = '2022-08-31';



load data inpath
    '/hive/warehouse/origin_db/df_a_zero_stock_day_kline'
    into table ods_a_stock_history partition (dt = '2022-07-05');


-- 个股每日详情表
desc ods_a_stock_detail_day;
DROP TABLE IF EXISTS ods_a_stock_detail_day;
create table if not exists ods_a_stock_detail_day
(
    `id`                   BIGINT COMMENT '编号',
    `market`               BIGINT COMMENT '市场',
    `code`                 STRING COMMENT '股票代码',
    `name`                 STRING COMMENT '股票名称',
    `current_price`        DECIMAL(10, 2) COMMENT '今日收盘价',
    `up_down_rate`         DECIMAL(10, 2) COMMENT '涨跌幅',
    `up_down_rate5`        DECIMAL(10, 2) COMMENT '5日涨幅',
    `up_down_rate10`       DECIMAL(10, 2) COMMENT '10日涨幅',
    `up_down_amount`       DECIMAL(16, 2) COMMENT '涨跌额',
    `turnover_rate`        DECIMAL(5, 2) COMMENT '换手率', -- 成交量/流通股本
    `PE_ratio_d`           DECIMAL(32, 2) COMMENT '市盈率(动态)',
    `amplitude`            DECIMAL(10, 2) COMMENT '振幅',
    `volume_ratio`         DECIMAL(10, 2) COMMENT '量比',
    `highest`              DECIMAL(10, 2) COMMENT '最高价',
    `lowest`               DECIMAL(10, 2) COMMENT '最低价',
    `opening_price`        DECIMAL(10, 2) COMMENT '今日开盘',
    `t_1_price`            DECIMAL(10, 2) COMMENT '昨日收盘',
    `total_market_v`       DECIMAL(32, 5) COMMENT '总市值',
    `circulation_market_v` DECIMAL(32, 5) COMMENT '流通市值',
    `price_to_b_ratio`     DECIMAL(32, 5) COMMENT '市净率', -- 每股市价/每股净资产
    `increase_this_year`   DECIMAL(10, 2) COMMENT '今年涨幅',
    `time_to_market`       BIGINT COMMENT '上市时间',
    `outer_disk`           DECIMAL(32, 2) COMMENT '外盘',
    `inner_disk`           DECIMAL(32, 2) COMMENT '内盘',
    `roe`                  DECIMAL(20, 3) COMMENT 'ROE加权净资产收益率',
    `total_share_capital`  DECIMAL(32, 3) COMMENT '总股本',
    `tradable_shares`      DECIMAL(32, 3) COMMENT '流通A股',
    `total_revenue`        DECIMAL(32, 5) COMMENT '总营收',
    `total_revenue_r`      DECIMAL(32, 5) COMMENT '总营收同比',
    `gross_profit_margin`  DECIMAL(32, 10) COMMENT '毛利率',
    `total_assets`         DECIMAL(32, 10) COMMENT '总资产',
    `debt_ratio`           DECIMAL(32, 10) COMMENT '负债率',
    `industry`             STRING COMMENT '行业',
    `regional_plate`       STRING COMMENT '地区板块',
    `profit`               DECIMAL(32, 10) COMMENT '收益',
    `PE_ratio_s`           DECIMAL(32, 2) COMMENT '市盈率(静态)',
    `ttm`                  DECIMAL(32, 2) COMMENT '市盈率(TTM)',
    `net_assets`           DECIMAL(32, 10) COMMENT '净资产',
    `deal_amount`          DECIMAL(20, 5) COMMENT '成交额',
    `deal_vol`             DECIMAL(20, 5) COMMENT '成交量',
    `dealTradeStae`        BIGINT COMMENT '交易状态',
    `commission`           DECIMAL(5, 2) COMMENT '委比',
    `net_margin`           DECIMAL(32, 5) COMMENT '净利率',
    `total_profit`         decimal(32, 5) COMMENT '总利润',
    `net_assets_per_share` decimal(32, 5) COMMENT '每股净资产',
    `net_profit`           decimal(32, 5) COMMENT '净利润',
    `net_profit_r`         decimal(32, 5) COMMENT '净利润同比',
    `unearnings_per_share` decimal(32, 5) COMMENT '每股未分配利润',
    `main_inflow`          decimal(32, 5) COMMENT '主力净流入',
    `main_inflow_ratio`    decimal(20, 2) COMMENT '主力比',
    `Slarge_inflow`        decimal(32, 5) COMMENT '超大单净流入',
    `Slarge_inflow_ratio`  decimal(20, 2) COMMENT '超大单净比',
    `large_inflow`         decimal(32, 5) COMMENT '大单净流入',
    `large_inflow_ratio`   decimal(20, 2) COMMENT '大单净比',
    `mid_inflow`           decimal(32, 5) COMMENT '中单净流入',
    `mid_inflow_ratio`     decimal(20, 2) COMMENT '中单净比',
    `small_inflow`         decimal(32, 5) COMMENT '小单净流入',
    `small_inflow_ratio`   decimal(20, 2) COMMENT '小单净比',
    `ds`                   STRING COMMENT '交易时间'
)
    COMMENT '个股每日详情表(不含历史)'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_detail_day'
;

show tables;
-- 每日加载，记得开启动态加载
-- 先用datax将每日数据送到<origin_db/df_a_stock_detail/日期>的目录下，然后使用load命令加载分区数据
-- 这都可以写在脚本中
load data inpath
    '/hive/warehouse/origin_db/df_a_stock_detail/2022-08-11'
    into table ods_a_stock_detail_day partition (dt = '2022-08-11');



select net_assets_per_share
from ods_a_stock_detail_day
where code = '301266';

show partitions ods_a_stock_detail_day;

//开启动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert into ods_a_stock_history_list partition (dt)
select market
     , code
     , name
     , 0                        as dk_total
     , up_down_rate
     , up_down_amount
     , turnover_rate
     , amplitude
     , highest
     , lowest
     , opening_price
     , current_price
     , deal_amount
     , deal_vol
     , DATE_FORMAT(ds, 'yyyy')  as year
     , DATE_FORMAT(ds, 'MM-dd') as month_day
     , ds
from ods_a_stock_detail_day
where ds = DATE_FORMAT(`current_date`(), 'yyyy-MM-dd');


drop table ods_stock_step_one;
CREATE TABLE `ods_stock_step_one`
(
    `code`          STRING COMMENT '股票代码',
    `name`          STRING COMMENT '股票名称',
    `closing_price` DECIMAL(10, 2) COMMENT '今日收盘',
    `last_closing`  DECIMAL(10, 2) COMMENT '昨日收盘',
    `highest`       DECIMAL(10, 5) COMMENT '最高价',
    `lowest`        DECIMAL(10, 5) COMMENT '最低价',
    `ds`            STRING COMMENT '交易日',
    `deal_amount`   DECIMAL(20, 5) COMMENT '成交额',
    `closing_diff`  DECIMAL(20, 5) COMMENT '差额',
    `rk`            bigint COMMENT 'rk',
    `x`             DECIMAL(10, 2) COMMENT 'hhv',
    `i`             DECIMAL(10, 2) COMMENT 'llv',
    `rsv`           DECIMAL(32, 10) COMMENT 'rsv',
    `sar_high`      DECIMAL(20, 5) COMMENT 'sar_high',
    `sar_low`       DECIMAL(20, 5) COMMENT 'sar_low',
    `tr`            DECIMAL(20, 5) COMMENT 'tr',
    `dmp`           DECIMAL(20, 5) COMMENT 'dmp',
    `dmm`           DECIMAL(20, 5) COMMENT 'dmm'
) COMMENT '第一步股票表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/ods/ods_stock_step_one'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日脚本,插入到ods_stock_step_one
insert into ods_stock_step_one partition (dt = '2022-07-08')
select code
     , name
     , closing_price
     , last_closing
     , highest
     , lowest
     , `date`                        as ds
     , deal_amount
     , closing_diff
     , rk
     , hhv
     , llv
     , rsv
     , sar_high
     , sar_low
     , tr
     , if(hd > 0 and hd > ld, hd, 0) as dmp
     , if(ld > 0 and ld > hd, ld, 0) as dmm
from (
         select *
              , nvl(if(hhv != llv, (closing_price - llv) * 100 / (hhv - llv), 0), 0)                      as rsv
              , if(rk <= 4,
                   max(highest) over (partition by code order by `date` rows between 3 preceding and current row),
                   max(highest)
                       over (partition by code order by `date` rows between 1 preceding and current row)) as sar_high
              , if(rk <= 4,
                   min(lowest) over (partition by code order by `date` rows between 3 preceding and current row),
                   min(lowest)
                       over (partition by code order by `date` rows between 1 preceding and current row)) as sar_low
              , if(csl > hsc, if(csl > hsl, csl, hsl), if(hsc > hsl, hsc, hsl))                           as tr
              , if(rk = 1, 0, highest - last_high)                                                        as hd
              , if(rk = 1, 0, last_low - lowest)                                                          as ld
         from (
                  SELECT CODE
                       , NAME
                       , closing_price
                       , lag(closing_price, 1, closing_price)
                             over (partition by code order by concat(year, '-', month_day))                                          as last_closing
                       , highest
                       , lowest
                       , concat(year, '-', month_day)                                                                                as `date`
                       , deal_amount
                       , closing_price - lag(closing_price, 1, closing_price)
                                             over ( PARTITION BY CODE ORDER BY concat(YEAR, '-', month_day))                         AS closing_diff
                       , row_number()
                          over (PARTITION BY CODE ORDER BY concat(YEAR, '-', month_day))                                             AS rk
                       , max(highest)
                             over (partition by code order by concat(year, '-', month_day) rows between 8 preceding and current row) as hhv
                       , min(lowest)
                             over (partition by code order by concat(year, '-', month_day) rows between 8 preceding and current row) as llv
                       , lag(highest, 1, 0)
                             over (partition by code order by concat(year, '-', month_day))                                          as last_high
                       , lag(lowest, 1, 0)
                             over (partition by code order by concat(year, '-', month_day))                                          as last_low
                       , abs(lag(closing_price, 1, closing_price)
                                 over (partition by code order by concat(year, '-', month_day)) -
                             lowest)                                                                                                 as csl
                       , abs(highest - lag(closing_price, 1, closing_price)
                                           over (partition by code order by concat(year, '-', month_day)))                           as hsc
                       , highest - lowest                                                                                            as hsl
                  FROM `ods_a_stock_history_list`) t1) t2
;


-- 分时数据
CREATE TABLE `ods_a_stock_hour_kline`
(
    `id`            bigint COMMENT '主键ID',
    `market`        bigint COMMENT '市场',
    `type`          bigint NOT NULL COMMENT '股票类型',
    `code`          STRING COMMENT '股票代码',
    `name`          STRING COMMENT '股票名称f3',
    `trendsTotal`   decimal(10, 2) COMMENT 'trendsTotal',
    `preClose`      decimal(10, 2) COMMENT '昨日收盘价',
    `trends`        STRING COMMENT '分时时间',
    `current_price` decimal(10, 2) COMMENT '当前价格f53',
    `current_high`  decimal(10, 2) COMMENT '当前最高价f54',
    `current_low`   decimal(10, 2) COMMENT '当前最低价f55',
    `deal_amount`   decimal(20, 5) COMMENT '成交额',
    `deal_vol`      decimal(32, 10) COMMENT '成交量'
) COMMENT '个股每日分时详情表(不含历史)'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_hour_kline'
;

use dfdb;
load data inpath
    '/hive/warehouse/origin_db/df_a_stock_hour_detail/2022-08-11'
    into table ods_a_stock_hour_kline partition (dt = '2022-08-11');



select count(1)
from ods_a_stock_detail_day
where dt = '2022-09-01'
  and current_price <> 0;



select * from dwd_temp where dt='2022-11-01' and code='301266' and ds>'2012-10-01';



-------------

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
                           where ds >= '2014-01-01'
                             and code = '301266'
                       ) a
                  WHERE start_rk > 0
              ) b
                  left JOIN (
             SELECT code
                  , up_down_rate
                  , rk
             FROM dwd_temp
             where ds >= '2014-01-01'
               and code = '301266'
         ) c
                            ON b.code = c.code
                                AND c.rk >= start_rk
                                AND c.rk <= end_rk
     ) temp
group by code, tags having count(no)=13;

-- 新增字段,最后新增在最后面
ALTER TABLE ods_a_stock_detail_day
    DROP PARTITION (dt = '2022-11-08');

-- 新增board字段
--alter table ods_a_stock_detail_day add columns (board string COMMENT '板块') CASCADE;
-- 移动board字段
--alter table ods_a_stock_detail_day change column board board string after small_inflow_ratio;
desc ods_a_stock_detail_day;



