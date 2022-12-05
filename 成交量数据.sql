-- 成交量数据
drop table ods_a_stock_deal;
CREATE TABLE `ods_a_stock_deal`
(
    `id`           bigint COMMENT '主键ID',
    `market`       bigint COMMENT '市场',
    `code`         STRING COMMENT '股票代码',
    `prePrice`     decimal(10, 2) COMMENT '昨日收盘价',
    `current_time` STRING COMMENT '交易时间',
    `deal`         decimal(10, 2) COMMENT '成交价',
    `draw`         decimal(15, 0) COMMENT '手数/成交量',
    `dealNum`      decimal(15, 0) COMMENT '手数/成交量',
    `buyOrsale`    decimal(15, 0) COMMENT '1是卖2是买4是挂着'
) COMMENT '个股每日分时成交量详情表(不含历史)'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_deal'
;

-- load到表中
use dfdb;
load data inpath
    '/hive/warehouse/origin_db/df_a_stock_deal/2022-08-11'
    into table ods_a_stock_deal partition (dt = '2022-08-11');


select code, sum(if(buyOrsale=2,1,0)) as nums from ods_a_stock_deal group by code order by nums desc;


desc ods_a_stock_deal;