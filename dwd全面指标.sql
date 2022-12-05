select substr(code, 1, 1), count(1) as nums
from ods_stock_inrecursive_index
group by substr(code, 1, 1);


set hive.auto.convert.join;
set hive.mapjoin.smalltable.filesize;
set hive.cbo.enable = true;
set hive.compute.query.using.stats = true;
set hive.stats.fetch.column.stats = true;
set hive.stats.fetch.partition.stats = true;
set mapreduce.job.reduces;

drop table dwd_stock_detail;
create table if not exists dwd_stock_detail
(
    `rk`            bigint COMMENT 'rk',
    `code`          STRING COMMENT '股票代码',
    `name`          STRING COMMENT '股票名称',
    `closing_price` decimal(10, 2) COMMENT '今日收盘',
    `closing_diff`  decimal(10, 2) COMMENT '今日涨额',
    `deal_amount`   decimal(20, 5) COMMENT '成交额',
    `highest`       decimal(10, 5) COMMENT '最高价',
    `lowest`        decimal(10, 5) COMMENT '最低价',
    `opening_price` decimal(10, 2) COMMENT '今日开盘',
    `asi`           decimal(20, 5) COMMENT 'asi',
    `bbi`           decimal(20, 5) COMMENT 'bbi',
    `br`            decimal(20, 5) COMMENT 'br',
    `ar`            decimal(20, 5) COMMENT 'ar',
    `ma3`           decimal(20, 5) COMMENT 'ma3',
    `ma5`           decimal(20, 5) COMMENT 'ma5',
    `ma6`           decimal(20, 5) COMMENT 'ma5',
    `ma10`          decimal(20, 5) COMMENT 'ma10',
    `ma12`          decimal(20, 5) COMMENT 'ma12',
    `ma20`          decimal(20, 5) COMMENT 'ma20',
    `ma24`          decimal(20, 5) COMMENT 'ma24',
    `ma50`          decimal(20, 5) COMMENT 'ma50',
    `ma60`          decimal(20, 5) COMMENT 'ma60',
    `bias6`         decimal(20, 5) COMMENT 'bias6',
    `bias12`        decimal(20, 5) COMMENT 'bias12',
    `bias24`        decimal(20, 5) COMMENT 'bias24',
    `bias36`        decimal(20, 5) COMMENT 'bias36',
    `mtr`           decimal(20, 5) COMMENT 'mtr',
    `atr`           decimal(20, 5) COMMENT 'atr',
    `dpo`           decimal(20, 5) COMMENT 'dpo',
    `upper_ene`     decimal(20, 5) COMMENT 'upper_ene',
    `lower_ene`     decimal(20, 5) COMMENT 'lower_ene',
    `ene`           decimal(20, 5) COMMENT 'ene',
    `emv`           decimal(20, 5) COMMENT 'emv',
    `mtm`           decimal(20, 5) COMMENT 'mtm',
    `wr6`           decimal(20, 5) COMMENT 'wr6',
    `wr10`          decimal(20, 5) COMMENT 'wr10',
    `psy`           decimal(20, 5) COMMENT 'psy',
    `psyma`         decimal(20, 5) COMMENT 'psyma',
    `roc`           decimal(20, 5) COMMENT 'roc',
    `maroc`         decimal(20, 5) COMMENT 'maroc',
    `upperl`        decimal(20, 5) COMMENT 'upperl',
    `uppers`        decimal(20, 5) COMMENT 'uppers',
    `lowerl`        decimal(20, 5) COMMENT 'lowerl',
    `lowers`        decimal(20, 5) COMMENT 'lowers',
    `k`             decimal(20, 5) COMMENT 'k',
    `d`             decimal(20, 5) COMMENT 'd',
    `j`             decimal(20, 5) COMMENT 'j',
    `pdi`           decimal(20, 5) COMMENT 'pdi',
    `mdi`           decimal(20, 5) COMMENT 'mdi',
    `adx`           decimal(20, 5) COMMENT 'adx',
    `dif`           decimal(32, 10) COMMENT 'dif',
    `dea`           decimal(20, 5) COMMENT 'dea',
    `macd`          decimal(20, 5) COMMENT 'macd',
    `rsi6`          decimal(20, 5) COMMENT 'rsi6',
    `rsi12`         decimal(20, 5) COMMENT 'rsi12',
    `rsi24`         decimal(20, 5) COMMENT 'rsi24',
    `sar`           decimal(20, 5) COMMENT 'sar',
    `trix`          decimal(20, 5) COMMENT 'trix',
    `lwr1`          decimal(20, 5) COMMENT 'lwr1',
    `lwr2`          decimal(20, 5) COMMENT 'lwr2',
    `stor`          decimal(20, 5) COMMENT '服务于mike',
    `midr`          decimal(20, 5) COMMENT '服务于mike',
    `wekr`          decimal(20, 5) COMMENT '服务于mike',
    `weks`          decimal(20, 5) COMMENT '服务于mike',
    `mids`          decimal(20, 5) COMMENT '服务于mike',
    `stos`          decimal(20, 5) COMMENT '服务于mike',
    `obv`           decimal(20, 5) COMMENT 'obv',
    `cci`           decimal(20, 5) COMMENT 'cci',
    `boll`          decimal(20, 5) COMMENT 'boll',
    `boll_up`       decimal(20, 5) COMMENT 'boll_up',
    `boll_down`     decimal(20, 5) COMMENT 'boll_down',
    `ds`            string COMMENT '交易日'
) COMMENT '东方财富A股技术指标'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/dwd/dwd_stock_detail'
    TBLPROPERTIES ('orc.compress' = 'snappy');


-- 技术全面指标首日脚本
alter table dwd_stock_detail
    drop partition (dt = '2022-09-02');

insert into table dwd_stock_detail partition (dt = '2022-09-02')
select dg.rk
     , dg.code
     , dg.name
     , round(dg.closing_price, 2) as closing_price
     , round(dg.closing_diff, 2)  as closing_diff
     , round(fdg.deal_amount, 4)  as deal_amount
     , round(dg.highest, 2)       as highest
     , round(dg.lowest, 2)        as lowest
     , round(dg.opening_price, 2) as opening_price
     , round(dg.asi, 4)           as asi
     , round(dg.bbi, 4)           as bbi
     , round(dg.br, 4)            as br
     , round(dg.ar, 4)            as ar
     , round(dg.ma3, 4)           as ma3
     , round(dg.ma5, 4)           as ma5
     , round(dg.ma6, 4)           as ma6
     , round(dg.ma10, 4)          as ma10
     , round(dg.ma12, 4)          as ma12
     , round(dg.ma20, 4)          as ma20
     , round(dg.ma24, 4)          as ma24
     , round(dg.ma50, 4)          as ma50
     , round(dg.ma60, 4)          as ma60
     , round(dg.bias6, 4)         as bias6
     , round(dg.bias12, 4)        as bias12
     , round(dg.bias24, 4)        as bias24
     , round(dg.bias36, 4)        as bias36
     , round(dg.mtr, 4)           as mtr
     , round(dg.atr, 4)           as atr
     , round(dg.dpo, 4)           as dpo
     , round(dg.upper_ene, 4)     as upper_ene
     , round(dg.lower_ene, 4)     as lower_ene
     , round(dg.ene, 4)           as ene
     , round(dg.emv, 4)           as emv
     , round(dg.mtm, 4)           as mtm
     , round(dg.wr6, 4)           as wr6
     , round(dg.wr10, 4)          as wr10
     , round(dg.psy, 4)           as psy
     , round(dg.psyma, 4)         as psyma
     , round(dg.roc, 4)           as roc
     , round(dg.maroc, 4)         as maroc
     , round(dg.upperl, 4)        as upperl
     , round(dg.uppers, 4)        as uppers
     , round(dg.lowerl, 4)        as lowerl
     , round(dg.lowers, 4)        as lowers
     , round(fdg.k, 4)            as k
     , round(fdg.d, 4)            as d
     , round(fdg.j, 4)            as j
     , round(fdg.pdi, 4)          as pdi
     , round(fdg.mdi, 4)          as mdi
     , round(fdg.adx, 4)          as adx
     , round(fdg.dif, 4)          as dif
     , round(fdg.dea, 4)          as dea
     , round(fdg.macd, 4)         as macd
     , round(fdg.rsi6, 4)         as rsi6
     , round(fdg.rsi12, 4)        as rsi12
     , round(fdg.rsi24, 4)        as rsi24
     , round(fdg.sar, 4)          as sar
     , round(fdg.trix, 4)         as trix
     , round(fdg.lwr1, 4)         as lwr1
     , round(fdg.lwr2, 4)         as lwr2
     , round(fdg.stor, 4)         as stor
     , round(fdg.midr, 4)         as midr
     , round(fdg.wekr, 4)         as wekr
     , round(fdg.weks, 4)         as weks
     , round(fdg.mids, 4)         as mids
     , round(fdg.stos, 4)         as stos
     , round(fdg.obv)             as obv
     , round(ie.cci, 4)           as cci
     , round(ie.boll, 4)          as boll
     , round(ie.boll_up, 4)       as boll_up
     , round(ie.boll_down, 4)     as boll_down
     , fdg.ds
from ods_stock_inrecursive_index as dg
         inner join ods_a_stock_recursive_index as fdg
                    on dg.code = fdg.code and dg.ds = fdg.ds
         inner join dwd_a_stock_inrecursive_extend as ie on dg.code = ie.code and dg.ds = ie.ds;


alter table dwd_stock_detail
    drop
        partition (dt = '2022-11-01');
-- dwd每日指标
insert into table dwd_stock_detail partition (dt = '2022-09-02')
select dg.rk
     , dg.code
     , dg.name
     , round(dg.closing_price, 2) as closing_price
     , round(dg.closing_diff, 2)  as closing_diff
     , round(fdg.deal_amount, 4)  as deal_amount
     , round(dg.highest, 2)       as highest
     , round(dg.lowest, 2)        as lowest
     , round(dg.opening_price, 2) as opening_price
     , round(dg.asi, 4)           as asi
     , round(dg.bbi, 4)           as bbi
     , round(dg.br, 4)            as br
     , round(dg.ar, 4)            as ar
     , round(dg.ma3, 4)           as ma3
     , round(dg.ma5, 4)           as ma5
     , round(dg.ma6, 4)           as ma6
     , round(dg.ma10, 4)          as ma10
     , round(dg.ma12, 4)          as ma12
     , round(dg.ma20, 4)          as ma20
     , round(dg.ma24, 4)          as ma24
     , round(dg.ma50, 4)          as ma50
     , round(dg.ma60, 4)          as ma60
     , round(dg.bias6, 4)         as bias6
     , round(dg.bias12, 4)        as bias12
     , round(dg.bias24, 4)        as bias24
     , round(dg.bias36, 4)        as bias36
     , round(dg.mtr, 4)           as mtr
     , round(dg.atr, 4)           as atr
     , round(dg.dpo, 4)           as dpo
     , round(dg.upper_ene, 4)     as upper_ene
     , round(dg.lower_ene, 4)     as lower_ene
     , round(dg.ene, 4)           as ene
     , round(dg.emv, 4)           as emv
     , round(dg.mtm, 4)           as mtm
     , round(dg.wr6, 4)           as wr6
     , round(dg.wr10, 4)          as wr10
     , round(dg.psy, 4)           as psy
     , round(dg.psyma, 4)         as psyma
     , round(dg.roc, 4)           as roc
     , round(dg.maroc, 4)         as maroc
     , round(dg.upperl, 4)        as upperl
     , round(dg.uppers, 4)        as uppers
     , round(dg.lowerl, 4)        as lowerl
     , round(dg.lowers, 4)        as lowers
     , round(fdg.k, 4)            as k
     , round(fdg.d, 4)            as d
     , round(fdg.j, 4)            as j
     , round(fdg.pdi, 4)          as pdi
     , round(fdg.mdi, 4)          as mdi
     , round(fdg.adx, 4)          as adx
     , round(fdg.dif, 4)          as dif
     , round(fdg.dea, 4)          as dea
     , round(fdg.macd, 4)         as macd
     , round(fdg.rsi6, 4)         as rsi6
     , round(fdg.rsi12, 4)        as rsi12
     , round(fdg.rsi24, 4)        as rsi24
     , round(fdg.sar, 4)          as sar
     , round(fdg.trix, 4)         as trix
     , round(fdg.lwr1, 4)         as lwr1
     , round(fdg.lwr2, 4)         as lwr2
     , round(fdg.stor, 4)         as stor
     , round(fdg.midr, 4)         as midr
     , round(fdg.wekr, 4)         as wekr
     , round(fdg.weks, 4)         as weks
     , round(fdg.mids, 4)         as mids
     , round(fdg.stos, 4)         as stos
     , round(fdg.obv)             as obv
     , round(ie.cci, 4)           as cci
     , round(ie.boll, 4)          as boll
     , round(ie.boll_up, 4)       as boll_up
     , round(ie.boll_down, 4)     as boll_down
     , fdg.ds
from (
         select *
         from ods_stock_inrecursive_index
         where dt = '2022-09-02'
     ) dg
         inner join (
    select *
    from ods_a_stock_recursive_index
    where dt = '2022-09-02'
) fdg on fdg.code = dg.code and dg.ds = fdg.ds
         inner join (
    select *
    from dwd_a_stock_inrecursive_extend
    where dt = '2022-09-02'
) ie on dg.code = ie.code and dg.ds = ie.ds;


desc dwd_a_stock_inrecursive_extend;