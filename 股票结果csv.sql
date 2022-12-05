drop table ads_stock;
create table if not exists ads_stock
(
    `code`                STRING COMMENT '股票代码',
    `co`                  decimal(10, 2) COMMENT 'co',
    `closing_price`       decimal(10, 2) COMMENT '今日收盘',
    `turnover_rate`       DECIMAL(5, 2) COMMENT '换手率',
    `PE_ratio_d`          DECIMAL(20, 2) COMMENT '市盈率(动态)',
    `PE_ratio_s`          DECIMAL(20, 2) COMMENT '市盈率(静态)',
    `amplitude`           DECIMAL(10, 2) COMMENT '振幅',
    `dd`                  DECIMAL(10, 2) COMMENT 'dd',
    `volume_ratio`        DECIMAL(10, 2) COMMENT 'volume_ratio',
    `pdi`                 DECIMAL(10, 5) COMMENT 'pdi',
    `mdi`                 DECIMAL(10, 5) COMMENT 'mdi',
    `ct`                  DECIMAL(20, 5) COMMENT 'ct',
    `price_to_b_ratio`    DECIMAL(10, 2) COMMENT 'price_to_b_ratio',
    `ttt`                 DECIMAL(20, 5) COMMENT 'ttt',
    `io`                  DECIMAL(20, 5) COMMENT 'io',
    `roe`                 DECIMAL(20, 3) COMMENT 'ROE加权净资产收益率',
    `total_revenue_r`     DECIMAL(32, 5) COMMENT '总营收同比',
    `gross_profit_margin` DECIMAL(32, 10) COMMENT '毛利率',
    `debt_ratio`          DECIMAL(32, 10) COMMENT '负债率',
    `ttm`                 DECIMAL(32, 2) COMMENT '市盈率(TTM)',
    `commission`          DECIMAL(5, 2) COMMENT '委比',
    `net_margin`          DECIMAL(32, 5) COMMENT '净利率',
    `net_profit_r`        decimal(32, 5) COMMENT '净利润同比',
    `main_inflow_ratio`   decimal(20, 2) COMMENT '主力比',
    `large_inflow_ratio`  decimal(20, 2) COMMENT '大单净比',
    `Slarge_inflow_ratio` decimal(20, 2) COMMENT '超大单净比',
    `mid_inflow_ratio`    decimal(20, 2) COMMENT '中单净比',
    `small_inflow_ratio`  decimal(20, 2) COMMENT '小单净比',
    `ma6`                 decimal(20, 5) COMMENT 'ma5',
    `cci`                 decimal(20, 5) COMMENT 'cci',
    `wr6`                 decimal(20, 5) COMMENT 'wr6',
    `maroc`               decimal(20, 5) COMMENT 'maroc',
    `asi`                 decimal(20, 5) COMMENT 'asi',
    `ds`                  string COMMENT '交易日',
    `result`              string COMMENT '结果'
) COMMENT '东方财富A股维度表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/ads/ads_stock'
    TBLPROPERTIES ('orc.compress' = 'snappy');

alter table ads_stock
    drop partition (dt = '2022-09-02');


insert into table ads_stock partition (dt = '2022-09-02')
select a.code,
       a.closing_price - a.opening_price                                 as co,
       a.closing_price                                                   as closing_price,
       a.turnover_rate,
       a.pe_ratio_d,
       a.pe_ratio_s,
       a.amplitude,
       a.deal_amount / a.deal_vol                                        as dd,
       a.volume_ratio,
       a.pdi,
       a.mdi,
       a.circulation_market_v / a.total_market_v                         as ct,
       a.price_to_b_ratio,
       a.tradable_shares / a.total_share_capital                         as ttt,
       a.inner_disk / a.outer_disk                                       as io,
       a.roe,
       a.total_revenue_r,
       a.gross_profit_margin,
       a.debt_ratio,
       a.ttm,
       a.commission,
       a.net_margin,
       a.net_profit_r,
       a.main_inflow_ratio,
       a.large_inflow_ratio,
       a.slarge_inflow_ratio,
       a.mid_inflow_ratio,
       a.small_inflow_ratio,
       a.ma6,
       a.cci,
       a.wr6,
       a.maroc,
       a.asi,
       a.ds,
       if(b.closing_diff > 0, "up", if(b.closing_diff = 0, "-", "down")) as result
from dim_stock a
         inner join dim_stock b
                    on a.rk = b.rk and a.code = b.code
where a.ds = '2022-09-02';



select dt, round(sum(if(up_down_rate > 0, 1, 0)) * 100 / count(1), 2) as zb
from ods_a_stock_detail_day
group by dt
order by zb;

select *
from ods_a_stock_detail_day
where dt = '2022-09-06';

select code
     , date_format(ds, 'yyyy')                                          as year
     , sum(if(date_format(ds, 'MMdd') = '1231', closing_price,
              if(date_format(ds, 'MMdd') = '0101', -closing_price, 0))) as diff
from dwd_stock_detail
group by code, date_format(ds, 'yyyy')
order by code, year;

select *
from ods_a_stock_detail_day
group by industry;


select ds, count(distinct code) as codeNum
from dwd_stock_detail
group by ds
order by ds desc;

select sum(if(up_down_rate>0,1,0)) as up,sum(if(up_down_rate<0,1,0)) as down from ods_a_stock_detail_day where dt='2022-09-28';

desc ods_a_stock_detail_day;

