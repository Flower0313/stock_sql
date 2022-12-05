use dfdb;

select *
from ods_a_stock_news
where dt = '2022-11-29';

select a.*, b.up_down_rate
from ods_a_stock_news a
         inner join (
    select code, up_down_rate
    from ods_a_stock_detail_day
    where dt = '2022-11-30'
      and board in (2, 6)
      and up_down_rate >= 4
) b on a.code = b.code
where a.dt = '2022-11-29';

select a.code, b.title
from (
         select code, up_down_rate
         from ods_a_stock_detail_day
         where dt = '2022-11-30'
           and board in (2, 6)
           and up_down_rate >= 4
     ) a
         left join ods_a_stock_news b
                   on a.code = b.code
                       and b.dt = '2022-11-29'
;

-- 根据昨日新闻来查找今日股票涨跌情况
select a.code, a.title, a.notice_date, b.up_down_rate
from ods_a_stock_news a
         left join ods_a_stock_detail_day b
                   on a.code = b.code
                       and a.dt = '2022-11-29'
where b.dt = '2022-11-30'
  and b.board in (2, 6)
  and a.notice_date >= '2022-11-29 15:00'
order by b.up_down_rate desc;



select *
from ods_a_stock_news
where dt = '2022-11-30';

select a.code, up_down_rate, c.title, b.financing_purchase - b.financing_repay as dif
from ods_a_stock_detail_day a
         left join
     ods_a_stock_finance_info b on a.code = b.code
         and b.dt = '2022-11-29'
         left join ods_a_stock_news c
                   on a.code = c.code
                       and c.dt = '2022-11-29'
where a.board in (2, 6)
  and a.dt = '2022-11-30'
  and c.title is
  and (b.financing_purchase - b.financing_repay) > 0
order by up_down_rate desc
;

select a.*, b.up_down_rate
from (
         select a.code, a.title, (b.financing_purchase - b.financing_repay) as dif
         from ods_a_stock_news a
                  full join ods_a_stock_finance_info b
                            on a.code = b.code
         where a.notice_date > '2022-11-30 15:00:00'
           and b.dt = '2022-11-30'
           and b.financing_purchase - b.financing_repay > 0
           and a.title is
     ) a
         left join ods_a_stock_detail_day b
                   on a.code = b.code
                       and b.dt = '2022-12-01'
                       and b.board in (2, 6)
order by b.up_down_rate desc
;



create table if not exists ods_a_stock_report_info
(
    `code`                        String COMMENT '股票代码',
    `report_date`                 string COMMENT '报告日期',
    `monetary_funds`              decimal(32, 3) COMMENT 'zcfz货币资金',
    `monetary_funds_yoy`          decimal(10, 2) COMMENT 'zcfz货币资金(同比)',
    `total_current_assets`        decimal(32, 3) COMMENT 'zcfz流动资产合计',
    `total_current_assets_yoy`    decimal(10, 2) COMMENT 'zcfz流动资产合计(同比)',
    `total_noncurrent_assets`     decimal(32, 3) COMMENT 'zcfz非流动资产合计',
    `total_noncurrent_assets_yoy` decimal(10, 2) COMMENT 'zcfz非流动资产合计(同比)',
    `total_current_liab`          decimal(32, 3) COMMENT 'zcfz流动负债合计',
    `total_current_liab_yoy`      decimal(10, 2) COMMENT 'zcfz流动负债合计(同比)',
    `total_noncurrent_liab`       decimal(32, 3) COMMENT 'zcfz非流动负债合计',
    `total_noncurrent_liab_yoy`   decimal(10, 2) COMMENT 'zcfz非流动负债合计(同比)',
    `total_equity`                decimal(32, 3) COMMENT 'zcfz股东权益合计',
    `total_equity_yoy`            decimal(10, 2) COMMENT 'zcfz股东权益合计(同比)',
    `total_operate_income`        decimal(32, 3) COMMENT 'lrb营收总收入',
    `total_operate_income_yoy`    decimal(10, 2) COMMENT 'lrb营收总收入(同比)',
    `total_operate_cost`          decimal(32, 3) COMMENT 'lrb营收总成本',
    `total_operate_cost_yoy`      decimal(10, 2) COMMENT 'lrb营收总成本(同比)',
    `operate_profit`              decimal(32, 3) COMMENT 'lrb营业利润',
    `operate_profit_yoy`          decimal(10, 2) COMMENT 'lrb营业利润(同比)',
    `total_profit`                decimal(32, 3) COMMENT 'lrb利润总额',
    `total_profit_yoy`            decimal(10, 2) COMMENT 'lrb利润总额(同比)',
    `netprofit`                   decimal(32, 3) COMMENT 'lrb净利润',
    `netprofit_yoy`               decimal(10, 2) COMMENT 'lrb净利润(同比)',
    `total_compre_income`         decimal(32, 3) COMMENT 'lrb综合收益总额',
    `total_compre_income_yoy`     decimal(10, 2) COMMENT 'lrb综合收益总额(同比)',
    `netcash_operate`             decimal(32, 3) COMMENT 'xjl经营活动现金流量净额',
    `netcash_operate_yoy`         decimal(10, 2) COMMENT 'xjl经营活动现金流量净额(同比)',
    `netcash_invest`              decimal(32, 3) COMMENT 'xjl投资活动产生的现金流量净额',
    `netcash_invest_yoy`          decimal(32, 3) COMMENT 'xjl投资活动产生的现金流量净额(同比)',
    `netcash_finance`             decimal(32, 3) COMMENT 'xjl筹资活动产生的现金流量净额',
    `netcash_finance_yoy`         decimal(10, 2) COMMENT 'xjl筹资活动产生的现金流量净额(同比)',
    `end_cce`                     decimal(32, 3) COMMENT 'xjl期末现金及现金等价物余额',
    `end_cce_yoy`                 decimal(10, 2) COMMENT 'xjl期末现金及现金等价物余额(同比)'
)
    COMMENT '个股三表数据'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_report_info'
;

desc ods_a_stock_report_info;

select a.code,
       a.up_down_rate,
       b.title,
       c.monetary_funds_yoy,
       c.total_operate_income_yoy,
       c.netcash_operate_yoy
from (
         select code, up_down_rate
         from ods_a_stock_detail_day
         where dt = '2022-12-02'
           and board in (2, 6)
           and up_down_rate >= 5
     ) a
         left join ods_a_stock_news b
                   on a.code = b.code
                       and b.dt = '2022-12-01'
         left join ods_a_stock_report_info c
                   on a.code = c.code
;



select * from ods_a_stock_news where dt='2022-12-04';





