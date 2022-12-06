show tables;

desc ods_a_stock_detail_day;

drop table ods_a_stock_company_info;
create table if not exists ods_a_stock_company_info
(
    `code`               STRING COMMENT '股票代码',
    `event_type`         STRING COMMENT '事件类型',
    `specific_eventtype` STRING COMMENT '特殊类型',
    `l1_comment`         STRING COMMENT 'L1',
    `l2_comment`         STRING COMMENT 'L2',
    `notice_date`        STRING COMMENT '事件时间'
)
    COMMENT '个股每日详情表(不含历史)'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_company_info'
;

load data inpath '/hive/warehouse/origin_db/df_a_stock_company_info/2022-11-22' into table ods_a_stock_company_info partition (dt = '2022-11-22');



select cal.code,
       cal.event_type,
       cal.specific_eventtype,
       concat(round((ed.current_price - st.current_price) * 100 / st.current_price, 2), '%') as upordown
from (
         select a.code, a.event_type, a.specific_eventtype, a.l1_comment, b.ds
         from ods_a_stock_company_info a
                  join ods_calendar b
                       on a.notice_date >= '2022-11-14'
                           and b.ds >= '2022-11-14'
                           and b.astatus = 1
                           and date_add(a.notice_date, 1) = b.ds
     ) cal
         join ods_a_stock_detail_day st
              on cal.code = st.code
                  and cal.ds = st.ds
         join ods_a_stock_detail_day ed
              on cal.code = ed.code
                  and ed.ds = '2022-11-21'
order by round((ed.current_price - st.current_price) * 100 / st.current_price, 2)
;



select event_type
     , round(sum(upordown), 2) *
       100 as up2down
from (
         select cal.code,
                cal.event_type,
                round((ed.current_price - st.current_price) * 100 / st.current_price, 2) as upordown
         from (
                  select a.code, a.event_type, '2022-11-18' as ds
                  from ods_a_stock_company_info a
                  where a.notice_date = '2022-11-18'
                  group by a.code, a.event_type
              ) cal
                  join ods_a_stock_detail_day st
                       on cal.code = st.code
                           and cal.ds = st.ds
                  join ods_a_stock_detail_day ed
                       on cal.code = ed.code
                           and ed.ds = '2022-11-21'
     ) result
where upordown is not null
group by event_type
order by up2down desc
;


select d.event_type,
       sum(updown)                                                                         as bi
        ,
       sum(if(updown > 0, 1, 0)) / (sum(if(updown > 0, 1, 0)) + sum(if(updown < 0, 1, 0))) as biamount
from (
         select a.code,
                a.event_type,
                a.notice_date,
                round((c.current_price - b.current_price) / b.current_price, 2) updown
         from (
                  select distinct com.code, com.ds, com.notice_date, com.event_type
                  from (
                           SELECT code, event_type, notice_date, date_add(notice_date, 5) as ds
                           FROM `ods_a_stock_company_info`
                           where notice_date >= '2022-08-05'
                             and notice_date <= '2022-11-15'
                           group by code, event_type, notice_date
                       ) com
                           join ods_calendar cal
                                on (com.ds = cal.ds or com.notice_date = cal.ds)
                                    and cal.astatus = 1
              ) a
                  join ods_a_stock_detail_day b
                       on a.code = b.code
                           and a.notice_date = b.ds
                  join ods_a_stock_detail_day c
                       on a.code = c.code
                           and a.ds = c.ds
     ) d
group by d.event_type
order by biamount desc
;

-- 批量删除分区
-- alter table ods_a_stock_detail_day drop partition (dt>='2022-11-08',dt<='2022-11-21');
-- 持有5~7天比较好
select max(code), floor(cash_dif) as cash, avg(dif) as dif_day
from (
         select a.code
              , (b.current_price - c.current_price) * 100 / c.current_price as cash_dif
              , datediff(b.ds, c.ds)                                        as dif
              , b.ds
         from ods_a_stock_company_info a
                  join ods_a_stock_detail_day c
                       on a.code = c.code
                           and a.notice_date = c.ds
                           and a.dt = '2022-11-22'
                           and c.board in (2, 6)
                  left join ods_a_stock_detail_day b
                            on a.code = b.code
                                and b.ds > a.notice_date
                                and a.notice_date >= '2022-08-05'
                                and a.dt = '2022-11-22'
         where b.current_price - c.current_price > 0
         order by cash_dif desc
     ) a
group by floor(cash_dif)
order by cash desc
;



select a.code
     , (b.current_price - c.current_price) * 100 / c.current_price as cash_dif
     , datediff(b.ds, c.ds)                                        as dif
     , b.ds
     , floor(a.financing_purchase * 100 / c.total_market_v)        as ffbi
from ods_a_stock_finance_info a
         join ods_a_stock_detail_day c
              on a.code = c.code
                  and a.notice_date = c.ds
                  and a.dt = '2022-11-23'
                  and c.board in (2, 6)
         left join ods_a_stock_detail_day b
                   on a.code = b.code
                       and b.ds > a.notice_date
                       and a.notice_date >= '2022-08-05'
                       and a.dt = '2022-11-23'
where datediff(b.ds, c.ds) >= 5
order by cash_dif desc;



select *
from ods_a_stock_finance_info
where dt = '2022-11-23'
  and notice_date >= '2022-08-05'
limit 100;

create table if not exists ods_a_stock_finance_info
(
    `code`               STRING COMMENT '股票代码',
    `financing_purchase` STRING COMMENT '融资买入额(元)',
    `financing_repay`    STRING COMMENT '融资偿还额(元)',
    `financing_balance`  STRING COMMENT '融资余额(元)',
    `bond_sell`          STRING COMMENT '融券卖出量(股)',
    `bond_repay`         STRING COMMENT '融券偿还量(股)',
    `bond_balance`       STRING COMMENT '融券余额(元)',
    `notice_date`        STRING COMMENT '事件时间'
)
    COMMENT '个股每日详情表(不含历史)'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_finance_info'
;

create table if not exists ods_a_stock_news
(
    `code`        STRING COMMENT '股票代码',
    `mediaName`   STRING COMMENT '媒体',
    `title`       STRING COMMENT '标题',
    `content`     STRING COMMENT '内容',
    `notice_date` STRING COMMENT '时间'
)
    COMMENT '个股每日新闻'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_news'
;


select *
from (
         select a.code,
                a.title,
                a.notice_date,
                round((c.current_price - b.current_price) * 100 / b.current_price, 2) as dif
         from ods_a_stock_news a
                  join ods_a_stock_detail_day b
                       on a.code = b.code and to_date(a.notice_date) = b.ds
                           and b.board in (2, 6)
                  left join ods_a_stock_detail_day c
                            on a.code = c.code
                                and date_add(to_date(a.notice_date), 3) = c.ds) a
where dif >= 8
order by dif desc
;

-- 融资买入额：当日融资买入的部分
-- 融资偿还额：当日偿还融资的部分
-- 融资余额：前日融资余额+本日融资买入额-本日融资偿还额
desc ods_a_stock_detail_day;

select a.code,
       a.financing_purchase,
       a.financing_repay,
       a.financing_balance,
       b.up_down_rate,
       b.total_market_v,
       b.circulation_market_v,
       b.tradable_shares,
       c.title,
       b.ds
from ods_a_stock_finance_info a
         join ods_a_stock_detail_day b
              on a.code = b.code
                  and b.dt = '2022-11-24'
                  and board in (2, 6)
         left join ods_a_stock_news c
                   on a.code = c.code
                       and to_date(c.notice_date) = '2022-11-23'
where a.dt = '2022-11-23'
  and a.notice_date = '2022-11-23'
order by up_down_rate desc;


-- 连续增加的融资买入额，然后股票的响应

select a.*, b.title
from (select code, up_down_rate, turnover_rate
      from ods_a_stock_detail_day
      where dt = '2022-12-05'
        and board in (2, 6)
        and current_price <= 40
        and code in (
          select b.code
          from (
                   select a.*
                        , count(1) over (partition by code,date_add(a.notice_date, -rk)) as num
                   from (
                            select a.code,
                                   a.notice_date,
                                   a.financing_purchase                                           as pre,
                                   row_number() over (partition by a.code order by a.notice_date) as rk
                            from ods_a_stock_finance_info a
                                     join ods_a_stock_finance_info b
                                          on a.code = b.code and a.notice_date = date_add(b.notice_date, 1)
                            where cast(a.financing_purchase as double) >= cast(b.financing_purchase as double)
                        ) a
               ) b
                   inner join ods_a_stock_detail_day c
                              on b.code = c.code
                                  and c.dt = '2022-12-02'
          where num >= 2
            and notice_date = '2022-12-02'
      )) a
         left join ods_a_stock_news b on a.code = b.code and b.dt in ('2022-12-02', '2022-12-04')
;


desc ods_a_stock_detail_day;

select *
from ods_a_stock_news
where code = '600602';



create table if not exists ods_a_stock_chance_info
(
    `code`               STRING COMMENT '股票代码',
    `TOTAL_SCORE`        STRING COMMENT '总分',
    `RISE_1_PROBABILITY` STRING COMMENT '次日上涨概率',
    `AVERAGE_1_INCREASE` STRING COMMENT '次日平均概率',
    `RISE_5_PROBABILITY` STRING COMMENT '5日上涨概率',
    `AVERAGE_5_INCREASE` STRING COMMENT '5日平均概率',
    `STOCK_RANK_RATIO`   STRING COMMENT '打败多少股票',
    `WORDS_EXPLAIN`      STRING COMMENT '消息面',
    `notice_date`        STRING COMMENT '事件时间'
)
    COMMENT '个股每日涨跌率'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/hive/warehouse/df_db/ods/ods_a_stock_chance_info'
;

select *
from ods_a_stock_chance_info
where dt = '2022-11-25'
  and notice_date = '2022-11-25'
order by STOCK_RANK_RATIO desc;



select *
from ods_a_stock_news
where code = '688026';

select *
from ods_a_stock_finance_info
where code = '688026';

select *
from ods_a_stock_detail_day
where dt = '2022-11-28'
  and board in (2, 6)
order by up_down_rate desc;
select *
from ods_a_stock_deal
where dt = '2022-11-28';


select a.code, a.up_down_rate, b.title, c.operate_profit_yoy, c.total_current_assets_yoy, turnover_rate
from ods_a_stock_detail_day a
         left join ods_a_stock_news b
                   on a.code = b.code
                       and b.dt = '2022-12-04'
         left join ods_a_stock_report_info c
                   on a.code = c.code
where a.dt = '2022-12-05'
  and a.board in (2, 6)
order by a.up_down_rate desc
;

desc ods_a_stock_report_info;


select a.*, b.rk
from (
         select a.code, a.ds, a.current_price, a.up_down_rate, a.turnover_rate
         from (select * from ods_a_stock_detail_day where dt >= '2022-11-20' and board in (2, 6)) a
                  left join (select * from ods_a_stock_detail_day where dt >= '2022-11-19' and board in (2, 6)) b
                            on a.ds = date_add(b.ds, 1)
                                and a.code = b.code
                                and a.current_price >= b.current_price
     ) a
         inner join (
    select ds, row_number() over (order by ds ) as rk
    from ods_calendar
    where astatus = 1
      and substr(ds, 1, 7) >= '2022-11'
) b on a.ds = b.ds
;

drop table dwd_stock_continuation_up;

create table if not exists dwd_stock_continuation_up
(
    `code`    STRING COMMENT '股票代码',
    `times`   bigint COMMENT '连涨次数',
    `sumrate` decimal(8, 2) COMMENT '累积幅度',
    `tag`     int COMMENT '1连涨，2连跌'
)
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/dwd/dwd_stock_continuation_up'
    TBLPROPERTIES ('orc.compress' = 'snappy');
;

-- 连续上涨or下跌的股票(>=3次)
insert overwrite table dwd_stock_continuation_up partition (dt = '9999-12-31')
select code, count(1) + 1 as times, sum(up_down_rate) as sumrate, 1 as tag
from (
         select res.*, (rk - row_number() over (partition by code order by rk)) as g
         from (
                  select today.ds, today.code, today.up_down_rate, today.current_price, today.turnover_rate, today.rk
                  from (
                           select a.ds, a.code, a.current_price, a.up_down_rate, a.turnover_rate, b.rk
                           from (select *
                                 from ods_a_stock_detail_day
                                 where dt >= date_add(current_date(), -10)
                                   and board in (2, 6)) a
                                    left join
                                (select ds, row_number() over (order by ds) as rk
                                 from ods_calendar
                                 where ds >= date_add(current_date(), -10)
                                   and astatus = 1) b
                                on a.ds = b.ds
                       ) today
                           inner join (
                      select a.ds, a.code, a.current_price, a.up_down_rate, a.turnover_rate, b.rk
                      from (select *
                            from ods_a_stock_detail_day
                            where dt >= date_add(current_date(), -10)
                              and board in (2, 6)) a
                               left join
                           (select ds, row_number() over (order by ds) as rk
                            from ods_calendar
                            where ds >= date_add(current_date(), -10)
                              and astatus = 1) b
                           on a.ds = b.ds
                  ) yester on today.code = yester.code
                      and today.rk = yester.rk + 1
                      and today.current_price > yester.current_price) res
     ) a
group by code, g
having count(1) >= 2
   and max(ds) = current_date()
union all
select code, count(1) + 1 as times, sum(up_down_rate) as sumrate, 2 as tag
from (
         select res.*, (rk - row_number() over (partition by code order by rk)) as g
         from (
                  select today.ds, today.code, today.up_down_rate, today.current_price, today.turnover_rate, today.rk
                  from (
                           select a.ds, a.code, a.current_price, a.up_down_rate, a.turnover_rate, b.rk
                           from (select *
                                 from ods_a_stock_detail_day
                                 where dt >= date_add(current_date(), -10)
                                   and board in (2, 6)) a
                                    left join
                                (select ds, row_number() over (order by ds) as rk
                                 from ods_calendar
                                 where ds >= date_add(current_date(), -10)
                                   and astatus = 1) b
                                on a.ds = b.ds
                       ) today
                           inner join (
                      select a.ds, a.code, a.current_price, a.up_down_rate, a.turnover_rate, b.rk
                      from (select *
                            from ods_a_stock_detail_day
                            where dt >= date_add(current_date(), -10)
                              and board in (2, 6)) a
                               left join
                           (select ds, row_number() over (order by ds) as rk
                            from ods_calendar
                            where ds >= date_add(current_date(), -10)
                              and astatus = 1) b
                           on a.ds = b.ds
                  ) yester on today.code = yester.code
                      and today.rk = yester.rk + 1
                      and today.current_price < yester.current_price) res
     ) a
group by code, g
having count(1) >= 2
   and max(ds) = current_date()
;



create table if not exists dwd_finance_continuation_up
(
    `code`     STRING COMMENT '股票代码',
    `times`    bigint COMMENT '连涨次数',
    `sumprice` decimal(32, 4) COMMENT '累积价格',
    `tag`      int COMMENT '1连涨，2连跌'
)
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/dwd/dwd_finance_continuation_up'
    TBLPROPERTIES ('orc.compress' = 'snappy');
;

insert overwrite table dwd_finance_continuation_up partition (dt = '9999-12-31')
select code, count(1) + 1 as times, max(financing_balance) - min(financing_balance) as sumbuy, 1 as tag
from (
         select res.*, (rk - row_number() over (partition by code order by rk)) as g
         from (
                  select today.code,
                         today.financing_balance,
                         today.bond_balance,
                         today.notice_date,
                         today.rk
                  from (
                           select code,
                                  financing_balance,
                                  bond_balance,
                                  notice_date,
                                  row_number() over (partition by code order by notice_date) as rk
                           from ods_a_stock_finance_info
                           where dt >= date_add(current_date(), -11)
                             and code in
                                 (select distinct code
                                  from ods_a_stock_detail_day
                                  where dt = date_add(current_date(), -1)
                                    and board in (2, 6))
                       ) today
                           inner join (
                      select code,
                             financing_balance,
                             bond_balance,
                             notice_date,
                             row_number() over (partition by code order by notice_date) as rk
                      from ods_a_stock_finance_info
                      where dt >= date_add(current_date(), -11)
                        and code in
                            (select distinct code
                             from ods_a_stock_detail_day
                             where dt = date_add(current_date(), -1)
                               and board in (2, 6))
                  ) yester on today.code = yester.code
                      and today.rk = yester.rk + 1
                      and today.financing_balance > yester.financing_balance
              ) res
     ) a
group by code, g
having count(1) >= 2
   and max(notice_date) = date_add(current_date(), -1)
union all
select code, count(1) + 1 as times, min(financing_balance) - max(financing_balance) as sumbuy, 2 as tag
from (
         select res.*, (rk - row_number() over (partition by code order by rk)) as g
         from (
                  select today.code,
                         today.financing_balance,
                         today.bond_balance,
                         today.notice_date,
                         today.rk
                  from (
                           select code,
                                  financing_balance,
                                  bond_balance,
                                  notice_date,
                                  row_number() over (partition by code order by notice_date) as rk
                           from ods_a_stock_finance_info
                           where dt >= date_add(current_date(), -11)
                             and code in
                                 (select distinct code
                                  from ods_a_stock_detail_day
                                  where dt = date_add(current_date(), -1)
                                    and board in (2, 6))
                       ) today
                           inner join (
                      select code,
                             financing_balance,
                             bond_balance,
                             notice_date,
                             row_number() over (partition by code order by notice_date) as rk
                      from ods_a_stock_finance_info
                      where dt >= date_add(current_date(), -11)
                        and code in
                            (select distinct code
                             from ods_a_stock_detail_day
                             where dt = date_add(current_date(), -1)
                               and board in (2, 6))
                  ) yester on today.code = yester.code
                      and today.rk = yester.rk + 1
                      and today.financing_balance < yester.financing_balance
              ) res
     ) a
group by code, g
having count(1) >= 2
   and max(notice_date) = date_add(current_date(), -1);

select count(1) from dwd_finance_continuation_up;