-- 非递归指标每日脚本

set hive.groupby.skewindata=true;
set hive.map.aggr=true;

-- alter table ods_stock_inrecursive_index drop partition (dt = '2022-09-02');

insert into table ods_stock_inrecursive_index partition (dt = '2022-09-02')
select *
from (
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
                       , IF(rk >= 6,
                            AVG(roc) OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ),
                            0)                                                                              AS maroc
                       , IF(aa > bb AND aa > cc, aa + bb / 2 + dd / 4,
                            IF(bb > cc AND bb > aa, bb + aa / 2 + dd / 4, cc + dd / 4))                     AS r
                       , closing_price - last_closing + (closing_price - opening_price) / 2 + last_closing -
                         last_opening                                                                       AS x
                       , (upper + lower) / 2                                                                AS ene
                       , upper                                                                              AS upper_ene
                       , lower                                                                              AS lower_ene
                       , AVG(psy)
                             OVER (partition by code ORDER BY ds ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) AS psyma
                       , nvl(SUM(br1) OVER (partition by code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW ) *
                             100 /
                             SUM(br2) OVER (partition by code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW ),
                             0)                                                                             AS br
                       , nvl(SUM(ar1) OVER (partition by code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW ) *
                             100 /
                             SUM(ar2) OVER (partition by code ORDER BY ds ROWS BETWEEN 25 PRECEDING AND CURRENT ROW ),
                             0)                                                                             AS ar
                       , IF(hsl > lh, IF(hsl > ll, hsl, ll), IF(lh > ll, lh, ll))                           AS mtr
                       , round(AVG(mid * volume * (highest - lowest) / h_l)
                                   OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 13 PRECEDING AND CURRENT ROW ),
                               5)                                                                           AS emv
                  FROM (
                           SELECT *
                                , IF(rk >= 24, (MA3 + MA6 + MA12 + MA24) / 4, 0)            AS BBI
                                , (highest6 - closing_price) * 100 /
                                  if((highest6 - lowest6) = 0, 1, highest6 - lowest6)       AS WR6
                                , (highest10 - closing_price) * 100 /
                                  if((highest10 - lowest10) = 0, 1, highest10 - lowest10)   AS WR10
                                , (closing_price - avg6) * 100 / avg6                       AS BIAS6
                                , (closing_price - avg12) * 100 / avg12                     AS BIAS12
                                , (closing_price - avg24) * 100 / avg24                     AS BIAS24
                                , (closing_price - avg36) * 100 / avg36                     AS BIAS36
                                , (closing_price - pre_day) * 100 / pre_day                 AS roc
                                , abs(highest - last_closing)                               AS aa
                                , abs(lowest - last_closing)                                AS bb
                                , abs(highest - last_low)                                   AS cc
                                , abs(last_closing - last_opening)                          AS dd
                                , (1 + 6 / 100) * c25                                       AS upper
                                , (1 - 6 / 100) * c25                                       AS lower
                                , SUM(IF(closing_price > last_closing, 1, 0))
                                      OVER (partition by code ORDER BY ds ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) *
                                  100 /
                                  12                                                        AS psy
                                , IF(highest - last_closing > 0, highest - last_closing, 0) AS br1
                                , IF(last_closing - lowest > 0, last_closing - lowest, 0)   AS br2
                                , highest - opening_price                                   AS ar1
                                , opening_price - lowest                                    AS ar2
                                , abs(last_closing - highest)                               AS lh
                                , abs(last_closing - lowest)                                AS ll
                                , high30 * (1 + 15 / 100)                                   AS upperl
                                , high3 * (1 + 3 / 100)                                     AS uppers
                                , low3 * (1 - 3 / 100)                                      AS lowers
                                , low30 * (1 - 15 / 100)                                    AS lowerl
                                , 100 * (highest + lowest - last_hl) / (highest + lowest)   AS mid
                                , IF(rk > 30, closing_price - LAG(MA20, 11, MA20) OVER (PARTITION BY code ORDER BY ds ),
                                     0)                                                     AS dpo
                                , closing_price - LAG(closing_price, 12, closing_price)
                                                      OVER (PARTITION BY code ORDER BY ds ) AS mtm
                           FROM (
                                    select code
                                         , name
                                         , rk
                                         , opening_price
                                         , closing_price
                                         , highest
                                         , lowest
                                         , deal_amount
                                         , ds
                                         , LAG(closing_price, 1, closing_price)
                                               OVER (PARTITION BY code ORDER BY ds )                                           AS      last_closing
                                         , LAG(highest, 1, 0)
                                               OVER (PARTITION BY code ORDER BY ds )                                           AS      last_high -- 昨日最高
                                         , LAG(lowest, 1, 0)
                                               OVER (PARTITION BY code ORDER BY ds )                                           AS      last_low  -- 昨日最低
                                         , abs(LAG(closing_price, 1, closing_price)
                                                   OVER (PARTITION BY code ORDER BY ds ) -
                                               lowest)                                                                         AS      csl
                                         , abs(highest - LAG(closing_price, 1, closing_price)
                                                             OVER (PARTITION BY code ORDER BY ds ))                            AS      hsc
                                         , highest - lowest                                                                    AS      hsl
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds ) >= 3,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) /
                                              3,
                                              0)                                                                               AS      MA3
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds ) >= 5,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 4 PRECEDING AND CURRENT ROW ) /
                                              5,
                                              0)                                                                               AS      MA5
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds ) >= 6,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) /
                                              6,
                                              0)                                                                               AS      MA6
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds ) >= 10,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) /
                                              10,
                                              0)                                                                               AS      MA10
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds ) >= 12,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) /
                                              12,
                                              0)                                                                               AS      MA12
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds ) >= 20,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) /
                                              20,
                                              0)                                                                               AS      MA20
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds) >= 24,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 23 PRECEDING AND CURRENT ROW ) /
                                              24,
                                              0)                                                                               AS      MA24
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds ) >= 50,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 49 PRECEDING AND CURRENT ROW ) /
                                              50,
                                              0)                                                                               AS      MA50
                                         , IF(ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds ) >= 60,
                                              SUM(closing_price)
                                                  OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 59 PRECEDING AND CURRENT ROW ) /
                                              60,
                                              0)                                                                               AS      MA60
                                         , MAX(highest)
                                               OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )  AS      highest6
                                         , MIN(lowest)
                                               OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )  AS      lowest6
                                         , MAX(highest)
                                               OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 9 PRECEDING AND CURRENT ROW )  AS      highest10
                                         , MIN(lowest)
                                               OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 9 PRECEDING AND CURRENT ROW )  AS      lowest10
                                         , AVG(closing_price)
                                               OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 5 PRECEDING AND CURRENT ROW )  AS      avg6
                                         , AVG(closing_price)
                                               OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) AS      avg12
                                         , AVG(closing_price)
                                               OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 23 PRECEDING AND CURRENT ROW ) AS      avg24
                                         , AVG(closing_price)
                                               OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 35 PRECEDING AND CURRENT ROW ) AS      avg36
                                         , FIRST_VALUE(closing_price)
                                                       OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) pre_day
                                         , LAG(opening_price, 1, 0)
                                               OVER (PARTITION BY CODE ORDER BY ds )                                           AS      last_opening
                                         , IF(ROW_NUMBER() OVER (PARTITION BY code ORDER BY ds ) >= 25,
                                              AVG(closing_price)
                                                  OVER (partition by code order by ds ROWS BETWEEN 24 PRECEDING AND CURRENT ROW ),
                                              0)                                                                               AS      c25
                                         , AVG(highest)
                                               OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) AS      high30
                                         , AVG(highest)
                                               OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )  AS      high3
                                         , AVG(lowest)
                                               OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )  AS      low3
                                         , AVG(lowest)
                                               OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) AS      low30
                                         , AVG(deal_amount)
                                               OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 13 PRECEDING AND CURRENT ROW ) /
                                           deal_amount                                                                         AS      volume
                                         , LAG(highest + lowest, 1, highest + lowest)
                                               OVER (PARTITION BY code ORDER BY ds )                                           AS      last_hl
                                         , AVG(highest - lowest)
                                               OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 13 PRECEDING AND CURRENT ROW ) AS      h_l
                                    from (
                                             select a.code,
                                                    a.name,
                                                    nvl(b.current_rk,0) + 1 as rk,
                                                    a.opening_price,
                                                    a.closing_price,
                                                    a.highest,
                                                    a.lowest,
                                                    a.deal_amount,
                                                    a.ds
                                             from (
                                                      select code,
                                                             name,
                                                             opening_price,
                                                             current_price as closing_price,
                                                             highest,
                                                             lowest,
                                                             deal_vol      as deal_amount,
                                                             ds
                                                      from ods_a_stock_detail_day
                                                      where ds = '2022-09-02'
                                                        and current_price <> 0
                                                  ) a
                                                      left join (
                                                 select code, max(rk) as current_rk
                                                 from ods_stock_inrecursive_index
                                                 where ds < '2022-09-02'
                                                 group by code
                                             ) b on a.code = b.code -- 最新数据
                                             union all
                                             select code,
                                                    name,
                                                    rk,
                                                    opening_price,
                                                    closing_price,
                                                    highest,
                                                    lowest,
                                                    deal_amount,
                                                    ds
                                             from (
                                                      select code,
                                                             name,
                                                             rk,
                                                             opening_price,
                                                             closing_price,
                                                             highest,
                                                             lowest,
                                                             deal_amount,
                                                             ds,
                                                             row_number() over (partition by code order by ds desc) as rowid
                                                      from ods_stock_inrecursive_index
                                                      where ds < '2022-09-02'
                                                        and code in
                                                            (select code
                                                             from ods_a_stock_detail_day
                                                             where ds = '2022-09-02'
                                                               and current_price <> 0) -- 还未退市的股票
                                                  ) c
                                             where rowid <= 60
                                         ) today
                                ) t1
                       ) t2
              ) t3
     ) t4
where ds = '2022-09-02'
;


----------
select * from dwd_stock_detail where code='603185';







