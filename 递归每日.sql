show tables;
CREATE TABLE `ods_a_stock_recursive_index`
(
    `code`          STRING COMMENT '股票代码',
    `name`          STRING COMMENT '股票名称',
    `opening_price` decimal(10, 2) COMMENT '今日开盘',
    `closing_price` decimal(10, 2) COMMENT '今日收盘',
    `last_closing`  decimal(10, 5) COMMENT '昨日收盘',
    `highest`       decimal(10, 5) COMMENT '最高价',
    `lowest`        decimal(10, 5) COMMENT '最低价',
    `ds`            string COMMENT '交易日',
    `deal_amount`   decimal(20, 5) COMMENT '成交额',
    `closing_diff`  decimal(20, 5) COMMENT '差额',
    `rk`            bigint COMMENT '序号',
    `ema12`         decimal(15, 5) COMMENT '服务于macd',
    `ema26`         decimal(15, 5) COMMENT '服务于macd',
    `dif`           decimal(32, 10) COMMENT 'dif',
    `obv`           decimal(20, 5) COMMENT 'obv',
    `rsv`           decimal(20, 5) COMMENT 'rsv',
    `up6`           decimal(20, 5) COMMENT 'up6',
    `down6`         decimal(20, 5) COMMENT 'down6',
    `up12`          decimal(20, 5) COMMENT 'up12',
    `down12`        decimal(20, 5) COMMENT 'down12',
    `up24`          decimal(20, 5) COMMENT 'up24',
    `down24`        decimal(20, 5) COMMENT 'down24',
    `rsi6`          decimal(20, 5) COMMENT 'rsi6',
    `rsi12`         decimal(20, 5) COMMENT 'rsi12',
    `rsi24`         decimal(20, 5) COMMENT 'rsi24',
    `k`             decimal(20, 5) COMMENT 'k',
    `d`             decimal(20, 5) COMMENT 'd',
    `j`             decimal(20, 5) COMMENT 'j',
    `sar`           decimal(20, 5) COMMENT 'sar',
    `dea`           decimal(20, 5) COMMENT 'dea',
    `macd`          decimal(20, 5) COMMENT 'macd',
    `pdi`           decimal(20, 5) COMMENT 'pdi',
    `mdi`           decimal(20, 5) COMMENT 'mdi',
    `adx`           decimal(20, 5) COMMENT 'adx',
    `trex`          decimal(20, 5) COMMENT 'trex',
    `dmpex`         decimal(20, 5) COMMENT 'dmpex',
    `dmmex`         decimal(20, 5) COMMENT 'dmmex',
    `sar_bull`      boolean COMMENT 'sar_bull',
    `sar_low`       decimal(20, 5) COMMENT 'sar_low',
    `sar_high`      decimal(20, 5) COMMENT 'sar_high',
    `sar_af`        decimal(20, 5) COMMENT 'sar_af',
    `mpdi`          decimal(20, 5) COMMENT 'ar',
    `STOR`          decimal(20, 5) COMMENT '服务于mike',
    `MIDR`          decimal(20, 5) COMMENT '服务于mike',
    `WEKR`          decimal(20, 5) COMMENT '服务于mike',
    `WEKS`          decimal(20, 5) COMMENT '服务于mike',
    `MIDS`          decimal(20, 5) COMMENT '服务于mike',
    `STOS`          decimal(20, 5) COMMENT '服务于mike',
    `hlc`           decimal(20, 5) COMMENT '服务于mike',
    `hv`            decimal(20, 5) COMMENT '服务于mike',
    `lv`            decimal(20, 5) COMMENT '服务于mike',
    `ema1L`         decimal(20, 5) COMMENT '服务于trix',
    `mtr`           decimal(20, 5) COMMENT '服务于trix',
    `trix`          decimal(20, 5) COMMENT 'trix',
    `lwr1`          decimal(20, 5) COMMENT 'lwr1',
    `lwr2`          decimal(20, 5) COMMENT 'lwr2',
    `ema2L`         decimal(20, 5) COMMENT '服务于trix'
) COMMENT '东方财富A股递归指标'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/hive/warehouse/df_db/ods/ods_stock_recursive_index'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 递归指标第一步
insert into table ods_stock_step_one partition (dt = '2022-09-02')
SELECT code
     , name
     , opening_price
     , closing_price
     , last_closing
     , highest
     , lowest
     , ds
     , deal_amount
     , closing_diff
     , rk
     , hhv
     , llv
     , rsv
     , sar_high
     , sar_low
     , tr
     , IF(hd > 0 AND hd > ld, hd, 0) AS dmp
     , IF(ld > 0 AND ld > hd, ld, 0) AS dmm
     , hlc
     , highest9
     , lowest9
     , hhv10
     , llv10
FROM (
         SELECT *
              , nvl(IF(hhv != llv, (closing_price - llv) * 100 / (hhv - llv), 0), 0) AS rsv
              , IF(
                     rk <= 4
             , MAX(highest) OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 3 PRECEDING AND CURRENT ROW )
             , MAX(highest) OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 1 PRECEDING AND CURRENT ROW )
             )                                                                       AS sar_high
              , IF(
                     rk <= 4
             , MIN(lowest) OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 3 PRECEDING AND CURRENT ROW )
             , MIN(lowest) OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 1 PRECEDING AND CURRENT ROW )
             )                                                                       AS sar_low
              , IF(csl > hsc, IF(csl > hsl, csl, hsl), IF(hsc > hsl, hsc, hsl))      AS tr
              , IF(rk = 1, 0, highest - last_high)                                   AS hd
              , IF(rk = 1, 0, last_low - lowest)                                     AS ld
              , LAG(tmp_hlc, 1, tmp_hlc) OVER (PARTITION BY code ORDER BY ds )       AS hlc
         FROM (
                  SELECT CODE
                       , NAME
                       , opening_price
                       , closing_price
                       , LAG(closing_price, 1, closing_price) OVER (PARTITION BY code ORDER BY ds )         AS last_closing
                       , highest
                       , lowest
                       , ds                                                                                 AS ds
                       , deal_amount
                       , closing_price -
                         LAG(closing_price, 1, closing_price) OVER (PARTITION BY CODE ORDER BY ds )         AS closing_diff
                       , ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY ds )                                 AS rk
                       , MAX(highest)
                             OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 8 PRECEDING AND CURRENT ROW ) AS hhv
                       , MIN(lowest)
                             OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 8 PRECEDING AND CURRENT ROW ) AS llv
                       , LAG(highest, 1, 0) OVER (PARTITION BY code ORDER BY ds )                           AS last_high -- 昨日最高
                       , LAG(lowest, 1, 0) OVER (PARTITION BY code ORDER BY ds )                            AS last_low  -- 昨日最低
                       , highest - lowest                                                                   AS hsl
                       , abs(
                              LAG(closing_price, 1, closing_price) OVER (PARTITION BY code ORDER BY ds ) - lowest
                      )                                                                                     AS csl
                       , abs(
                              highest - LAG(closing_price, 1, closing_price) OVER (PARTITION BY code ORDER BY ds )
                      )                                                                                     AS hsc
                       , MAX(highest)
                             OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 8 PRECEDING AND CURRENT ROW ) AS highest9
                       , MIN(lowest)
                             OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 8 PRECEDING AND CURRENT ROW ) AS lowest9
                       , AVG((highest + lowest + closing_price) / 3)
                             OVER (PARTITION BY CODE ORDER BY ds ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS tmp_hlc
                       , MAX(highest)
                             OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS hhv10
                       , MIN(lowest)
                             OVER (PARTITION BY code ORDER BY ds ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS llv10
                  FROM (
                           SELECT a.code
                                , a.name
                                , nvl(b.current_rk, 0) + 1 AS rk
                                , a.opening_price
                                , a.closing_price
                                , a.highest
                                , a.lowest
                                , a.deal_amount
                                , a.ds
                           FROM (
                                    SELECT code
                                         , name
                                         , opening_price
                                         , current_price AS closing_price
                                         , highest
                                         , lowest
                                         , deal_vol      AS deal_amount
                                         , ds
                                    FROM ods_a_stock_detail_day
                                    WHERE ds = '2022-09-02'
                                      AND current_price <> 0
                                ) a
                                    left JOIN (
                               SELECT code
                                    , max(rk) AS current_rk
                               FROM ods_a_stock_recursive_index
                               WHERE ds < '2022-09-02'
                               GROUP BY code
                           ) b
                                              ON a.code = b.code -- 最新数据
                           UNION ALL
                           SELECT code
                                , name
                                , rk
                                , opening_price
                                , closing_price
                                , highest
                                , lowest
                                , deal_amount
                                , ds
                           FROM (
                                    SELECT code
                                         , name
                                         , rk
                                         , closing_price
                                         , opening_price
                                         , highest
                                         , lowest
                                         , deal_amount
                                         , ds
                                         , row_number() over (PARTITION BY code ORDER BY ds DESC) AS rowid
                                    FROM ods_stock_step_one
                                    WHERE ds < '2022-09-02'
                                      AND code IN (SELECT code
                                                   FROM ods_a_stock_detail_day
                                                   WHERE ds = '2022-09-02'
                                                     AND current_price <> 0) -- 还未退市的股票
                                ) c
                           WHERE rowid <= 10
                       ) a
              ) t1
     ) t3
where ds = '2022-09-02'
;

select dif
from ods_a_stock_recursive_index
where ds = '2022-09-02'
  and code = '000004';

-- 递归指标第二步
-- 解决类型转换的问题
set hive.vectorized.execution.enabled=false;
alter table ods_stock_step_one
    drop partition (dt = '2022-09-02');
select code,dif from ods_a_stock_recursive_index where ds='2022-09-02' and code in (000004,000001);

insert into table ods_a_stock_recursive_index partition (dt = '2022-09-02')
SELECT code
     , name
     , opening_price
     , closing_price
     , nvl(if(rk = 1, 0, last_closing), 0)                                               AS last_closing
     , nvl(new_highest, 0)                                                               AS highest
     , nvl(new_lowest, 0)                                                                AS lowest
     , ds
     , deal_amount
     , nvl(closing_diff, 0)                                                              AS closing_diff
     , rk
     , nvl(ema12, 0)                                                                     AS ema12
     , nvl(ema26, 0)                                                                     AS ema26
     , nvl(dif, 0)                                                                       AS dif
     , nvl(obv, 0)                                                                       AS obv
     , nvl(rsv, 0)                                                                       AS rsv
     , nvl(up6, 0)                                                                       AS up6
     , nvl(down6, 0)                                                                     AS down6
     , nvl(up12, 0)                                                                      AS up12
     , nvl(down12, 0)                                                                    AS down12
     , nvl(up24, 0)                                                                      AS up24
     , nvl(down24, 0)                                                                    AS down24
     , nvl(rsi6, 0)                                                                      AS rsi6
     , nvl(rsi12, 0)                                                                     AS rsi12
     , nvl(rsi24, 0)                                                                     AS rsi24
     , nvl(k, 0)                                                                         AS k
     , nvl(d, 0)                                                                         AS d
     , nvl(j, 0)                                                                         AS j
     , IF(rk >= 4, IF(rk = 4 or rk = 5, first_sar_low,
                      IF(last_sar_bull, IF(v_sar > closing_price, first_sar_high, v_sar),
                         IF(v_sar < closing_price, first_sar_low, v_sar))), 0)           AS sar
     , nvl(dea, 0)                                                                       AS dea
     , nvl(macd, 0)                                                                      AS macd
     , nvl(pdi, 0)                                                                       AS pdi
     , nvl(mdi, 0)                                                                       AS mdi
     , nvl(adx, 0)                                                                       AS adx
     , nvl(trex, 0)                                                                      AS trex
     , nvl(dmpex, 0)                                                                     AS dmpex
     , nvl(dmmex, 0)                                                                     AS dmmex

     , IF(rk <= 5, true, IF(last_sar_bull, IF(v_sar > closing_price, false, last_sar_bull),
                            IF(v_sar < closing_price, true, last_sar_bull)))             AS sar_bull
     , IF(rk >= 4, IF(rk = 4 or rk = 5, first_sar_low,
                      IF(last_sar_bull, IF(v_sar > closing_price, first_sar_low, last_sar_low),
                         IF(v_sar < closing_price, first_sar_low,
                            IF(today_low < last_sar_low, today_low, last_sar_low)))), 0) AS sar_low
     , IF(rk >= 4, IF(rk = 4 or rk = 5, first_sar_high, IF(last_sar_bull = 1, IF(v_sar > closing_price, first_sar_high,
                                                                                 IF(today_high > last_sar_high, today_high, last_sar_high)),
                                                           IF(v_sar < closing_price, first_sar_high, last_sar_high))),
          0)                                                                             AS sar_high
     , if(rk >= 4, IF(rk = 4 OR rk = 5, 0.02, IF(last_sar_bull, IF(v_sar > closing_price, 0.02,
                                                                   IF(today_high > last_sar_high,
                                                                      IF(last_sar_af + 0.02 > 0.2, 0.2, last_sar_af + 0.02),
                                                                      last_sar_af)), IF(v_sar < closing_price, 0.02,
                                                                                        IF(today_low < last_sar_low,
                                                                                           IF(last_sar_af + 0.02 > 0.2, 0.2, last_sar_af + 0.02),
                                                                                           last_sar_af)))),
          0.02)                                                                          AS sar_af
     , nvl(mpdi, 0)                                                                      AS mpdi
     , nvl(stor, 0)                                                                      AS stor
     , nvl(midr, 0)                                                                      AS midr
     , nvl(wekr, 0)                                                                      AS wekr
     , nvl(weks, 0)                                                                      AS weks
     , nvl(mids, 0)                                                                      AS mids
     , nvl(stos, 0)                                                                      AS stos
     , nvl(hlc, 0)                                                                       AS hlc
     , nvl(hv, 0)                                                                        AS hv
     , nvl(lv, 0)                                                                        AS lv
     , nvl(ema1L, 0)                                                                     AS ema1L
     , nvl(mtr, 0)                                                                       AS mtr
     , nvl((mtr - last_mtr) * 100 / if(last_mtr = 0, 1, last_mtr), 0)                    AS trix
     , lwr1
     , lwr2
     , nvl(ema2L, 0)                                                                     AS ema2L
FROM (
         SELECT *
              , IF(pdi + mdi <> 0, abs(mdi - pdi) * 100 / mdi + pdi, 0)     AS mpdi
              , IF(rk = 19, (mpdisum + IF(pdi + mdi <> 0, abs(mdi - pdi) * 100 / mdi + pdi, 0)) / 6,
                   (2 * IF(pdi + mdi <> 0, abs(mdi - pdi) * 100 / (
                       mdi + pdi
                       ), 0) + 5 * last_adx) / 7)                           AS adx
              , IF(last_sar_bull, last_sar + last_sar_af * (last_sar_high - last_sar),
                   last_sar + last_sar_af * (last_sar_low - last_sar))      AS v_sar
              , IF(rk = 1, closing_price, (2 * ema2L + 11 * last_mtr) / 13) AS mtr
         FROM (
                  SELECT *
                       , if(rk >= 14, IF(rk = 14, trsum / 14, (2 * tr + 13 * last_trex) / 15), 0) AS trex
                       , if(rk >= 14, IF(rk = 14, dmpsum / 14, (2 * dmp + 13 * last_dmpex) / 15),
                            0)                                                                    AS dmpex
                       , if(rk >= 14, IF(rk = 14, dmmsum / 14, (2 * dmm + 13 * last_dmmex) / 15),
                            0)                                                                    AS dmmex
                       , if(rk >= 14, IF(IF(rk = 14, trsum / 14, (2 * tr + 13 * last_trex) / 15) <> 0,
                                         IF(rk = 14, dmpsum / 14, (2 * dmp + 13 * last_dmpex) / 15) * 100 / (
                                             IF(rk = 14, (trsum) / 14, (2 * tr + 13 * last_trex) / 15)
                                             ), 0),
                            0)                                                                    AS pdi
                       , if(rk >= 14, IF(IF(rk = 14, (trsum) / 14, (2 * tr + 13 * last_trex) / 15) <> 0,
                                         IF(rk = 14, (dmmsum) / 14, (2 * dmm + 13 * last_dmmex) / 15) * 100 / (
                                             IF(rk = 14, (trsum) / 14, (2 * tr + 13 * last_trex) / 15)
                                             ), 0),
                            0)                                                                    AS mdi
                       , IF(rk = 1, rsv, (rsv + 2 * last_k) / 3)                                  AS k
                       , IF(rk = 1, rsv, ((rsv + 2 * last_k) / 3 + 2 * last_d) / 3)               AS d
                       , IF(rk = 1, rsv, 3 * (rsv + 2 * last_k) / 3 - 2 * ((rsv + 2 * last_k) / 3 + 2 * last_d) /
                                                                      3)                          AS j
                       , ema12 - ema26                                                            AS dif
                       , (2 * (ema12 - ema26) + 8 * last_dea) / 10                                AS dea
                       , (ema12 - ema26 - ((2 * (ema12 - ema26) + 8 * last_dea) / 10)) * 2        AS macd
                       , IF(rk = 1, closing_price, (2 * ema1L + 11 * last_ema2L) / 13)            AS ema2L
                       , IF(rk = 1, lwr1, (lwr1 + 2 * last_lwr2) / 3)                             AS lwr2
                  FROM (
                           SELECT *
                                , round(up6 * 100 / (up6 + down6), 3)                              AS rsi6
                                , round(up12 * 100 / (up12 + down12), 3)                           AS rsi12
                                , round(up24 * 100 / (up24 + down24), 3)                           AS rsi24
                                , IF(hd > 0 AND hd > ld, hd, 0)                                    AS dmp
                                , IF(ld > 0 AND ld > hd, ld, 0)                                    AS dmm
                                , IF(csl > hsc, IF(csl > hsl, csl, hsl), IF(hsc > hsl, hsc, hsl))  AS tr
                                , IF(rk = 1, 2 * hv - lv, ((2 * hv - lv) * 2 + 2 * last_stor) / 4) AS stor
                                , IF(rk = 1, 2 * lv - hv, ((2 * lv - hv) * 2 + 2 * last_stos) / 4) AS stos
                                , IF(rk >= 11, IF(rk = 11, hlc + hv - lv, (2 * (hlc + hv - lv) + 2 * last_midr) / 4),
                                     0)                                                            AS midr
                                , IF(rk >= 11, IF(rk = 11, hlc * 2 - lv, (2 * (hlc * 2 - lv) + 2 * last_wekr) / 4),
                                     0)                                                            AS wekr
                                , IF(rk >= 11, IF(rk = 11, hlc * 2 - hv, (2 * (hlc * 2 - hv) + 2 * last_weks) / 4),
                                     0)                                                            AS weks
                                , IF(rk >= 11, IF(rk = 11, hlc - hv + lv, (2 * (hlc - hv + lv) + 2 * last_mids) / 4),
                                     0)                                                            AS mids
                                , IF(rk = 1, closing_price, (2 * closing_price + 11 * nvl(last_ema12, 0)) /
                                                            13)                                    AS ema12
                                , IF(rk = 1, closing_price, (2 * closing_price + 25 * nvl(last_ema26, 0)) /
                                                            27)                                    AS ema26
                                , IF(rk = 1, closing_price, (2 * closing_price + 11 * nvl(last_ema1L, 0)) /
                                                            13)                                    AS ema1L
                                , IF(rk = 1, (highest9 - closing_price) * 100 / h9_l9,
                                     ((highest9 - closing_price) * 100 / h9_l9 + 2 * last_lwr1) /
                                     3)                                                            AS lwr1
                           FROM (
                                    SELECT a.code
                                         , a.name
                                         , a.ds                                                              AS ds
                                         , nvl(b.rk, 0) + 1                                                  AS rk
                                         , a.current_price                                                   AS closing_price
                                         , IF(b.rk IS NULL, 0, (a.up + b.up6 * 5) / 6)                       AS up6
                                         , (a.dn + nvl(b.down6, 0) * 5) / 6                                  AS down6
                                         , IF(b.rk IS NULL, 0, (a.up + b.up12 * 11) / 12)                    AS up12
                                         , (a.dn + nvl(b.down12, 0) * 11) / 12                               AS down12
                                         , IF(b.rk IS NULL, 0, (a.up + b.up24 * 23) / 24)                    AS up24
                                         , (a.dn + nvl(b.down24, 0) * 23) / 24                               AS down24
                                         , a.closing_diff
                                         , a.t_1_price                                                       AS last_closing
                                         , nvl(hhv, a.highest)                                               AS hhv
                                         , nvl(llv, a.lowest)                                                AS llv
                                         , nvl(b.k, 0)                                                       AS last_k
                                         , nvl(b.d, 0)                                                       AS last_d
                                         , nvl(b.j, 0)                                                       AS last_j
                                         , b.dea                                                             AS last_dea
                                         , b.trex                                                            AS last_trex
                                         , b.dmpex                                                           AS last_dmpex
                                         , b.dmmex                                                           AS last_dmmex
                                         , a.highest                                                         AS today_high
                                         , a.lowest                                                          AS today_low
                                         , b.adx                                                             AS last_adx
                                         , a.highest - b.highest                                             AS hd
                                         , b.lowest - a.lowest                                               AS ld
                                         , abs(b.closing_price - a.lowest)                                   AS csl
                                         , abs(a.highest - t_1_price)                                        AS hsc
                                         , a.highest - a.lowest                                              AS hsl
                                         , b.sar_bull                                                        AS last_sar_bull
                                         , nvl(b.sar_high, 0)                                                AS last_sar_high
                                         , nvl(b.sar_low, 0)                                                 AS last_sar_low
                                         , nvl(b.sar_af, 0.02)                                               AS last_sar_af
                                         , b.sar                                                             AS last_sar
                                         , b.highest                                                         AS last_highest
                                         , b.lowest                                                          AS last_lowest
                                         , a.highest                                                         AS new_highest
                                         , a.lowest                                                          AS new_lowest
                                         , IF(a.current_price > a.t_1_price, b.obv + a.deal_amount,
                                              IF(a.current_price < a.t_1_price, b.obv - a.deal_amount, obv)) AS obv
                                         , a.deal_amount                                                     AS deal_amount
                                         , nvl(c.trsum, 0)                                                   as trsum
                                         , nvl(c.dmpsum, 0)                                                  as dmpsum
                                         , nvl(c.dmmsum, 0)                                                  as dmmsum
                                         , nvl(d.mpdisum, 0)                                                 as mpdisum
                                         , a.sar_high                                                        AS first_sar_high
                                         , a.sar_low                                                         AS first_sar_low
                                         , IF(b.hv IS NULL, a.hhv10, (b.hv * 2 + 2 * a.hhv10) / 4)           AS hv
                                         , IF(b.lv IS NULL, a.llv10, (b.lv * 2 + 2 * a.llv10) / 4)           AS lv
                                         , nvl(b.stor, 0)                                                    AS last_stor
                                         , nvl(b.midr, 0)                                                    AS last_midr
                                         , nvl(b.wekr, 0)                                                    AS last_wekr
                                         , nvl(b.stos, 0)                                                    AS last_stos
                                         , nvl(b.weks, 0)                                                    AS last_weks
                                         , nvl(b.mids, 0)                                                    AS last_mids
                                         , a.hlc
                                         , nvl(b.ema1L, 0)                                                   AS last_ema1L
                                         , nvl(b.mtr, b.mtr)                                                 AS last_mtr
                                         , nvl(b.ema2L, 0)                                                   AS last_ema2L
                                         , nvl(b.ema12, 0)                                                   AS last_ema12
                                         , nvl(b.ema26, 0)                                                   AS last_ema26
                                         , a.opening_price
                                         , nvl(b.lwr1, 0)                                                    AS last_lwr1
                                         , nvl(b.lwr2, 0)                                                    AS last_lwr2
                                         , a.highest9
                                         , h9_l9
                                         , a.rsv
                                    FROM (
                                             SELECT IF(current_price - t_1_price > 0, current_price - t_1_price, 0)      AS up
                                                  , abs(IF(current_price - t_1_price < 0, current_price - t_1_price, 0)) AS dn
                                                  , a.name
                                                  , a.code
                                                  , a.ds
                                                  , a.current_price
                                                  , a.highest
                                                  , a.lowest
                                                  , a.current_price - t_1_price                                          AS closing_diff
                                                  , t_1_price
                                                  , a.deal_vol                                                           AS deal_amount
                                                  , b.hhv10
                                                  , b.llv10
                                                  , b.hlc
                                                  , a.opening_price
                                                  , b.highest9
                                                  , b.lowest9
                                                  , IF((b.highest9 - b.lowest9) = 0, 1, b.highest9 - b.lowest9)          AS h9_l9
                                                  , hhv
                                                  , llv
                                                  , rsv
                                                  , b.sar_low
                                                  , b.sar_high
                                             FROM ods_a_stock_detail_day a
                                                      LEFT JOIN ods_stock_step_one b
                                                                ON a.code = b.code
                                                                    AND a.ds = b.ds
                                             WHERE a.ds = '2022-09-02'
                                         ) a -- 今日数据
                                             LEFT JOIN (
                                        SELECT t1.*
                                        FROM ods_a_stock_recursive_index t1
                                                 inner join (
                                            SELECT code
                                                 , MAX(rk) AS rk
                                            FROM ods_a_stock_recursive_index
                                            GROUP BY code
                                        ) t2 on t1.code = t2.code and t1.rk = t2.rk
                                    ) b -- 昨日数据
                                                       ON a.code = b.code
                                             LEFT JOIN (
                                        SELECT code
                                             , SUM(tr)  AS trsum
                                             , SUM(dmp) AS dmpsum
                                             , SUM(dmm) AS dmmsum
                                        FROM ods_stock_step_one
                                        GROUP BY code
                                        HAVING COUNT(rk) = 14
                                    ) c
                                                       ON a.code = c.code -- 专为第14日pdi和mdi指标设计
                                             LEFT JOIN (
                                        SELECT code
                                             , SUM(mpdi) AS mpdisum
                                        FROM ods_a_stock_recursive_index
                                        GROUP BY code
                                        HAVING COUNT(rk) = 18
                                    ) d -- 专为adx指标设计
                                                       ON a.code = d.code
                                ) t1
                       ) t2
              ) t3
     ) t4
;




