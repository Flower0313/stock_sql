select code, name, pe_ratio_s, up_down_rate, main_inflow_ratio, large_inflow_ratio, mid_inflow_ratio
from ods_a_stock_detail_day
where dt = '2022-08-17'
  and up_down_rate > 0
order by up_down_rate desc;


select code
     , sum(if(buyorsale = 2, dealnum, 0))     as buy1
     , sum(if(buyorsale = 1, dealnum, 0))     as sell1
     , sum(if(buyorsale = 2, draw, 0))        as buy2
     , sum(if(buyorsale = 1, draw, 0))        as sell2
     , sum(if(buyorsale = 2, draw * deal, 0)) as buy3
     , sum(if(buyorsale = 1, draw * deal, 0)) as sell3
from ods_a_stock_deal
where dt = '2022-08-17'
  and code = '001258'
group by code
;


select main_inflow + slarge_inflow + large_inflow + mid_inflow + small_inflow
from ods_a_stock_detail_day
where dt = '2022-08-17'
  and code = '001258'
;





