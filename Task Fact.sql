-- Procedure for populating daily sales with incremental data updates
CREATE OR REPLACE PROCEDURE tpcds.intermediate.populating_daily_aggregated_sales_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_SOLD_DATE_SK number;
    BEGIN
      SELECT MAX(SOLD_DATE_SK) INTO :LAST_SOLD_DATE_SK FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES; 
      -- Removing the last day's records to prepare for incremental update
    DELETE FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES WHERE sold_date_sk=:LAST_SOLD_DATE_SK;



CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP AS (
-- compiling all incremental sales records
with incremental_sales as (
SELECT 
            CS_WAREHOUSE_SK as warehouse_sk,
            CS_ITEM_SK as item_sk,
            CS_SOLD_DATE_SK as sold_date_sk,
            CS_QUANTITY as quantity,
            cs_sales_price * cs_quantity as sales_amt,
            CS_NET_PROFIT as net_profit
    from tpcds.raw.catalog_sales
    WHERE sold_date_sk >= NVL(:LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
    
    union all

    SELECT 
            WS_WAREHOUSE_SK as warehouse_sk,
            WS_ITEM_SK as item_sk,
            WS_SOLD_DATE_SK as sold_date_sk,
            WS_QUANTITY as quantity,
            ws_sales_price * ws_quantity as sales_amt,
            WS_NET_PROFIT as net_profit
    from tpcds.raw.web_sales
    WHERE sold_date_sk >= NVL(:LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
),

aggregating_records_to_daily_sales as
(
select 
    warehouse_sk,
    item_sk,
    sold_date_sk, 
    sum(quantity) as daily_qty,
    sum(sales_amt) as daily_sales_amt,
    sum(net_profit) as daily_net_profit 
from incremental_sales
group by 1, 2, 3

),

adding_week_number_and_yr_number as
(
select 
    *,
    date.wk_num as sold_wk_num,
    date.yr_num as sold_yr_num
from aggregating_records_to_daily_sales 
LEFT JOIN tpcds.raw.date_dim date 
    ON sold_date_sk = d_date_sk

)

SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) as sold_wk_num,
    max(sold_yr_num) as sold_yr_num,
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_net_profit) as daily_net_profit 
FROM adding_week_number_and_yr_number
GROUP BY 1,2,3
ORDER BY 1,2,3
)
;




-- Inserting new records
INSERT INTO TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES
(	
    WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_DATE_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    DAILY_QTY, 
    DAILY_SALES_AMT, 
    DAILY_NET_PROFIT
)
SELECT 
    DISTINCT
	warehouse_sk,
    item_sk,
    sold_date_sk,
    sold_wk_num,
    sold_yr_num,
    daily_qty,
    daily_sales_amt,
    daily_net_profit 
FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP;
  
  END
  $$;
-- Schedule the daily sales population task
CREATE OR REPLACE TASK tpcds.INTERMEDIATE.populating_daily_aggregated_sales_incrementally
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
CALL tpcds.INTERMEDIATE.populating_daily_aggregated_sales_incrementally();

-- Reactivating the task for daily execution
ALTER TASK tpcds.INTERMEDIATE.populating_daily_aggregated_sales_incrementally RESUME;

-- Initiating the daily sales population task execution
EXECUTE TASK tpcds.INTERMEDIATE.populating_daily_aggregated_sales_incrementally;

-- Cleanup: Removing the daily sales population task when no longer needed
DROP TASK tpcds.INTERMEDIATE.populating_daily_aggregated_sales_incrementally;

-- Cleanup: Removing the daily sales population procedure after its use is complete
DROP PROCEDURE tpcds.INTERMEDIATE.populating_daily_aggregated_sales_incrementally;

-- Refreshing the daily sales data by clearing existing records
TRUNCATE TABLE tpcds.INTERMEDIATE.daily_aggregated_sales;
--------------------------------------------------------------------------



-- Procedure for populating weekly sales by aggregating daily data
CREATE OR REPLACE PROCEDURE tpcds.intermediate.populating_weekly_aggregated_sales_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_SOLD_DATE_SK number;
    BEGIN
      SELECT MAX(SOLD_DATE_SK) INTO :LAST_SOLD_DATE_SK FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES; 
      -- Preparing for weekly data update by removing last week's aggregated data
      DELETE FROM TPCDS.INTERMEDIATE.WEEKLY_SALES_INVENTORY WHERE sold_wk_sk=:LAST_SOLD_WK_SK;

-- compiling all incremental sales records
CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.WEEKLY_SALES_INVENTORY_TMP AS (
with aggregating_daily_sales_to_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    MIN(SOLD_DATE_SK) AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM(DAILY_QTY) AS SUM_QTY_WK, 
    SUM(DAILY_SALES_AMT) AS SUM_AMT_WK, 
    SUM(DAILY_NET_PROFIT) AS SUM_PROFIT_WK
FROM
    TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES
GROUP BY
    1,2,4,5
HAVING 
    sold_wk_sk >= NVL(:LAST_SOLD_WK_SK,0)
),

-- We need to have the same sold_wk_sk for all the items. Currently, any items that didn't have any sales on Sunday (first day of the week) would not have Sunday date as sold_wk_sk so this CTE will correct that.
finding_first_date_of_the_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    date.d_date_sk AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK
FROM
    aggregating_daily_sales_to_week daily_sales
INNER JOIN TPCDS.RAW.DATE_DIM as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0
),

-- This will help sales and inventory tables to join together using wk_num and yr_num
date_columns_in_inventory_table as (
SELECT 
    inventory.*,
    date.wk_num as inv_wk_num,
    date.yr_num as inv_yr_num
FROM
    tpcds.raw.inventory inventory
INNER JOIN TPCDS.RAW.DATE_DIM as date
on inventory.inv_date_sk = date.d_date_sk
)

select 
       warehouse_sk, 
       item_sk, 
       min(SOLD_WK_SK) as sold_wk_sk,
       sold_wk_num as sold_wk_num,
       sold_yr_num as sold_yr_num,
       sum(sum_qty_wk) as sum_qty_wk,
       sum(sum_amt_wk) as sum_amt_wk,
       sum(sum_profit_wk) as sum_profit_wk,
       sum(sum_qty_wk)/7 as avg_qty_dy,
       sum(coalesce(inv.inv_quantity_on_hand, 0)) as inv_qty_wk, 
       sum(coalesce(inv.inv_quantity_on_hand, 0)) / sum(sum_qty_wk) as wks_sply,
       iff(avg_qty_dy>0 and avg_qty_dy>inv_qty_wk, true , false) as low_stock_flg_wk
from finding_first_date_of_the_week
left join date_columns_in_inventory_table inv 
    on inv_wk_num = sold_wk_num and inv_yr_num = sold_yr_num and item_sk = inv_item_sk and inv_warehouse_sk = warehouse_sk
group by 1, 2, 4, 5
-- extra precaution because we don't want negative or zero quantities in our final model
having sum(sum_qty_wk) > 0
);

-- Inserting new records
INSERT INTO TPCDS.INTERMEDIATE.WEEKLY_SALES_INVENTORY
(	
	WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK, 
    AVG_QTY_DY, 
    INV_QTY_WK, 
    WKS_SPLY, 
    LOW_STOCK_FLG_WK
    
)
SELECT 
    DISTINCT
	WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK, 
    AVG_QTY_DY, 
    INV_QTY_WK, 
    WKS_SPLY, 
    LOW_STOCK_FLG_WK
FROM TPCDS.INTERMEDIATE.WEEKLY_SALES_INVENTORY_TMP;

select * from TPCDS.INTERMEDIATE.WEEKLY_SALES_INVENTORY;
  END
  $$;
  -- Schedule the weekly sales aggregation task
CREATE OR REPLACE TASK tpcds.INTERMEDIATE.populating_weekly_aggregated_sales_incrementally
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 9 * * * UTC'
AS
CALL tpcds.INTERMEDIATE.populating_weekly_aggregated_sales_incrementally();

-- Resume task to continue regular weekly updates
ALTER TASK tpcds.INTERMEDIATE.populating_weekly_aggregated_sales_incrementally RESUME;

-- Immediate execution of the weekly sales update task
EXECUTE TASK tpcds.INTERMEDIATE.populating_weekly_aggregated_sales_incrementally;

-- Drop the task if it is no longer needed
DROP TASK tpcds.INTERMEDIATE.populating_weekly_aggregated_sales_incrementally;

-- Removing the procedure for weekly sales updates after completion
DROP PROCEDURE tpcds.INTERMEDIATE.populating_weekly_aggregated_sales_incrementally;

-- Resetting the weekly sales inventory table for new data
TRUNCATE TABLE tpcds.INTERMEDIATE.daily_aggregated_sales;
