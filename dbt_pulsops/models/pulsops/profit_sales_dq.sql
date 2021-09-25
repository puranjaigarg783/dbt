{{ config(materialized='table') }}



with date_rank as (
select D_DATE,D_DATE_SK,D_WEEK_SEQ, rank() over (
  partition by D_WEEK_SEQ order by D_DATE_SK
    ) as day_of_week from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."DATE_DIM"
        order by D_DATE
)

, profit_sale_wkly_wkly as (
with profit_sale_wkly as (
select a.WEEKID,a.STOREID,a.NETPROFIT,b.SALECOUNT from "DEMO_DB"."PUBLIC"."PROFIT_BY_STORE_WKLY" a
inner join "DEMO_DB"."PUBLIC"."SALE_BY_STORE_WKLY" b
on a.WEEKID = b.WEEKID
)

select a.WEEKID,a.STOREID,a.NETPROFIT,a.SALECOUNT, b.D_WEEK_SEQ, b.day_of_week from profit_sale_wkly a
inner join date_rank b 
on a.WEEKID = b.D_WEEK_SEQ
)

, profit_sale_dly_wkly as (
with profit_sale_dly as (
select a.SOLDDATE,a.STOREID,a.NETPROFIT,b.SALECOUNT from "DEMO_DB"."PUBLIC"."PROFIT_BY_STORE_DLY" a
inner join "DEMO_DB"."PUBLIC"."SALE_BY_STORE_DLY" b
on a.SOLDDATE = b.SOLDDATE
)

select b.D_DATE_SK,a.SOLDDATE,a.STOREID,a.NETPROFIT,a.SALECOUNT, b.D_WEEK_SEQ, b.day_of_week from profit_sale_dly a
inner join date_rank b 
on a.SOLDDATE = b.D_DATE
)

select a.STOREID, a.WEEKID as tp_current,b.WEEKID as tp_comparison, (a.NETPROFIT-b.NETPROFIT) as profit_change, (a.SALECOUNT-b.SALECOUNT) as sales_change from profit_sale_wkly_wkly a
inner join profit_sale_wkly_wkly b 
on (a.STOREID=b.STOREID and a.D_WEEK_SEQ = b.D_WEEK_SEQ -1)

union all 

select a.STOREID, a.D_DATE_SK as tp_current,b.D_DATE_SK as tp_comparison, (a.NETPROFIT-b.NETPROFIT) as profit_change, (a.SALECOUNT-b.SALECOUNT) as sales_change from profit_sale_dly_wkly a
inner join profit_sale_dly_wkly b 
on (a.STOREID=b.STOREID and a.day_of_week=b.day_of_week and a.D_WEEK_SEQ = b.D_WEEK_SEQ -1)


