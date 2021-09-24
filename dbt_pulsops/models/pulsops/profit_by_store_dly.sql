{{ config(materialized='table') }}


with nps_daily as (
with 
store as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE" )
,storesales as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" limit 1000)
,datedim as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."DATE_DIM" )
select a.S_STORE_ID as storeid,a.S_STORE_NAME as storename,b.SS_NET_PROFIT as netprofit, c.D_DATE as solddate from store a 
inner join storesales b on a.S_STORE_SK=b.SS_STORE_SK
inner join datedim c on b.SS_SOLD_DATE_SK=c.D_DATE_SK
)

select storeid , solddate , sum(netprofit) as netprofit from nps_daily group by storeid, solddate
