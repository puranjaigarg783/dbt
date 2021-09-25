{{ config(materialized='table') }}

with nps_weekly as 
(
with 
store as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE" )
,storesales as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE_SALES")
,datedim as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."DATE_DIM" )
select a.S_STORE_ID as storeid,a.S_STORE_NAME as storename,c.D_WEEK_SEQ as weekid,b.SS_NET_PROFIT as netprofit from store a 
inner join storesales b on a.S_STORE_SK=b.SS_STORE_SK
inner join datedim c on b.SS_SOLD_DATE_SK=c.D_DATE_SK
)

select storeid, weekid, sum(netprofit) as netprofit from nps_weekly group by storeid, weekid


