{{ config(materialized='table') }}


with sbc_daily as 
(
with 
storesales as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE_SALES")
,items as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."ITEM")
,datedim as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."DATE_DIM" )
select a.SS_TICKET_NUMBER as sale, b.I_CATEGORY as category,c.D_DATE as solddate from storesales a 
inner join items b on a.SS_ITEM_SK=b.I_ITEM_SK
inner join datedim c on a.SS_SOLD_DATE_SK=c.D_DATE_SK
)

select category, solddate , count(sale) as salecount from sbc_daily group by category, solddate
