{ { config(materialized = 'table') } } with date_rank as (
      select
            D_DATE,
            D_DATE_SK,
            D_WEEK_SEQ,
            rank() over (
                  partition by D_WEEK_SEQ
                  order by
                        D_DATE_SK
            ) as day_of_week
      from
            "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."DATE_DIM"
      order by
            D_DATE
),
profit_sale_wkly_wkly as (
      with profit_sale_wkly as (
            select
                  a.WEEKID,
                  a.STOREID,
                  a.NETPROFIT,
                  b.SALECOUNT
            from
                  "DEMO_DB"."PUBLIC"."PROFIT_BY_STORE_WKLY" a
                  inner join "DEMO_DB"."PUBLIC"."SALE_BY_STORE_WKLY" b on a.WEEKID = b.WEEKID
                  and a.STOREID = b.STOREID
      )
      select
            min(b.D_DATE) as DATE_MON,
            a.WEEKID,
            a.STOREID,
            a.NETPROFIT,
            a.SALECOUNT
      from
            profit_sale_wkly a
            inner join date_rank b on a.WEEKID = b.D_WEEK_SEQ
      group by
            (a.STOREID, a.NETPROFIT, a.SALECOUNT, a.WEEKID)
),
profit_sale_dly_wkly as (
      with profit_sale_dly as (
            select
                  a.SOLDDATE,
                  a.STOREID,
                  a.NETPROFIT,
                  b.SALECOUNT
            from
                  "DEMO_DB"."PUBLIC"."PROFIT_BY_STORE_DLY" a
                  inner join "DEMO_DB"."PUBLIC"."SALE_BY_STORE_DLY" b on a.SOLDDATE = b.SOLDDATE
                  and a.STOREID = b.STOREID
      )
      select
            b.D_DATE_SK,
            a.SOLDDATE,
            a.STOREID,
            a.NETPROFIT,
            a.SALECOUNT,
            b.D_WEEK_SEQ,
            b.day_of_week
      from
            profit_sale_dly a
            inner join date_rank b on a.SOLDDATE = b.D_DATE
)
select
      'Weekly' as Frequency,
      'Prev_Week' as Comparison_Type,
      a.DATE_MON as TimePeriodCurrent,
      b.DATE_MON as TimePeriodComparison,
      null as DimIndex,
      null as Dim1_ID,
      'Store_ID' as Dim1_Name,
      a.STOREID as Dim1_Value,
      'Net Profit' as Metric_1_name,
      null as Metric_1_Direction,
      null as Metric_1_Type,
      a.NETPROFIT as Metric_1_Value_Current,
      b.NETPROFIT as Metric_1_Value_Comparison,
      'Net Sales' as Metric_2_name,
      null as Metric_2_Direction,
      null as Metric_2_Type,
      a.SALECOUNT as Metric_2_Value_Current,
      b.SALECOUNT as Metric_2_Value_Comparison
from
      profit_sale_wkly_wkly a
      inner join profit_sale_wkly_wkly b on (
            a.STOREID = b.STOREID
            and a.WEEKID = b.WEEKID -1
      )
union
all
select
      'Daily' as Frequency,
      'Prev_Week' as Comparison_Type,
      a.SOLDDATE as TimePeriodCurrent,
      b.SOLDDATE as TimePeriodComparison,
      null as DimIndex,
      null as Dim1_ID,
      'Store_ID' as Dim1_Name,
      a.STOREID as Dim1_Value,
      'Net Profit' as Metric_1_name,
      null as Metric_1_Direction,
      null as Metric_1_Type,
      a.NETPROFIT as Metric_1_Value_Current,
      b.NETPROFIT as Metric_1_Value_Comparison,
      'Net Sales' as Metric_2_name,
      null as Metric_2_Direction,
      null as Metric_2_Type,
      a.SALECOUNT as Metric_2_Value_Current,
      b.SALECOUNT as Metric_2_Value_Comparison
from
      profit_sale_dly_wkly a
      inner join profit_sale_dly_wkly b on (
            a.STOREID = b.STOREID
            and a.day_of_week = b.day_of_week
            and a.D_WEEK_SEQ = b.D_WEEK_SEQ -1
      )