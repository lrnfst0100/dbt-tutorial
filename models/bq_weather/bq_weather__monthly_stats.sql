WITH base AS (
SELECT  
    EXTRACT(YEAR FROM t1.date_valid_std) as year,
    EXTRACT(Month FROM t1.date_valid_std) as month,
    t1.avg_temperature_air_2m_f as avg_temp
FROM {{ source('weathersource-com', 'sample_weather_and_crime_comparison_chicago_daily_2016') }} as t1
ORDER BY 1 DESC, 2 DESC
LIMIT 1000
),
avg_temp_table AS (
  select *,
  AVG(base.avg_temp) OVER (PARTITION BY base.year, base.month) as m_avg_temp
  from base
),
agg_t AS (
  select
  avg_temp_table.year as w_year,
  avg_temp_table.month as w_month,
  MAX(avg_temp_table.m_avg_temp) as w_max_temp
  from avg_temp_table
  GROUP BY 1,2
),
final AS (
  select * from agg_t
  order by 1,2 desc
)

select * from final
