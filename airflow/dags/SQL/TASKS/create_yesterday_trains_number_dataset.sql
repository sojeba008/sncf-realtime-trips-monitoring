WITH hours AS (
    SELECT generate_series(
        date_trunc('hour', now()::DATE - INTERVAL '1 days' - INTERVAL '23 hours'),
        date_trunc('hour', now()::DATE- INTERVAL '1 hour'),
        INTERVAL '1 hour'
    ) AS hour_start
)
,enriched_hours AS (
	SELECT hour_start,
        d_d.is_public_holiday,
		CASE 
			WHEN  to_char(d_d.date, 'D')::INT4 IN (1,7) THEN TRUE
			ELSE FALSE
		END is_weekend, 
		to_char(d_d.date, 'D')::INT4 AS day_in_the_week,
		d_d.day_in_month, d_d.is_first_day_of_month, d_d.is_first_day_of_week, d_d.is_first_month_of_quarter, d_d.is_last_day_of_month,
d_d.is_last_day_of_week, d_d.is_last_month_of_quarter
	FROM hours 
	INNER JOIN dwh.d_date d_d ON d_d.tk_date = TO_CHAR(hour_start, 'YYYYMMDD')::INT4
),
trips AS MATERIALIZED (
    SELECT
        f.trip_id,
        (d_arr.date + t_arr.hour_date) AS expected_arrival_ts,
        (d_dep.date + t_dep.hour_date) AS expected_departure_ts
    FROM dwh.f_trips f
    LEFT JOIN dwh.d_date d_arr ON d_arr.tk_date = f.expected_arrival_date_tk
    LEFT JOIN dwh.d_time t_arr ON t_arr.tk_time = f.expected_arrival_time_tk
    LEFT JOIN dwh.d_date d_dep ON d_dep.tk_date = f.expected_departure_date_tk
    LEFT JOIN dwh.d_time t_dep ON t_dep.tk_time = f.expected_departure_time_tk
),
trip_hours AS (
    SELECT
        e_h.hour_start,
        t.trip_id,
        COALESCE(e_h.is_weekend, FALSE) is_weekend,
        e_h.is_public_holiday, e_h.day_in_month, e_h.is_first_day_of_month, e_h.is_first_day_of_week, e_h.is_first_month_of_quarter, e_h.is_last_day_of_month,
e_h.is_last_day_of_week, e_h.is_last_month_of_quarter, e_h.day_in_the_week
    FROM enriched_hours e_h
    LEFT JOIN trips t
      ON t.expected_departure_ts <  e_h.hour_start + INTERVAL '1 hour'
     AND t.expected_arrival_ts   >= e_h.hour_start
)
SELECT
	TO_CHAR(h.hour_start, 'YYYY-MM-DD')::DATE AS date, 
    h.hour_start,
    h.hour_start + INTERVAL '1 hour' AS hour_end,
    th.is_weekend, th.day_in_month, th.is_first_day_of_month, th.is_first_day_of_week, th.is_first_month_of_quarter, th.is_last_day_of_month,
th.is_last_day_of_week, th.is_last_month_of_quarter, th.day_in_the_week,
    COALESCE(BOOL_OR(th.is_public_holiday), FALSE) AS is_public_holiday,
    COUNT(DISTINCT th.trip_id) AS nb_trains_actifs
FROM trip_hours th
LEFT JOIN hours h ON th.hour_start = h.hour_start
GROUP BY h.hour_start, th.is_weekend, th.day_in_month, th.is_first_day_of_month, th.is_first_day_of_week, th.is_first_month_of_quarter, th.is_last_day_of_month,
th.is_last_day_of_week, th.is_last_month_of_quarter, th.day_in_the_week
ORDER BY h.hour_start;