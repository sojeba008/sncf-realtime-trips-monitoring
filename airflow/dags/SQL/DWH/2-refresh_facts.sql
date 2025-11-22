INSERT INTO dwh.f_trips (
      trip_id,
      vehicule_tk,
      num_vehicule,
      ref_date_tk,
      stop_station_tk,
      origin_station_tk,
      destination_station_tk,
	  line_tk,
      aimed_arrival_date_tk,
      aimed_arrival_time_tk,
      expected_arrival_date_tk,
      expected_arrival_time_tk,
      aimed_departure_date_tk,
      aimed_departure_time_tk,
      expected_departure_date_tk,
      expected_departure_time_tk,
      delay_arrival_minutes,
      delay_departure_minutes,  
      departure_platform_name,
      arrival_platform_name,
      is_starting_point,
      is_terminus
)
WITH src AS (
    SELECT
        s.trip_id,
        t.vehicule AS num_vehicule,
        s.stop_name,
        s.is_starting_point::boolean,
        s.is_terminus::boolean,
        s.ref_date,
        s.aimed_arrival,
        s.expected_arrival,
        s.aimed_departure,
        s.expected_departure,
        t.origin_name,
        t.dest_name,
        t.departure_time AS departure_ref_date,
        s.departure_platform_name,
        s.arrival_platform_name,
        t.published_line_name
    FROM ods.stops  s
    JOIN ods.trips  t USING (trip_id, ref_date)
    WHERE t.departure_time::DATE >= current_date::DATE - interval '1 day'
)
SELECT
    s.trip_id,
    dv.tk_vehicule,
    s.num_vehicule,
    d_ref.tk_date,
    COALESCE(st_stop.tk_station, -1),
    COALESCE(st_origin.tk_station, -1),
    COALESCE(st_dest.tk_station, -1),
    COALESCE(dl.tk_line, -1),
    COALESCE(d_aim_arr.tk_date, -1),
    COALESCE(tm_aim_arr.tk_time, -1),
    COALESCE(d_exp_arr.tk_date, -1),
    COALESCE(tm_exp_arr.tk_time, -1),
    COALESCE(d_aim_dep.tk_date, -1),
    COALESCE(tm_aim_dep.tk_time, -1),
    COALESCE(d_exp_dep.tk_date, -1),
    COALESCE(tm_exp_dep.tk_time, -1),
    COALESCE(EXTRACT(EPOCH FROM (s.expected_arrival  - s.aimed_arrival ))/60::int, 0)  AS delay_arrival_minutes,
    COALESCE(EXTRACT(EPOCH FROM (s.expected_departure - s.aimed_departure))/60::int, 0) AS delay_departure_minutes,
    s.departure_platform_name,
    s.arrival_platform_name,
    s.is_starting_point,
    s.is_terminus
FROM src s
INNER JOIN dwh.d_vehicule dv ON dv.num_vehicule = s.num_vehicule
-- === Main Dates ===
JOIN dwh.d_date d_ref       ON d_ref.date = s.departure_ref_date::DATE
-- ==== Stations Dim ===
JOIN dwh.d_station st_stop   ON st_stop.station_name  = s.stop_name
LEFT JOIN dwh.d_station st_origin ON st_origin.station_name = s.origin_name
LEFT JOIN dwh.d_station st_dest   ON st_dest.station_name   = s.dest_name
--- Line Dim ---
LEFT JOIN dwh.d_line dl ON dl.line_name=s.published_line_name
-- ==== Dates & Time ===
LEFT JOIN dwh.d_date d_aim_arr  ON d_aim_arr.date = s.aimed_arrival::date
LEFT JOIN dwh.d_date d_exp_arr  ON d_exp_arr.date = s.expected_arrival::date
LEFT JOIN dwh.d_date d_aim_dep  ON d_aim_dep.date = s.aimed_departure::date
LEFT JOIN dwh.d_date d_exp_dep  ON d_exp_dep.date = s.expected_departure::date
LEFT JOIN dwh.d_time tm_aim_arr ON tm_aim_arr.tk_time = to_char(s.aimed_arrival,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_exp_arr ON tm_exp_arr.tk_time = to_char(s.expected_arrival,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_aim_dep ON tm_aim_dep.tk_time = to_char(s.aimed_departure,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_exp_dep ON tm_exp_dep.tk_time = to_char(s.expected_departure,'HH24MISS')::INT8
ON CONFLICT (trip_id, stop_station_tk, ref_date_tk) DO NOTHING;


INSERT INTO dwh.f_journey (
    trip_id,
    vehicule_tk,
    num_vehicule,
    ref_date_tk,
    origin_station_tk,
    destination_station_tk,
    line_tk,
    departure_journey_date_tk,
    departure_journey_time_tk,
    arrival_journey_date_tk,
    arrival_journey_time_tk,
    insert_date
)
WITH src AS (
    SELECT
        t.trip_id,
        t.vehicule AS num_vehicule,
        t.ref_date,
        t.origin_name,
        t.dest_name,
        t.departure_time AS aimed_departure,
        t.arrival_time   AS aimed_arrival,
        t.published_line_name
    FROM ods.trips t
    WHERE t.departure_time::DATE >= current_date::DATE - interval '1 day'
)
SELECT
    s.trip_id,
    dv.tk_vehicule,
    s.num_vehicule,
    d_ref.tk_date AS ref_date_tk,
    COALESCE(st_origin.tk_station, -1)       AS origin_station_tk,
    COALESCE(st_dest.tk_station, -1)         AS destination_station_tk,
    COALESCE(dl.tk_line, -1)				 AS line_tk,
    COALESCE(d_dep.tk_date, -1)              AS departure_journey_date_tk,
    COALESCE(tm_dep.tk_time, -1)             AS departure_journey_time_tk,
    COALESCE(d_arr.tk_date, -1)              AS arrival_journey_date_tk,
    COALESCE(tm_arr.tk_time, -1)             AS arrival_journey_time_tk,
    clock_timestamp()
FROM src s
JOIN dwh.d_date d_ref ON d_ref.date = s.ref_date::date
INNER JOIN dwh.d_vehicule dv ON dv.num_vehicule = s.num_vehicule
LEFT JOIN dwh.d_station st_origin ON st_origin.station_name = s.origin_name
LEFT JOIN dwh.d_station st_dest   ON st_dest.station_name   = s.dest_name
LEFT JOIN dwh.d_line dl ON dl.line_name=s.published_line_name
LEFT JOIN dwh.d_date d_dep ON d_dep.date = s.aimed_departure::date
LEFT JOIN dwh.d_date d_arr ON d_arr.date = s.aimed_arrival::date
LEFT JOIN dwh.d_time tm_dep ON tm_dep.tk_time = to_char(s.aimed_departure,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_arr ON tm_arr.tk_time = to_char(s.aimed_arrival,'HH24MISS')::INT8
ON CONFLICT (trip_id, ref_date_tk) DO NOTHING;





TRUNCATE TABLE dwh.f_trips_realtime RESTART IDENTITY;
INSERT INTO dwh.f_trips_realtime (
    trip_id,
    vehicule_tk,
    num_vehicule,
    ref_date,
    stop_station_tk,
    origin_station_tk,
    destination_station_tk,
    line_tk,
    aimed_arrival_date_tk,
    aimed_arrival_time_tk,
    expected_arrival_date_tk,
    expected_arrival_time_tk,
    aimed_departure_date_tk,
    aimed_departure_time_tk,
    expected_departure_date_tk,
    expected_departure_time_tk,
    aimed_departure,
    aimed_arrival,
    expected_departure,
    expected_arrival,
    delay_arrival_minutes,
    delay_departure_minutes,
    departure_time_journey,
    arrival_time_journey,
    departure_platform_name,
    arrival_platform_name,
    is_starting_point,
    is_terminus
)
WITH src AS (
    SELECT
        s.trip_id,
        t.vehicule AS num_vehicule,
        s.stop_name,
        s.is_starting_point::boolean,
        s.is_terminus::boolean,
        s.ref_date,
        s.aimed_arrival,
        s.expected_arrival,
        s.aimed_departure,
        s.expected_departure,
        t.departure_time AS departure_time_journey,
        t.arrival_time AS arrival_time_journey,
        t.origin_name,
        t.dest_name,
        s.departure_platform_name,
        s.arrival_platform_name,
        t.published_line_name
    FROM ods.stops s
    JOIN ods.trips t USING (trip_id, ref_date)
    WHERE s.ref_date::DATE >= (CURRENT_DATE - INTERVAL '2 day')
)
SELECT
    s.trip_id,
    dv.tk_vehicule,
    s.num_vehicule,
    NOW()::TIMESTAMP AS tk_date,
    COALESCE(st_stop.tk_station, -1),
    COALESCE(st_origin.tk_station, -1),
    COALESCE(st_dest.tk_station, -1),
    COALESCE(dl.tk_line, -1),
    COALESCE(d_aim_arr.tk_date, -1),
    COALESCE(tm_aim_arr.tk_time, -1),
    COALESCE(d_exp_arr.tk_date, -1),
    COALESCE(tm_exp_arr.tk_time, -1),
    COALESCE(d_aim_dep.tk_date, -1),
    COALESCE(tm_aim_dep.tk_time, -1),
    COALESCE(d_exp_dep.tk_date, -1),
    COALESCE(tm_exp_dep.tk_time, -1),
    s.aimed_departure,
    s.aimed_arrival,
    s.expected_departure,
    s.expected_arrival,
    COALESCE(EXTRACT(EPOCH FROM (s.expected_arrival  - s.aimed_arrival ))/60::int, 0),
    COALESCE(EXTRACT(EPOCH FROM (s.expected_departure - s.aimed_departure))/60::int, 0),
    departure_time_journey,
    arrival_time_journey,
    s.departure_platform_name,
    s.arrival_platform_name,
    s.is_starting_point,
    s.is_terminus
FROM src s
JOIN dwh.d_date d_ref ON d_ref.date = s.ref_date
INNER JOIN dwh.d_vehicule dv ON dv.num_vehicule = s.num_vehicule
LEFT JOIN dwh.d_station st_stop   ON st_stop.station_name  = s.stop_name
LEFT JOIN dwh.d_station st_origin ON st_origin.station_name = s.origin_name
LEFT JOIN dwh.d_station st_dest   ON st_dest.station_name   = s.dest_name
LEFT JOIN dwh.d_line dl ON dl.line_name=s.published_line_name
LEFT JOIN dwh.d_date d_aim_arr  ON d_aim_arr.date = s.aimed_arrival::date
LEFT JOIN dwh.d_date d_exp_arr  ON d_exp_arr.date = s.expected_arrival::date
LEFT JOIN dwh.d_date d_aim_dep  ON d_aim_dep.date = s.aimed_departure::date
LEFT JOIN dwh.d_date d_exp_dep  ON d_exp_dep.date = s.expected_departure::date
LEFT JOIN dwh.d_time tm_aim_arr ON tm_aim_arr.tk_time = to_char(s.aimed_arrival,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_exp_arr ON tm_exp_arr.tk_time = to_char(s.expected_arrival,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_aim_dep ON tm_aim_dep.tk_time = to_char(s.aimed_departure,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_exp_dep ON tm_exp_dep.tk_time = to_char(s.expected_departure,'HH24MISS')::INT8
ON CONFLICT DO NOTHING;

INSERT INTO dwh.f_line_metrics (
    line_tk,
    ref_date_tk,
    nb_journey,
    nb_delay,
    delay_rate,
    delay_minutes,
    avg_delay_minutes
)
SELECT
    t.line_tk,
    t.expected_arrival_date_tk AS ref_date_tk,
    COUNT(DISTINCT t.trip_id) AS nb_journey,
    COUNT(DISTINCT t.trip_id) FILTER (
        WHERE t.delay_arrival_minutes > 0
    ) AS nb_delay,  
    ROUND(
        100.0 * COUNT(DISTINCT t.trip_id) FILTER (WHERE t.delay_arrival_minutes > 5)
        / NULLIF(COUNT(DISTINCT t.trip_id), 0),
    2) AS delay_rate,      
    COALESCE(SUM(t.delay_arrival_minutes) FILTER (WHERE t.delay_arrival_minutes > 0), 0) AS delay_minutes,
    ROUND(AVG(t.delay_arrival_minutes) FILTER (WHERE t.delay_arrival_minutes > 0), 2) AS avg_delay_minutes 
FROM dwh.f_trips t
INNER JOIN dwh.d_date dd ON dd.tk_date = t.expected_arrival_date_tk 
WHERE t.line_tk IS NOT NULL AND dd.date>=NOW()::DATE-1
GROUP BY t.line_tk, t.expected_arrival_date_tk
ON CONFLICT (line_tk, ref_date_tk) DO UPDATE
SET 
    nb_journey = EXCLUDED.nb_journey,
    nb_delay = EXCLUDED.nb_delay,
    delay_rate = EXCLUDED.delay_rate,
    delay_minutes = EXCLUDED.delay_minutes,
    avg_delay_minutes = EXCLUDED.avg_delay_minutes,
    insert_date = clock_timestamp();


INSERT INTO dwh.f_station_platform_usage (
    station_tk,
    platform_name,
    ref_date_tk,
    nb_arrivals,
    nb_departures,
    nb_departures_delayed,
    nb_arrivals_delayed,
    avg_delay_minutes,
    max_delay_minutes,
    total_delay_minutes
)
SELECT
    station_tk,
    platform_name,
    ref_date_tk,
    COUNT(*) FILTER (WHERE event_type = 'arrival')      AS nb_arrivals,
    COUNT(*) FILTER (WHERE event_type = 'departure')    AS nb_departures,
    COUNT(*) FILTER (
        WHERE event_type = 'arrival'
          AND delay_minutes > 0
    ) AS nb_arrivals_delayed,
    COUNT(*) FILTER (
        WHERE event_type = 'departure'
          AND delay_minutes > 0
    ) AS nb_departures_delayed,
    AVG(NULLIF(delay_minutes, 0))                       AS avg_delay_minutes,
    MAX(delay_minutes)                                  AS max_delay_minutes,
    SUM(delay_minutes)									AS total_delay_minutes
FROM (
    SELECT
        stop_station_tk AS station_tk,
        arrival_platform_name AS platform_name,
        t.expected_arrival_date_tk AS  ref_date_tk,
        'arrival' AS event_type,
        GREATEST(COALESCE(delay_arrival_minutes, 0), 0) AS delay_minutes
    FROM dwh.f_trips t
    INNER JOIN dwh.d_date dd ON dd.tk_date = t.expected_arrival_date_tk 
    WHERE arrival_platform_name IS NOT NULL AND arrival_platform_name<> '' AND dd.date>=NOW()::DATE-1
    UNION ALL
    SELECT
        stop_station_tk AS station_tk,
        departure_platform_name AS platform_name,
        t.expected_arrival_date_tk AS  ref_date_tk,
        'departure' AS event_type,
        GREATEST(COALESCE(delay_departure_minutes, 0), 0) AS delay_minutes
    FROM dwh.f_trips t
    INNER JOIN dwh.d_date dd ON dd.tk_date = t.expected_arrival_date_tk 
    WHERE departure_platform_name IS NOT NULL AND departure_platform_name<> '' AND dd.date>=NOW()::DATE-1
) t
GROUP BY station_tk, platform_name, ref_date_tk
ON CONFLICT (station_tk, platform_name, ref_date_tk) DO UPDATE
SET
    nb_arrivals           = EXCLUDED.nb_arrivals,
    nb_departures         = EXCLUDED.nb_departures,
    nb_arrivals_delayed   = EXCLUDED.nb_arrivals_delayed,
    nb_departures_delayed = EXCLUDED.nb_departures_delayed,
    avg_delay_minutes     = EXCLUDED.avg_delay_minutes,
    max_delay_minutes     = EXCLUDED.max_delay_minutes,
    total_delay_minutes    = EXCLUDED.total_delay_minutes,
    insert_date           = clock_timestamp();



INSERT INTO dwh.f_station_daily_metrics (
    station_tk,
    ref_date_tk,
    nb_arrivals,
    nb_departures,
    nb_departures_delayed,
    nb_arrivals_delayed,
    delay_rate,
    total_delay_minutes,
    avg_delay_minutes,
    max_delay_minutes,
    nb_platforms_used,
    most_used_platform
)
SELECT
    station_tk,
    ref_date_tk,
    SUM(nb_arrivals)               AS nb_arrivals,
    SUM(nb_departures)             AS nb_departures,
    SUM(nb_departures_delayed)     AS nb_departures_delayed,
    SUM(nb_arrivals_delayed)       AS nb_arrivals_delayed,
    100*(SUM(nb_departures_delayed) + SUM(nb_arrivals_delayed))::float
        / NULLIF(SUM(nb_arrivals) + SUM(nb_departures), 0) AS delay_rate,
    SUM(pu.total_delay_minutes)       AS total_delay_minutes,
    AVG(pu.avg_delay_minutes)         AS avg_delay_minutes,
    MAX(pu.max_delay_minutes)         AS max_delay_minutes,
    COUNT(*)                       AS nb_platforms_used,
    (
        SELECT platform_name
        FROM dwh.f_station_platform_usage pu2
        WHERE pu2.station_tk = pu.station_tk
          AND pu2.ref_date_tk = pu.ref_date_tk
        ORDER BY (pu2.nb_arrivals + pu2.nb_departures) DESC
        LIMIT 1
    ) AS most_used_platform
FROM dwh.f_station_platform_usage pu
INNER JOIN dwh.d_date dd ON dd.tk_date = pu.ref_date_tk 
LEFT JOIN LATERAL (
    SELECT
        GREATEST(pu.max_delay_minutes, 0) AS max_delay_minutes,
        GREATEST(pu.avg_delay_minutes, 0) AS avg_delay_minutes
) d ON TRUE
WHERE dd.date::DATE>=NOW()::DATE-1
GROUP BY station_tk, ref_date_tk
ON CONFLICT (station_tk, ref_date_tk) DO UPDATE
SET
    nb_arrivals           = EXCLUDED.nb_arrivals,
    nb_departures         = EXCLUDED.nb_departures,
    nb_departures_delayed = EXCLUDED.nb_departures_delayed,
    nb_arrivals_delayed   = EXCLUDED.nb_arrivals_delayed,
    delay_rate            = EXCLUDED.delay_rate,
    total_delay_minutes   = EXCLUDED.total_delay_minutes,
    avg_delay_minutes     = EXCLUDED.avg_delay_minutes,
    max_delay_minutes     = EXCLUDED.max_delay_minutes,
    nb_platforms_used     = EXCLUDED.nb_platforms_used,
    most_used_platform    = EXCLUDED.most_used_platform,
    insert_date           = clock_timestamp();