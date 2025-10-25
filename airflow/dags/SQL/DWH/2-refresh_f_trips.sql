INSERT INTO dwh.f_trips (
      trip_id,
      num_train,
      ref_date_tk,
      stop_station_tk,
      origin_station_tk,
      destination_station_tk,

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
      is_starting_point,
      is_terminus
)
WITH src AS (
    SELECT
        s.trip_id,
        t.train AS num_train,
        s.stop_name,
        s.is_starting_point::boolean,
        s.is_terminus::boolean,
        s.ref_date,
        s.aimed_arrival,
        s.expected_arrival,
        s.aimed_departure,
        s.expected_departure,
        t.origin_name,
        t.dest_name
    FROM ods.stops  s
    JOIN ods.trips  t USING (trip_id, ref_date)
)
SELECT
    s.trip_id,
    s.num_train,
    d_ref.tk_date,
    COALESCE(st_stop.tk_station, -1),
    COALESCE(st_origin.tk_station, -1),
    COALESCE(st_dest.tk_station, -1),
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
    s.is_starting_point,
    s.is_terminus
FROM src s
-- === Main Dates ===
JOIN dwh.d_date d_ref       ON d_ref.date = s.ref_date
-- ==== Stations Dim ===
JOIN dwh.d_station st_stop   ON st_stop.station_name  = s.stop_name
LEFT JOIN dwh.d_station st_origin ON st_origin.station_name = s.origin_name
LEFT JOIN dwh.d_station st_dest   ON st_dest.station_name   = s.dest_name
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

TRUNCATE TABLE dwh.f_trips_realtime RESTART IDENTITY;
INSERT INTO dwh.f_trips_realtime (
    trip_id,
    num_train,
    ref_date,
    stop_station_tk,
    origin_station_tk,
    destination_station_tk,
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
        
    departure_time_trip,
    arrival_time_trip,
    
    is_starting_point,
    is_terminus
)
WITH src AS (
    SELECT
        s.trip_id,
        t.train AS num_train,
        s.stop_name,
        s.is_starting_point::boolean,
        s.is_terminus::boolean,
        s.ref_date,
        s.aimed_arrival,
        s.expected_arrival,
        s.aimed_departure,
        s.expected_departure,
        t.departure_time AS departure_time_trip,
        t.arrival_time AS arrival_time_trip,
        t.origin_name,
        t.dest_name
    FROM ods.stops s
    JOIN ods.trips t USING (trip_id, ref_date)
    WHERE s.ref_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE + INTERVAL '1 day')
      AND (
            s.expected_arrival > (NOW() - INTERVAL '3 hours')
            OR s.expected_departure > (NOW() - INTERVAL '1 hour')
          )
)
SELECT
    s.trip_id,
    s.num_train,
    NOW()::TIMESTAMP AS tk_date,
    COALESCE(st_stop.tk_station, -1),
    COALESCE(st_origin.tk_station, -1),
    COALESCE(st_dest.tk_station, -1),
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
    
    departure_time_trip,
    arrival_time_trip,
    
    s.is_starting_point,
    s.is_terminus
FROM src s
JOIN dwh.d_date d_ref ON d_ref.date = s.ref_date
LEFT JOIN dwh.d_station st_stop   ON st_stop.station_name  = s.stop_name
LEFT JOIN dwh.d_station st_origin ON st_origin.station_name = s.origin_name
LEFT JOIN dwh.d_station st_dest   ON st_dest.station_name   = s.dest_name
LEFT JOIN dwh.d_date d_aim_arr  ON d_aim_arr.date = s.aimed_arrival::date
LEFT JOIN dwh.d_date d_exp_arr  ON d_exp_arr.date = s.expected_arrival::date
LEFT JOIN dwh.d_date d_aim_dep  ON d_aim_dep.date = s.aimed_departure::date
LEFT JOIN dwh.d_date d_exp_dep  ON d_exp_dep.date = s.expected_departure::date
LEFT JOIN dwh.d_time tm_aim_arr ON tm_aim_arr.tk_time = to_char(s.aimed_arrival,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_exp_arr ON tm_exp_arr.tk_time = to_char(s.expected_arrival,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_aim_dep ON tm_aim_dep.tk_time = to_char(s.aimed_departure,'HH24MISS')::INT8
LEFT JOIN dwh.d_time tm_exp_dep ON tm_exp_dep.tk_time = to_char(s.expected_departure,'HH24MISS')::INT8
ON CONFLICT DO NOTHING;
