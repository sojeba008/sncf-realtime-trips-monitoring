INSERT INTO ods.stops (
    trip_id,
    stop_name,
    aimed_arrival,
    expected_arrival,
    aimed_departure,
    expected_departure,
    is_starting_point,
    is_terminus,
    production_date,
    ref_date
)
SELECT
    trip_id,
    stop_name,
    NULLIF(aimed_arrival, '')::timestamp aimed_arrival,
    NULLIF(expected_arrival, '')::timestamp expected_arrival,
    NULLIF(aimed_departure, '')::timestamp aimed_departure,
    NULLIF(expected_departure,'')::timestamp expected_departure,
    is_starting_point,
    is_terminus,
    ds.production_date,
    ot.departure_time::DATE
FROM dsa.stops ds
INNER JOIN ods.trips ot USING(trip_id)
ON CONFLICT DO NOTHING;
