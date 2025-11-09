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
    NULLIF(ds.aimed_arrival, '')::timestamp aimed_arrival,
    NULLIF(ds.expected_arrival, '')::timestamp expected_arrival,
    NULLIF(ds.aimed_departure, '')::timestamp aimed_departure,
    NULLIF(ds.expected_departure,'')::timestamp expected_departure,
    ds.is_starting_point,
    ds.is_terminus,
    ds.production_date,
    ot.departure_time::DATE
FROM dsa.stops ds
INNER JOIN ods.trips ot USING(trip_id)
LEFT JOIN ods.stops os USING(trip_id, stop_name)
WHERE os.id IS NULL AND 
ot.departure_time::DATE >= (NOW()::DATE- (INTERVAL '2 day'))
ON CONFLICT DO NOTHING;