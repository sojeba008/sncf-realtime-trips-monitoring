INSERT INTO ods.trips (
    trip_id,
    train,
    origin_name,
    departure_time,
    dest_name,
    arrival_time,
    production_date,
    ref_date
)
SELECT
    trip_id,
    train,
    origin_name,
    departure_time,
    dest_name,
    arrival_time,
    production_date,
    departure_time::DATE
FROM dsa.trips
ON CONFLICT DO NOTHING;

-- load of ods.trains
INSERT INTO ods.trains(num_train)
SELECT train DISTINCT FROM dsa.trips
ON CONFLICT DO NOTHING;