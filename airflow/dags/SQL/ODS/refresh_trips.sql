INSERT INTO ods.trips (
    trip_id,
    vehicule,
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

-- load of ods.vehicules
INSERT INTO ods.vehicules(num_vehicule, category_vehicule, vehicule_mode)
SELECT train AS vehicule_numero, 
		split_part(vehicule_category, '::', 2) AS category_vehicule,
		vehicule_mode
FROM dsa.trips
ON CONFLICT DO NOTHING;
