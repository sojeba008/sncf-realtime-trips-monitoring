CREATE SCHEMA IF NOT EXISTS dsa;

CREATE TABLE IF NOT EXISTS dsa.trips (
	trip_id TEXT NOT NULL,
	train TEXT NULL,
	origin_name TEXT NULL,
	departure_time TIMESTAMP NULL,
	dest_name TEXT NULL,
	arrival_time TIMESTAMP NULL,
	production_date TIMESTAMP NULL,
	CONSTRAINT trips_pkey PRIMARY KEY (trip_id)
);

CREATE TABLE IF NOT EXISTS dsa.stops (
	id INT8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	trip_id TEXT NULL,
	stop_name TEXT NULL,
	aimed_arrival TEXT NULL,
	expected_arrival TEXT NULL,
	aimed_departure TEXT NULL,
	expected_departure TEXT NULL,
	is_starting_point INT8 NULL,
	is_terminus INT8 NULL,
	production_date TIMESTAMP NULL,
	CONSTRAINT stops_trip_id_fkey FOREIGN KEY (trip_id) REFERENCES dsa.trips(trip_id)
);
