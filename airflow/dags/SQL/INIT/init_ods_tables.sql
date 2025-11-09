CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.trips (
    trip_id TEXT NOT NULL,
    vehicule TEXT,
    origin_name TEXT,
    departure_time TIMESTAMP,
    dest_name TEXT,
    arrival_time TIMESTAMP,
    production_date TIMESTAMP,
    ref_date DATE NOT NULL,
    vehicule_category TEXT NULL,
	vehicule_mode TEXT NULL,
    CONSTRAINT trips_pkey PRIMARY KEY (trip_id, ref_date)
) PARTITION BY RANGE (ref_date);


CREATE TABLE IF NOT EXISTS ods.stops (
    id                 INT8 GENERATED ALWAYS AS IDENTITY,
    trip_id            text         NOT NULL,
    stop_name          text         NOT NULL,
    aimed_arrival      timestamp,
    expected_arrival   timestamp,
    aimed_departure    timestamp,
    expected_departure timestamp,
    is_starting_point  int,
    is_terminus        int,
    production_date    timestamp,
    departure_platform_name TEXT NULL,
	arrival_platform_name TEXT NULL,
    ref_date           date         NOT NULL,
    CONSTRAINT stops_pkey PRIMARY KEY (id, ref_date),
    CONSTRAINT stops_un UNIQUE (trip_id, stop_name, ref_date),
    CONSTRAINT stops_trip_id_fkey FOREIGN KEY (trip_id, ref_date) REFERENCES ods.trips (trip_id, ref_date)
) PARTITION BY RANGE (ref_date);

CREATE TABLE IF NOT EXISTS ods.stations (
    station_pk      INT8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code_uic        char(8)               NOT NULL,
    libelle         text                  NOT NULL,
    fret            char(1)               NOT NULL,      -- 'O' / 'N'
    voyageurs       char(1)               NOT NULL,      -- 'O' / 'N'
    code_ligne      char(6),
    rg_troncon      smallint,
    pk              text,
    commune         text,
    departemen      text,
    idreseau        integer,
    idgaia          uuid,
    x_l93           numeric(10,3),
    y_l93           numeric(10,3),
    x_wgs84         numeric(9,6),
    y_wgs84         numeric(9,6),
    geom            geography(Point,4326),
    geom_l93        geometry(Point,2154),
    inserted_at     timestamptz           DEFAULT now()
);

CREATE INDEX IF NOT EXISTS stations_geom_gix     ON ods.stations USING gist (geom);
CREATE INDEX IF NOT EXISTS stations_geom_l93_gix ON ods.stations USING gist (geom_l93);

CREATE TABLE IF NOT EXISTS ods.vehicules (
    tk_vehicule      INT8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   
    num_vehicule     TEXT,
    category_vehicule TEXT,
    vehicule_mode TEXT,
    CONSTRAINT vehicule_un UNIQUE (num_vehicule)
);
