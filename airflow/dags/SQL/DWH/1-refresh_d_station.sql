-- INSERT INTO dwh.d_station (station_name)
-- SELECT DISTINCT stop_name
-- FROM ods.stops
-- ON CONFLICT (station_name) DO NOTHING;


INSERT INTO dwh.d_station (
	code_uic, 
	station_name, 
	fret, 
	voyageurs, 
	code_ligne, 
	rg_troncon, 
	pk, 
	commune, 
	departement, 
	idreseau, 
	idgaia, 
	x_l93, 
	y_l93, 
	x_wgs84, 
	y_wgs84, 
	geom, 
	geom_l93, 
	inserted_at
)
WITH station_info AS (
	SELECT 
		code_uic, 
		libelle, 
		fret,
		voyageurs, 
		code_ligne, 
		rg_troncon, 
		pk, 
		commune, 
		departemen, 
		idreseau, 
		idgaia, 
		x_l93, 
		y_l93, 
		x_wgs84, 
		y_wgs84, 
		geom, 
		geom_l93, 
		inserted_at,
		RANK() OVER (PARTITION BY code_uic ORDER BY code_ligne, pk)
FROM ods.stations s 
)
, stops_list AS (
SELECT DISTINCT stop_name
FROM ods.stops
)
SELECT DISTINCT
		code_uic, 
		sl.stop_name, 
		CASE 
			WHEN fret='O' THEN TRUE
			WHEN fret='N' THEN FALSE
			ELSE NULL
		END AS fret, 
		CASE
			WHEN voyageurs='O' THEN TRUE
			WHEN voyageurs='N' THEN FALSE
			ELSE NULL
		END AS voyageurs,
		code_ligne, 
		rg_troncon, 
		pk, 
		COALESCE(commune, 'N/A') AS commune, 
		COALESCE(departemen, 'N/A') AS departement, 
		idreseau, 
		idgaia, 
		x_l93, 
		y_l93, 
		x_wgs84, 
		y_wgs84, 
		geom, 
		geom_l93, 
		inserted_at
FROM stops_list sl
LEFT JOIN station_info si ON si.libelle = sl.stop_name AND RANK = 1
ON CONFLICT (station_name, commune, departement) DO NOTHING;