INSERT INTO dwh.d_line (line_name)
SELECT DISTINCT t.published_line_name 
FROM ods.trips t 
WHERE t.published_line_name IS NOT NULL 
AND t.published_line_name <> 'N/A'
AND t.arrival_time::DATE > (NOW()::DATE-2)
ON CONFLICT DO NOTHING;

INSERT INTO dwh.d_station (
	code_uic, 
	station_name, 
	station_name_ref,
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
WHERE code_uic <> '87276063'
)
, stops_list AS (
	SELECT DISTINCT si.libelle, snc.ref_name, s.stop_name
	FROM ods.stops s
	LEFT JOIN station_info si ON si.RANK=1 AND
	LOWER(
	       REPLACE(
	         REPLACE(si.libelle, '-', ' '),
	         '''', ' '
	       )
	     )
	   = LOWER(
	       REPLACE(
	         REPLACE(s.stop_name, '-', ' '),
	         '''', ' '
	       )
	     )
	LEFT JOIN ods.station_name_corr snc ON snc.station_name = stop_name AND si.libelle IS NULL
)
SELECT DISTINCT ON (COALESCE(code_uic, 'N/A'), stop_name)
		code_uic, 
		sl.stop_name,
		si.libelle,
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
LEFT JOIN station_info si ON si.RANK=1 and
LOWER(
       REPLACE(
         REPLACE(si.libelle, '-', ' '),
         '''', ' '
       )
     )
   = LOWER(
       REPLACE(
         REPLACE(COALESCE(sl.libelle, sl.ref_name), '-', ' '),
         '''', ' '
       )
     )
ON CONFLICT (station_name, commune, departement) DO NOTHING;