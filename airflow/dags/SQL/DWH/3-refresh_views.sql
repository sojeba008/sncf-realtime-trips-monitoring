CREATE OR REPLACE VIEW dwh.v_trains_actifs_kpi AS
SELECT  
    COUNT(DISTINCT trip_id) FILTER (WHERE ftr.status_journey = 'EN_COURS') AS nb_trains_actifs,
    COUNT(DISTINCT trip_id) FILTER (
		WHERE ftr.departure_time_journey <= NOW() - INTERVAL '1 hour'
		  AND ftr.arrival_time_journey >= NOW() - INTERVAL '1 hour'
    ) AS nb_trains_actifs_h_1
FROM dwh.f_trips_realtime ftr;


--Vue pour le KPI Nombre de train en retard en TR --
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_trains_en_retard_kpi AS
WITH current_day AS (
    SELECT 
        COUNT(DISTINCT trip_id) AS trains_en_retard
    FROM dwh.f_trips_realtime
    WHERE departure_time_journey::DATE=NOW()::DATE AND delay_arrival_minutes > 5
),
previous_day AS (
    SELECT 
        COUNT(DISTINCT trip_id) AS trains_en_retard
    FROM dwh.f_trips_realtime
    WHERE delay_arrival_minutes > 5
      AND departure_time_journey::DATE=(NOW() - INTERVAL '1 day')::DATE
)
SELECT
    c.trains_en_retard AS nb_trains_en_retard,
    ROUND(
        ((c.trains_en_retard - p.trains_en_retard)::numeric / NULLIF(p.trains_en_retard, 0)) * 100,
        1
    ) AS variation_vs_hier
FROM current_day c, previous_day p;
REFRESH MATERIALIZED VIEW dwh.v_trains_en_retard_kpi;


--Vue pour le KPI retard moyen journalié en TR --
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_retard_moyen_kpi AS
WITH current_day AS (
    SELECT
        AVG(delay_arrival_minutes) AS retard_moyen
    FROM dwh.f_trips_realtime
    WHERE delay_arrival_minutes IS NOT NULL
      AND delay_arrival_minutes >= 0
      AND departure_time_journey::DATE = NOW()::DATE
      AND is_terminus 
),
daily_avg AS (
    SELECT
        AVG(delay_arrival_minutes) AS retard_moyen_journalier
    FROM dwh.f_trips_realtime ftr 
    WHERE delay_arrival_minutes IS NOT NULL
      AND delay_arrival_minutes >= 0
      AND is_terminus 
)
SELECT
    c.retard_moyen,
    ROUND(
        ((c.retard_moyen - d.retard_moyen_journalier)::numeric / NULLIF(d.retard_moyen_journalier, 0)) * 100,
        1
    ) AS variation_vs_moy_journaliere
FROM current_day c, daily_avg d;
REFRESH MATERIALIZED VIEW dwh.v_retard_moyen_kpi;

--Vue pour le KPI Nombre de gares active en TR --
CREATE OR REPLACE VIEW dwh.v_gares_actives_kpi AS
WITH current_hour AS (
    SELECT 
        COUNT(DISTINCT stop_station_tk) AS gares_actives
    FROM dwh.f_trips_realtime
    WHERE status_stop = 'EN_COURS'
),
previous_hour AS (
    SELECT 
        COUNT(DISTINCT stop_station_tk) AS gares_actives
    FROM dwh.f_trips_realtime
    WHERE expected_arrival BETWEEN NOW() - INTERVAL '2 hour' AND NOW() - INTERVAL '1 hour'
    AND expected_departure > NOW() - INTERVAL '2 hour'
)
SELECT
    c.gares_actives,
    ROUND(
        ((c.gares_actives - p.gares_actives)::numeric / NULLIF(p.gares_actives, 0)) * 100,
        1
    ) AS variation_vs_heure_precedente
FROM current_hour c, previous_hour p;


-- Vue pour les taux de retard journalier
--Taux = (Nb trains avec retard > 5 min) / (Nb total de trains terminés ou en cours)--
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_taux_retard AS
SELECT
    dd.date AS jour,
    COUNT(DISTINCT f.trip_id) FILTER (WHERE f.delay_arrival_minutes > 5) AS nb_trains_en_retard,
    COUNT(DISTINCT f.trip_id) AS nb_trains_total,
    ROUND(
        (COUNT(DISTINCT f.trip_id) FILTER (WHERE f.delay_arrival_minutes > 5)::numeric
        / NULLIF(COUNT(DISTINCT f.trip_id), 0)) * 100, 2
    ) AS taux_retard
FROM dwh.f_trips f
JOIN dwh.d_date dd ON dd.tk_date = f.ref_date_tk
GROUP BY dd.date
ORDER BY dd.date DESC;
REFRESH MATERIALIZED VIEW dwh.v_taux_retard;

---Nombre de trains actifs par heure--
CREATE OR REPLACE VIEW dwh.v_trains_actifs_par_heure AS
WITH hours AS (
    SELECT generate_series(
        date_trunc('hour', now() - INTERVAL '23 hours'),
        date_trunc('hour', now()),
        INTERVAL '1 hour'
    ) AS hour_start
)
SELECT
    h.hour_start,
    (h.hour_start + INTERVAL '1 hour') AS hour_end,
    (
      SELECT COUNT(DISTINCT trip_id)
      FROM dwh.f_trips_realtime f
      WHERE
        f.expected_departure <= (h.hour_start + INTERVAL '1 hour')
        AND f.expected_arrival >= h.hour_start
    ) AS nb_trains_actifs, 
    (
      SELECT COUNT(DISTINCT trip_id)
      FROM dwh.f_trips_realtime f
      WHERE
        f.expected_departure <= (h.hour_start + INTERVAL '1 hour')
        AND f.expected_arrival >= h.hour_start AND f.delay_arrival_minutes > 0
    ) AS nb_trains_actifs_en_retard,
    (
      SELECT COUNT(DISTINCT trip_id)
      FROM dwh.f_trips_realtime f
      WHERE
        f.expected_departure <= (h.hour_start + INTERVAL '1 hour') - INTERVAL '24 hours'
        AND f.expected_arrival >= h.hour_start - INTERVAL '24 hours'
    ) AS nb_trains_actifs_j_1,
    (
      SELECT COUNT(DISTINCT trip_id)
      FROM dwh.f_trips_realtime f
      WHERE
        f.expected_departure <= (h.hour_start + INTERVAL '1 hour') - INTERVAL '24 hours'
        AND f.expected_arrival >= h.hour_start - INTERVAL '24 hours' AND f.delay_arrival_minutes > 0
    ) AS nb_trains_en_retard_j_1
FROM hours h
ORDER BY h.hour_start;



-- View gare avec + de retard---
CREATE OR REPLACE VIEW dwh.v_gares_plus_retard AS
SELECT
	ds.tk_station,
    ds.station_name AS gare,
    ROUND(SUM(f.delay_arrival_minutes)/COUNT(DISTINCT f.trip_id), 1) AS retard_moyen_min,
    COUNT(DISTINCT f.trip_id) AS nb_trains
FROM dwh.f_trips_realtime f
JOIN dwh.d_station ds ON ds.tk_station = f.stop_station_tk
WHERE f.delay_arrival_minutes IS NOT NULL 
AND f.delay_arrival_minutes>0
AND f.ref_date::DATE = NOW()::DATE
GROUP BY ds.station_name, ds.tk_station
HAVING COUNT(DISTINCT f.trip_id) >= 1
ORDER BY retard_moyen_min DESC
LIMIT 100;


--Nombre de minute de retard par jour /!\ Mesure effectuée au terminus et non à chaque arret--
-- /!\ Compte les retards qu'au terminus
-- CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_total_minutes_retard_jour AS
-- SELECT
--     dd.date AS jour,
--     SUM(f.delay_arrival_minutes) AS total_minutes_retard
-- FROM dwh.f_trips f
-- JOIN dwh.d_date dd ON dd.tk_date = f.ref_date_tk
-- WHERE f.delay_arrival_minutes > 0
-- AND f.is_terminus 
-- GROUP BY dd.date
-- ORDER BY dd.date DESC;
-- REFRESH MATERIALIZED VIEW dwh.v_total_minutes_retard_jour;


--Retard par type de trains-------------------------
CREATE OR REPLACE VIEW dwh.v_retard_par_type_train AS
WITH base AS (
	SELECT DISTINCT
		trip_id,
       -- /!\ J considère le retard au dernier arrêts
		FIRST_VALUE(f.tk_trip_rt) OVER (PARTITION BY trip_id ORDER BY f.expected_departure DESC) AS last_tk_trip_rt
	FROM dwh.f_trips_realtime f 
)
, f_trips_realtime_filterd AS (
	SELECT f.trip_id, f.departure_time_journey, f.delay_arrival_minutes, f.num_vehicule
	FROM dwh.f_trips_realtime f 
	INNER JOIN base b ON b.last_tk_trip_rt = f.tk_trip_rt 
	WHERE f.departure_time_journey::DATE = NOW()::DATE
)
SELECT
    CASE 
        WHEN dt.categorie_vehicule ILIKE 'RER%' THEN 'RER'
        WHEN dt.categorie_vehicule ILIKE 'TRAIN TER%' THEN 'TRAIN TER'
        WHEN dt.categorie_vehicule ILIKE 'TRAIN SPECIAL' THEN 'TRAIN SPECIAL'
        WHEN dt.categorie_vehicule ILIKE 'TRAIN' THEN 'TRAIN'
        ELSE COALESCE(replace(dt.categorie_vehicule, '_', ' '), 'Non identifié')
    END AS type_vehicule,
    ROUND(SUM(f.delay_arrival_minutes)/COUNT(DISTINCT f.trip_id)::numeric, 5) AS retard_moyen_min,
    COUNT(DISTINCT f.trip_id) AS nb_trains
FROM f_trips_realtime_filterd f
JOIN dwh.d_vehicule dt ON dt.num_vehicule = f.num_vehicule
WHERE f.delay_arrival_minutes IS NOT NULL 
GROUP BY 
    CASE 
        WHEN dt.categorie_vehicule ILIKE 'RER%' THEN 'RER'
        WHEN dt.categorie_vehicule ILIKE 'TRAIN TER%' THEN 'TRAIN TER'
        WHEN dt.categorie_vehicule ILIKE 'TRAIN SPECIAL' THEN 'TRAIN SPECIAL'
        WHEN dt.categorie_vehicule ILIKE 'TRAIN' THEN 'TRAIN'
        ELSE COALESCE(replace(dt.categorie_vehicule, '_', ' '), 'Non identifié')
    END
ORDER BY retard_moyen_min DESC;


--stats-retard-region par date d'arrivé et par région
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_stats_retard_region AS
WITH ordered_stops AS (
    SELECT
    	fr.departure_time_journey::DATE,
        fr.trip_id,
        fr.num_vehicule,
        fr.stop_station_tk,
        st.station_name,
        st.commune,
        rd.nom_region AS region_station,
        COALESCE(fr.expected_arrival::DATE, fr.expected_departure::DATE ) AS ref_date, 
        fr.delay_arrival_minutes,
        rank() OVER (PARTITION BY fr.trip_id ORDER BY fr.expected_arrival, fr.stop_station_tk) AS stop_order
    FROM dwh.f_trips_realtime fr
    JOIN dwh.d_station st ON fr.stop_station_tk = st.tk_station
    JOIN ods.ref_departements rd ON LOWER(st.departement) = LOWER(rd.nom_departement) 
    WHERE fr.delay_arrival_minutes IS NOT NULL
),
lagged AS (
    SELECT
        o.*,
        LAG(delay_arrival_minutes) OVER (PARTITION BY trip_id ORDER BY stop_order) AS prev_delay
    FROM ordered_stops o
),
delay_gen AS (
    SELECT
    	ref_date,
        trip_id,
        num_vehicule,
        region_station,
        (delay_arrival_minutes - COALESCE(prev_delay, 0)) AS delay_diff
    FROM lagged
)
SELECT
	ref_date as date,
    region_station,
    COUNT(DISTINCT trip_id) AS nb_trains_impactes,
    SUM(CASE WHEN delay_diff > 0 THEN 1 ELSE 0 END) AS nb_points_generation,
    ROUND(AVG(CASE WHEN delay_diff > 0 THEN delay_diff END), 2) AS avg_delay_generated,
    ROUND(SUM(CASE WHEN delay_diff > 0 THEN delay_diff ELSE 0 END), 2) AS total_delay_generated
FROM delay_gen
GROUP BY ref_date, region_station
ORDER BY ref_date, region_station, total_delay_generated DESC;

REFRESH MATERIALIZED VIEW dwh.v_stats_retard_region;




-- stats-rattrapage-regions par date et par région
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_stats_rattrapage_regions AS
WITH ordered_stops AS (
    SELECT
        fr.trip_id,
        fr.num_vehicule,
        fr.stop_station_tk,
        st.station_name,
        st.commune,
        rd.nom_region AS region_station,
        COALESCE(fr.expected_arrival::DATE, fr.expected_departure::DATE) AS ref_date,
        fr.delay_arrival_minutes,
        rank() OVER (PARTITION BY fr.trip_id ORDER BY fr.expected_arrival, fr.stop_station_tk) AS stop_order
    FROM dwh.f_trips_realtime fr
    JOIN dwh.d_station st ON fr.stop_station_tk = st.tk_station
    JOIN ods.ref_departements rd ON LOWER(st.departement) = LOWER(rd.nom_departement)
    WHERE fr.delay_arrival_minutes IS NOT NULL
),
lagged AS (
    SELECT
        o.*,
        LAG(delay_arrival_minutes) OVER (PARTITION BY trip_id ORDER BY stop_order) AS prev_delay
    FROM ordered_stops o
),
delay_eval AS (
    SELECT
    	ref_date,
        region_station,
        trip_id,
        num_vehicule,
        delay_arrival_minutes,
        prev_delay,
        CASE 
            WHEN prev_delay IS NOT NULL AND delay_arrival_minutes < prev_delay THEN 1
            ELSE 0
        END AS is_rattrapage,
        CASE 
            WHEN prev_delay IS NOT NULL AND delay_arrival_minutes > prev_delay THEN 1
            ELSE 0
        END AS is_generation
    FROM lagged
)
SELECT
	ref_date AS date,
    region_station,
    COUNT(DISTINCT trip_id) AS nb_trains,
    SUM(is_rattrapage) AS nb_rattrapages,
    SUM(is_generation) AS nb_generations,
    ROUND(100.0 * SUM(is_rattrapage) / NULLIF(SUM(is_rattrapage) + SUM(is_generation), 0), 2) AS taux_rattrapage_pct
FROM delay_eval
GROUP BY ref_date, region_station
ORDER BY ref_date, region_station, taux_rattrapage_pct DESC;

REFRESH MATERIALIZED VIEW dwh.v_stats_rattrapage_regions;



--stats-correlation heure / retard généré (derniers 7 jours)
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_stats_retard_horaire AS
WITH ordered_stops AS (
    SELECT
        fr.trip_id,
        fr.num_vehicule,
        fr.stop_station_tk,
        st.station_name,
        st.commune,
        rd.nom_region AS region_station,
        dt.hour_date AS espected_arrival_hour,
        fr.delay_arrival_minutes,
        rank() OVER (PARTITION BY fr.trip_id ORDER BY fr.expected_arrival_date_tk  , fr.expected_arrival_time_tk, fr.stop_station_tk ) AS stop_order
    FROM dwh.f_trips fr
    INNER JOIN dwh.d_date dd_dep ON dd_dep.tk_date = fr.expected_departure_date_tk 
    INNER JOIN dwh.d_time dt  ON dt.tk_time  = fr.expected_arrival_time_tk   
    JOIN dwh.d_station st ON fr.stop_station_tk = st.tk_station
    JOIN ods.ref_departements rd ON LOWER(st.departement) = LOWER(rd.nom_departement)
    WHERE fr.delay_arrival_minutes IS NOT NULL AND fr.expected_arrival_time_tk <> -1
    AND dd_dep.date >= NOW()::DATE - INTERVAL '7 days'
),
lagged AS (
    SELECT
        o.*,
        LAG(delay_arrival_minutes) OVER (PARTITION BY trip_id ORDER BY stop_order) AS prev_delay
    FROM ordered_stops o
),
delay_gen AS (
    SELECT
        trip_id,
        num_vehicule,
        region_station,
        EXTRACT(HOUR FROM espected_arrival_hour) AS heure_passage,
        (delay_arrival_minutes - COALESCE(prev_delay, 0)) AS delay_diff
    FROM lagged
)
SELECT
    heure_passage,
    ROUND(AVG(CASE WHEN delay_diff > 0 THEN delay_diff ELSE 0 END), 2) AS avg_delay_generated,
    ROUND(SUM(CASE WHEN delay_diff > 0 THEN delay_diff ELSE 0 END), 2) AS total_delay_generated,
    COUNT(DISTINCT trip_id) AS nb_trains
FROM delay_gen
GROUP BY heure_passage
ORDER BY heure_passage;
REFRESH MATERIALIZED VIEW dwh.v_stats_retard_horaire;




--stats-correlation heure / retard généré par region(derniers 7 jours)
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_stats_retard_horaire_region AS
WITH ordered_stops AS (
    SELECT
        fr.trip_id,
        fr.num_vehicule,
        fr.stop_station_tk,
        st.station_name,
        st.commune,
        rd.nom_region AS region_station,
        dt.hour_date AS espected_arrival_hour,
        fr.delay_arrival_minutes,
        rank() OVER (PARTITION BY fr.trip_id ORDER BY fr.expected_arrival_date_tk  , fr.expected_arrival_time_tk ) AS stop_order
    FROM dwh.f_trips fr
    INNER JOIN dwh.d_date dd_dep ON dd_dep.tk_date = fr.expected_departure_date_tk 
    INNER JOIN dwh.d_time dt  ON dt.tk_time  = fr.expected_arrival_time_tk   
    JOIN dwh.d_station st ON fr.stop_station_tk = st.tk_station
    JOIN ods.ref_departements rd ON LOWER(st.departement) = LOWER(rd.nom_departement)
    WHERE fr.delay_arrival_minutes IS NOT NULL AND fr.expected_arrival_time_tk <> -1
    AND dd_dep.date >= NOW()::DATE - INTERVAL '7 days'
),
lagged AS (
    SELECT
        o.*,
        LAG(delay_arrival_minutes) OVER (PARTITION BY trip_id ORDER BY stop_order) AS prev_delay
    FROM ordered_stops o
),
delay_gen AS (
    SELECT
        trip_id,
        num_vehicule,
        region_station,
        EXTRACT(HOUR FROM espected_arrival_hour) AS heure_passage,
        (delay_arrival_minutes - COALESCE(prev_delay, 0)) AS delay_diff
    FROM lagged
)
SELECT
    heure_passage,
    region_station,
    ROUND(AVG(CASE WHEN delay_diff > 0 THEN delay_diff ELSE 0 END), 2) AS avg_delay_generated,
    ROUND(SUM(CASE WHEN delay_diff > 0 THEN delay_diff ELSE 0 END), 2) AS total_delay_generated,
    COUNT(DISTINCT trip_id) AS nb_trains
FROM delay_gen
GROUP BY region_station, heure_passage
ORDER BY region_station, heure_passage;
REFRESH MATERIALIZED VIEW dwh.v_stats_retard_horaire_region;
