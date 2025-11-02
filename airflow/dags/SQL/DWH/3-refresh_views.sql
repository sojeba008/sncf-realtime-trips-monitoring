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
        f.expected_departure <= (h.hour_start + INTERVAL '1 hour') - INTERVAL '24 hours'
        AND f.expected_arrival >= h.hour_start - INTERVAL '24 hours'
    ) AS nb_trains_actifs_j_1,
    ROUND(
      (
        (
          ( (SELECT COUNT(DISTINCT trip_id)
              FROM dwh.f_trips_realtime f
              WHERE f.expected_departure <= (h.hour_start + INTERVAL '1 hour')
                AND f.expected_arrival >= h.hour_start)
          )
          -
          ( (SELECT COUNT(DISTINCT trip_id)
              FROM dwh.f_trips_realtime f
              WHERE f.expected_departure <= (h.hour_start + INTERVAL '1 hour') - INTERVAL '24 hours'
                AND f.expected_arrival >= h.hour_start - INTERVAL '24 hours')
          )
        )::numeric
        / NULLIF(
            (SELECT COUNT(DISTINCT trip_id)
             FROM dwh.f_trips_realtime f
             WHERE f.expected_departure <= (h.hour_start + INTERVAL '1 hour') - INTERVAL '24 hours'
               AND f.expected_arrival >= h.hour_start - INTERVAL '24 hours'
            ), 0
        )
      ) * 100
    , 1) AS variation_vs_hier_pct
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
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.v_total_minutes_retard_jour AS
SELECT
    dd.date AS jour,
    SUM(f.delay_arrival_minutes) AS total_minutes_retard
FROM dwh.f_trips f
JOIN dwh.d_date dd ON dd.tk_date = f.ref_date_tk
WHERE f.delay_arrival_minutes > 0
AND f.is_terminus 
GROUP BY dd.date
ORDER BY dd.date DESC;
REFRESH MATERIALIZED VIEW dwh.v_total_minutes_retard_jour;


CREATE OR REPLACE VIEW dwh.v_retard_par_type_train AS
SELECT
    COALESCE(dt.type_train, 'Non identifié') AS type_train,
    ROUND(SUM(f.delay_arrival_minutes)/COUNT(DISTINCT f.trip_id), 1) AS retard_moyen_min,
    COUNT(DISTINCT f.trip_id) AS nb_trains
FROM dwh.f_trips_realtime f
JOIN dwh.d_train dt ON dt.num_train = f.num_train
WHERE f.delay_arrival_minutes IS NOT NULL 
AND f.is_terminus
GROUP BY dt.type_train
ORDER BY retard_moyen_min DESC;