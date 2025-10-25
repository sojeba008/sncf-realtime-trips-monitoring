CREATE OR REPLACE VIEW dwh.v_trains_actifs AS
SELECT  
    COUNT(DISTINCT trip_id) FILTER (WHERE ftr.status_trip = 'EN_COURS') AS nb_trains_actifs,
    COUNT(DISTINCT trip_id) FILTER (
		WHERE ftr.departure_time_journey <= NOW() - INTERVAL '1 hour'
		  AND ftr.arrival_time_journey >= NOW() - INTERVAL '1 hour'
    ) AS nb_trains_actifs_h_1
FROM dwh.f_trips_realtime ftr;

-- SELECT ftr.trip_id, 
-- 		dso.station_name,
-- 		dsd.station_name,
-- 		string_agg(dss.station_name, ' -> '  ORDER BY ftr.aimed_departure),
-- 	    ftr.departure_time_journey,
--     	ftr.arrival_time_journey,
--     	ftr.status_trip
-- FROM dwh.f_trips_realtime ftr
-- INNER JOIN dwh.d_station dso ON dso.tk_station = ftr.origin_station_tk
-- INNER JOIN dwh.d_station dsd ON dsd.tk_station = ftr.destination_station_tk
-- INNER JOIN dwh.d_station dss ON dss.tk_station = ftr.stop_station_tk
-- GROUP BY trip_id, dso.station_name,dsd.station_name,ftr.departure_time_journey, ftr.arrival_time_journey, ftr.status_trip
-- ORDER BY ftr.departure_time_journey ASC

