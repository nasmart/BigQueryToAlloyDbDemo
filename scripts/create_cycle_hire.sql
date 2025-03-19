CREATE TABLE cycle_hire (
    rental_id INTEGER,
    duration INTEGER,
    duration_ms BIGINT,
    bike_id INTEGER,
    bike_model TEXT,
    end_date TIMESTAMP,
    end_station_id INTEGER,
    end_station_name TEXT,
    start_date TIMESTAMP,
    start_station_id INTEGER,
    start_station_name TEXT,
    end_station_logical_terminal INTEGER,
    start_station_logical_terminal INTEGER,
    end_station_priority_id INTEGER
);
