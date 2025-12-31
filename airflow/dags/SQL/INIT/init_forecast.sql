CREATE SCHEMA IF NOT EXISTS forecast;

CREATE TABLE IF NOT EXISTS forecast.fact_train_volume_forecast (
    id INT8 GENERATED ALWAYS AS IDENTITY,
    date TIMESTAMP NOT NULL,
    predicted_nb_train INT4 NOT NULL,
    model_name TEXT
);
