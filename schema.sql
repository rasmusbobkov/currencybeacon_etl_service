CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id BIGINT PRIMARY KEY,
    name VARCHAR,
    short_code VARCHAR UNIQUE,
    code VARCHAR,
    precision INT,
    subunit INT,
    symbol VARCHAR,
    symbol_first BOOLEAN,
    decimal_mark VARCHAR,
    thousands_separator VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id BIGINT PRIMARY KEY,
    date DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

CREATE TABLE IF NOT EXISTS fact_exchange_rate (
    fact_id BIGINT PRIMARY KEY,
    date_id BIGINT NOT NULL,
    currency_id BIGINT NOT NULL,
    rate DOUBLE,
    base_currency VARCHAR,
    timestamp BIGINT,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (currency_id) REFERENCES dim_currency(currency_id)
);
