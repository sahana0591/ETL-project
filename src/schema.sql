CREATE TABLE IF NOT EXISTS sales (
    order_id VARCHAR PRIMARY KEY,
    order_date TIMESTAMP,
    customer VARCHAR,
    product VARCHAR,
    quantity INTEGER,
    price NUMERIC,
    total NUMERIC
);
