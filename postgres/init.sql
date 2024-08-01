CREATE TABLE stock_trades (
	trade_id SERIAL PRIMARY KEY,
	symbol VARCHAR(20) NOT NULL,
	price DECIMAL(15, 4) NOT NULL,
	volume DECIMAL(15, 4) NOT NULL,
	time_stamp BIGINT NOT NULL,
	trade_price DECIMAL(15, 4),
	type VARCHAR(10) NOT NULL
);