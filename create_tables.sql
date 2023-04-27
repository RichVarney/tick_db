DROP
  TABLE IF EXISTS product;
DROP
  TABLE IF EXISTS price_data;
DROP
  TABLE IF EXISTS tick_data_match;
DROP
  TABLE IF EXISTS tick_data_open;
DROP
  TABLE IF EXISTS tick_data_change;
DROP
  TABLE IF EXISTS tick_data_done;
DROP
  TABLE IF EXISTS tick_data_received;
DROP
  TABLE IF EXISTS file_log;
CREATE TABLE "product" (
  "unique_id" SMALLSERIAL NOT NULL PRIMARY KEY,
  "symbol" varchar(4) NOT NULL,
  "currency" varchar(4) NOT NULL,
  "name" varchar(40) NOT NULL,
  "exchange" varchar(40) NOT NULL,
  UNIQUE ("symbol", "currency", "exchange")
);
CREATE TABLE "price_data" (
  "unique_id" SERIAL NOT NULL PRIMARY KEY,
  "product_id" int2 NOT NULL,
  "time" TIMESTAMP NOT NULL,
  "open" int4 NOT NULL,
  "high" int4 NOT NULL,
  "low" int4 NOT NULL,
  "close" int4 NOT NULL,
  "volume" float8 NOT NULL
);
CREATE TABLE "tick_data_match" (
  "unique_id" SERIAL NOT NULL PRIMARY KEY,
  "product_id" int8 NOT NULL,
  "side" varchar(1) NOT NULL,
  --"time_seconds" int8 NOT NULL,
  "time" TIMESTAMP,
  "price" int8 NOT NULL,
  "size" float8 NOT NULL,
  "maker_order_id" UUID NOT NULL,
  "taker_order_id" UUID NOT NULL
);
CREATE TABLE "tick_data_open" (
  "unique_id" SERIAL NOT NULL PRIMARY KEY,
  "product_id" int8 NOT NULL,
  "side" varchar(1) NOT NULL,
  "time" TIMESTAMP,
  "price" int8 NOT NULL,
  "remaining_size" float8 NOT NULL,
  "order_id" UUID NOT NULL
);
CREATE TABLE "tick_data_change" (
  "unique_id" SERIAL NOT NULL PRIMARY KEY,
  "product_id" int8 NOT NULL,
  "side" varchar(1) NOT NULL,
  "time" TIMESTAMP,
  "price" int8 NOT NULL,
  "old_size" float8 NOT NULL,
  "new_size" float8 NOT NULL,
  "order_id" UUID NOT NULL
);
CREATE TABLE "tick_data_done" (
  "unique_id" SERIAL NOT NULL PRIMARY KEY,
  "product_id" int8 NOT NULL,
  "side" varchar(1) NOT NULL,
  "time" TIMESTAMP,
  "price" int8 NOT NULL,
  "remaining_size" float8 NOT NULL,
  "reason" varchar(8) NOT NULL,
  "order_id" UUID NOT NULL
);
CREATE TABLE "tick_data_received" (
  "unique_id" SERIAL NOT NULL PRIMARY KEY,
  "order_id" UUID NOT NULL,
  "order_type" varchar(1) NOT NULL,
  "side" varchar(1) NOT NULL
);
CREATE TABLE "file_log" (
  "unique_id" SMALLSERIAL NOT NULL PRIMARY KEY,
  "filename" varchar NOT NULL,
  "time" TIMESTAMP NOT NULL
);
SELECT
  create_hypertable('tick_data_match', 'unique_id', chunk_time_interval => 86400);
SELECT
  create_hypertable('tick_data_open', 'unique_id', chunk_time_interval => 86400);
SELECT
  create_hypertable('tick_data_change', 'unique_id', chunk_time_interval => 86400);
SELECT
  create_hypertable('tick_data_done', 'unique_id', chunk_time_interval => 86400);
SELECT
  create_hypertable('tick_data_received', 'unique_id', chunk_time_interval => 86400);
