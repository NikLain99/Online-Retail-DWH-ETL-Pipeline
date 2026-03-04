DROP TABLE IF EXISTS audit.raw_load_log;
CREATE TABLE audit.raw_load_log(
    batch_id uuid NOT NULL,
    file_name varchar(255),
    status varchar(20),
    row_count integer,
    started_at timestamp without time zone,
    finished_at timestamp without time zone,
    error_message text,
    PRIMARY KEY(batch_id)
);

DROP TABLE IF EXISTS audit.staging_load_log;
CREATE TABLE audit.staging_load_log(
    batch_id uuid NOT NULL,
    "table" varchar(50),
    status varchar(20),
    row_count integer,
    started_at timestamp without time zone,
    finished_at timestamp without time zone,
    error_message text,
    PRIMARY KEY(batch_id, "table")
);

DROP TABLE IF EXISTS audit.business_load_log;
CREATE TABLE audit.business_load_log(
    batch_id uuid NOT NULL,
    "table" varchar(50),
    status varchar(20),
    row_count integer,
    started_at timestamp without time zone,
    finished_at timestamp without time zone,
    error_message text,
    PRIMARY KEY(batch_id, "table")
);