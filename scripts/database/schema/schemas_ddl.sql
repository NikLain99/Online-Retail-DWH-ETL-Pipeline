--remove existing schemas if they exist and create new ones
DROP SCHEMA IF EXISTS raw CASCADE;
CREATE SCHEMA raw;

DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;

DROP SCHEMA IF EXISTS business CASCADE;
CREATE SCHEMA business;

DROP SCHEMA IF EXISTS marts CASCADE;
CREATE SCHEMA marts;

DROP SCHEMA IF EXISTS audit CASCADE;
CREATE SCHEMA audit;