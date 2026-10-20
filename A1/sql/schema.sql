-- COMP5339 Assignment 1, Member D: relational schema for NSW EV chargers.
-- Run this script against a new DuckDB database before loading data.
-- The spatial extension is needed for geometry construction and spatial queries.
INSTALL spatial;
LOAD spatial;

-- Import SA4_CODE26, SA4_NAME26 and the NSW polygons from the ABS shapefile.
-- The two non-spatial NSW categories have no polygon and are not imported.
-- Transform polygon geometry from EPSG:7844 to WGS84 longitude/latitude before
-- inserting it, so both geometry columns use the same coordinate system.
CREATE TABLE regions (
    sa4_code VARCHAR PRIMARY KEY,
    sa4_name VARCHAR NOT NULL UNIQUE,
    boundary_geom GEOMETRY NOT NULL
);

-- A station groups charger records. B's postcode and SA4 are consistent within
-- each station; names, addresses, operators and exact coordinates are not.
CREATE TABLE stations (
    station_id VARCHAR PRIMARY KEY,
    sa4_code VARCHAR,
    postcode VARCHAR,
    FOREIGN KEY (sa4_code) REFERENCES regions (sa4_code)
);

CREATE TABLE operators (
    operator_id INTEGER PRIMARY KEY,
    operator_name VARCHAR NOT NULL UNIQUE
);

-- Exact coordinates are stored once and may be shared by several charger rows.
-- The composite UNIQUE constraint makes (longitude, latitude) an alternate key.
CREATE TABLE charger_locations (
    location_id INTEGER PRIMARY KEY,
    latitude DOUBLE NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    location_geom GEOMETRY NOT NULL,
    UNIQUE (longitude, latitude)
);

-- One row per charger_id in chargers_clean.parquet. This is the central entity
-- table; location and technical characteristics are stored in related tables.
CREATE TABLE chargers (
    charger_id VARCHAR PRIMARY KEY,
    station_id VARCHAR NOT NULL,
    operator_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    source_objectid VARCHAR,
    station_name VARCHAR,
    address VARCHAR NOT NULL,
    lga_name VARCHAR,
    source_program VARCHAR,
    FOREIGN KEY (station_id) REFERENCES stations (station_id),
    FOREIGN KEY (operator_id) REFERENCES operators (operator_id),
    FOREIGN KEY (location_id) REFERENCES charger_locations (location_id)
);

-- Technical attributes have a mandatory one-to-one relationship with chargers.
-- rating_text preserves the readable source value; rating_kw supports analysis.
CREATE TABLE charger_characteristics (
    charger_id VARCHAR PRIMARY KEY,
    charger_type VARCHAR NOT NULL CHECK (charger_type IN ('AC', 'DC')),
    number_of_plugs INTEGER NOT NULL CHECK (number_of_plugs > 0),
    rating_text VARCHAR,
    rating_kw DOUBLE CHECK (rating_kw > 0),
    FOREIGN KEY (charger_id) REFERENCES chargers (charger_id)
);

-- C contains one row for every DC charger. charger_id is both the primary key
-- and a foreign key, so AC chargers have no enrichment row and a join cannot
-- multiply charger records. Externally unmatched DC chargers retain NULL values.
CREATE TABLE charger_enrichment (
    charger_id VARCHAR PRIMARY KEY,
    plug_types VARCHAR,
    charging_bays INTEGER CHECK (charging_bays > 0),
    pricing_information VARCHAR,
    FOREIGN KEY (charger_id) REFERENCES chargers (charger_id)
);
