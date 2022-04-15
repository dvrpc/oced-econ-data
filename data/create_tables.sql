/* Use psql: psql -f /path/to/this/file -d name_of_database (and potentially user, port, localhost
depending on setup)
*/

BEGIN;


DO $$ BEGIN
    CREATE TYPE geographic_area AS ENUM (
        'United States',
        'DVRPC Region',
        'Philadelphia MSA', 
        'Trenton MSA'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
    raise notice 'type "geographic_area" already exists, skipping';
END $$;

DO $$ BEGIN
    CREATE TYPE industry_group AS ENUM (
        'Mining, logging, and construction',
        'Manufacturing',
        'Trade, transportation, and utilities',
        'Information',
        'Financial activities',
        'Professional and business services',
        'Education and health services',
        'Leisure and hospitality',
        'Other services',
        'Government',
        'Total'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
    raise notice 'type "industry_group" already exists, skipping';
END $$;

CREATE TABLE IF NOT EXISTS inflation_rate (
    period DATE NOT NULL,
    rate REAL NOT NULL,
    area geographic_area NOT NULL
);

CREATE TABLE IF NOT EXISTS unemployment_rate (
    period DATE NOT NULL,
    rate REAL NOT NULL,
    area geographic_area NOT NULL
);

CREATE TABLE IF NOT EXISTS housing (
    period DATE NOT NULL,
    units SMALLINT,
    rate REAL NOT NULL,
    area geographic_area NOT NULL
);

CREATE TABLE IF NOT EXISTS employment_by_industry (
    period DATE NOT NULL,
    number REAL NOT NULL,
    share REAL NOT NULL,
    industry industry_group NOT NULL,
    area geographic_area NOT NULL
);

COMMIT;
