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
        'Mining, Logging, and Construction',
        'Manufacturing',
        'Trade, Transportation, and Utilities',
        'Information',
        'Financial Activities',
        'Professional and Business Services',
        'Education and Health Services',
        'Leisure and Hospitality',
        'Other Services',
        'Government',
        'Total Nonfarm'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
    raise notice 'type "industry_group" already exists, skipping';
END $$;

CREATE TABLE IF NOT EXISTS cpi (
    period DATE NOT NULL,
    area geographic_area NOT NULL,
    idx REAL NOT NULL,
    rate REAL,
    preliminary boolean NOT NULL,
    constraint cpi_unique unique(period, area)
);

CREATE TABLE IF NOT EXISTS unemployment_rate (
    period DATE NOT NULL,
    area geographic_area NOT NULL,
    rate REAL NOT NULL,
    preliminary boolean NOT NULL,
    constraint unemployment_unique unique(period, area)
);

CREATE TABLE IF NOT EXISTS housing (
    period DATE PRIMARY KEY,
    units SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS employment_by_industry (
    period DATE NOT NULL,
    area geographic_area NOT NULL,
    industry industry_group NOT NULL,
    number REAL NOT NULL,
    preliminary boolean NOT NULL,
    constraint industry_unique unique(period, industry, area)
);

COMMIT;
