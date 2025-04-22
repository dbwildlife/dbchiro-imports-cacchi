CREATE SCHEMA IF NOT EXISTS imports;

BEGIN
;

DROP TABLE IF EXISTS imports.source_data CASCADE
;
-- Import table
CREATE TABLE imports.source_data
(
    metaplace_id                 INT,
    metaplace_name               VARCHAR,
    place_id                     INT,
    place_uuid                   UUID,
    place_name                   VARCHAR,
    place_is_gite                BOOLEAN,
    place_alti                   INT,
    place_bdsource               VARCHAR,
    place_id_bdsource            VARCHAR,
    place_x                      FLOAT,
    place_y                      FLOAT,
    geom                         GEOMETRY(POINT, 4326),
    place_type_code              VARCHAR,
    session_id                   INT,
    session_uuid                 UUID,
    session_study_name           VARCHAR,
    session_name                 VARCHAR,
    session_contact_code         VARCHAR,
    session_date_start           DATE,
    session_time_start           TIME,
    session_date_end             DATE,
    session_time_end             TIME,
    session_comment              TEXT,
    sighting_main_observer       VARCHAR,
    sighting_id                  INT,
    sighting_uuid                UUID,
    sighting_codesp              VARCHAR,
    sighting_total_count         INT,
    sighting_bdsource            VARCHAR,
    sighting_id_bdsource         VARCHAR,
    sighting_comment             TEXT,
    sighting_period              VARCHAR,
    sighting_colo_repro          BOOLEAN,
    sighting_doubtful            BOOLEAN,
    countdetail_method           VARCHAR,
    countdetail_precision_code   VARCHAR,
    countdetail_transmetter_name VARCHAR,
    countdetail_unit_code        VARCHAR,
    countdetail_count            INT,
    countdetail_time             TIME,
    countdetail_ab               FLOAT,
    countdetail_d5               FLOAT,
    countdetail_d3               FLOAT,
    countdetail_pouce            FLOAT,
    countdetail_queue            FLOAT,
    countdetail_tibia            FLOAT,
    countdetail_pied             FLOAT,
    countdetail_cm3              FLOAT,
    countdetail_tragus           FLOAT,
    countdetail_poids            FLOAT,
    countdetail_comment          TEXT,
    countdetail_age_code         VARCHAR,
    countdetail_sex_code         VARCHAR,
    countdetail_gestation_code   VARCHAR,
    countdetail_chinspot_code    VARCHAR,
    countdetail_epid_code        VARCHAR,
    countdetail_epiph_code       VARCHAR,
    countdetail_glandcoul_code   VARCHAR,
    countdetail_glandtaille_code VARCHAR,
    countdetail_mamelle_code     VARCHAR,
    countdetail_testi_code       VARCHAR,
    countdetail_tunvag_code      VARCHAR,
    countdetail_usuredent_code   VARCHAR
)
;

/*
INSERT INTO imports.source_data
select *
FROM imports.jdd_gclr;
*/
CREATE INDEX ON imports.source_data USING gist (geom)
;


-- Place view

DROP VIEW IF EXISTS imports.v_import_place CASCADE
;

CREATE OR REPLACE VIEW imports.v_import_place AS
WITH places AS
    (SELECT DISTINCT i.place_name
                   , i.place_uuid
                   , i.geom
     FROM imports.source_data i)
   , uuids AS (SELECT i.*
                    , coalesce(i.place_uuid, coalesce(pl.uuid, uuid_generate_v4())) AS uuid
               FROM places i
                        LEFT JOIN dbchiro.sights_place pl
                                  ON (i.place_name = pl.name AND st_dwithin(i.geom, pl.geom, 0.05)))
   , datas AS (SELECT DISTINCT uuids.uuid
                             , source_data.place_name                                         AS name
                             , FALSE                                                             is_hidden
                             , coalesce(source_data.place_is_gite, FALSE)                     AS is_gite
                             , FALSE                                                          AS is_managed
                             , NULL                                                           AS proprietary
                             , FALSE                                                          AS convention
                             , NULL                                                           AS convention_file
                             , NULL                                                           AS map_file
                             , NULL                                                           AS photo_file
                             , NULL                                                           AS habitat
                             , source_data.place_alti                                         AS altitude
                             , NULL                                                           AS id_bdcavite
                             , NULL                                                           AS plan_localite
                             , NULL                                                           AS comment
                             , NULL                                                           AS other_imported_data
                             , FALSE                                                          AS telemetric_crossaz
                             , NULL                                                           AS bdsource
                             , NULL                                                           AS id_bdsource
                             , st_x(source_data.geom)                                         AS x
                             , st_y(source_data.geom)                                         AS y
                             , source_data.geom
                             , now()                                                          AS timestamp_create
                             , now()                                                          AS timestamp_update
                             , coalesce(accounts_profile.id, (SELECT id
                                                              FROM dbchiro.accounts_profile
                                                              WHERE username LIKE 'dbadmin')) AS created_by_id
                             , NULL::INT                                                      AS domain_id
                             , NULL::INT                                                      AS landcover_id
                             , metaplace_id
                             --, geodata_municipality.id                                           municipality_id
                             , dicts_placeprecision.id                                           precision_id
                             --, geodata_territory.id                                           AS territory_id
                             , NULL::INT                                                      AS type_id
                             , NULL::INT                                                      AS updated_by_id
               --   , json_build_object('info',
--                       'Donnée manuellement importée par fcloitre le ' || now())                                    AS extra_data
               FROM imports.source_data
                        JOIN
               uuids ON
                   uuids.place_name = source_data.place_name
                        LEFT JOIN dbchiro.accounts_profile ON sighting_main_observer = accounts_profile.username
                        LEFT JOIN dbchiro.geodata_landcover
                                  ON st_intersects(geodata_landcover.geom, source_data.geom)
                   /*LEFT JOIN dbchiro.geodata_areas
                             ON st_intersects(geodata_areas.geom, source_data.geom)
                   LEFT JOIN dbchiro.geodata_municipality
                             ON st_intersects(geodata_municipality.geom, source_data.geom)*/
                  , dbchiro.dicts_placeprecision
               WHERE dicts_placeprecision.code LIKE 'precis')
SELECT sights_place.id_place                                                AS id_place
     , datas.*
     , json_build_object('import_info',
                         json_build_object('comment',
                                           'Donnée importée directement en bdd par ' || current_user || ' le ' || now(),
                                           'type',
                                           'manual_import', 'date', now())) AS extra_data
FROM datas
         LEFT JOIN dbchiro.sights_place ON datas.uuid = sights_place.uuid
;

-- Study import view

DROP VIEW IF EXISTS imports.v_import_studies
;

CREATE OR REPLACE VIEW imports.v_import_studies AS
(
WITH studies AS (SELECT DISTINCT session_study_name                             AS name
                     /* Look for year in project name */
                               , (regexp_match(session_study_name, '\d{4}'))[1] AS year
                 FROM imports.source_data)
SELECT coalesce(ms.uuid, uuid_generate_v4())                                   AS uuid
     , ms.id_study
     , studies.name
     , studies.year::INT                                                       AS year
     , FALSE                                                                   AS public_funding
     , FALSE                                                                   AS public_report
     , FALSE                                                                   AS public_raw_data
     , FALSE                                                                   AS confidential
     , NULL::DATE                                                              AS confidential_end_date
     , 'A définir'                                                             AS type_etude
     , NULL                                                                    AS type_espace
     , 'A compléter'                                                           AS comment
     , now()                                                                   AS timestamp_create
     , now()                                                                   AS timestamp_update
     , (SELECT id FROM dbchiro.accounts_profile WHERE username LIKE 'dbadmin') AS created_by_id
     , accounts_profile.id                                                     AS project_manager_id
     , NULL::INT                                                               AS updated_by_id
FROM studies
         LEFT JOIN dbchiro.management_study ms ON studies.name = ms.name
   , dbchiro.accounts_profile
WHERE
    /* TODO: fix that hard coded condition */
    accounts_profile.username LIKE 'svincent')
;

-- Session import view


DROP VIEW IF EXISTS imports.v_import_session
;


-- BUG : ERROR: syntax error at end of input OK Correction faite (quelques espaces )
CREATE OR REPLACE VIEW imports.v_import_session AS
WITH sessions AS (SELECT DISTINCT sights_place.uuid AS place_uuid
                                , place_name
                                , source_data.geom
                                , session_uuid
                                , session_date_start
                                , session_time_start
                                , session_date_end
                                , session_time_end
                                , session_contact_code
                                , sighting_main_observer
                                , session_comment
                  FROM imports.source_data
                           JOIN dbchiro.sights_place
                                ON (source_data.place_name, source_data.geom) = (sights_place.name, sights_place.geom))
   , uuids AS (SELECT s.*
                    , spl.id_contact
                    , coalesce(spl.uuid, coalesce(s.session_uuid, uuid_generate_v4())) AS uuid
               FROM sessions s
                        LEFT JOIN (dbchiro.sights_session ss
                   JOIN (SELECT id_place, uuid AS place_uuid FROM dbchiro.sights_place) spl ON place_id = id_place
                   JOIN (SELECT id   AS id_contact
                              , code AS contact_code
                         FROM dbchiro.dicts_contact) contact ON ss.contact_id = contact.id_contact
                   ) AS spl
                                  ON (spl.place_uuid, spl.date_start, spl.contact_code) =
                                     (s.place_uuid, s.session_date_start, s.session_contact_code))
   , datas AS (SELECT DISTINCT sights_place.id_place
                             , sights_session.id_session
                             , uuids.place_uuid                                               AS place_uuid
                             , uuids.uuid                                                     AS uuid
                             , coalesce(source_data.session_name,
                                        'loc' || id_place || ' ' || source_data.session_date_start || ' ' ||
                                        accounts_profile.username || ' ' ||
                                        uuids.session_contact_code)                           AS name
                             , source_data.session_date_start                                 AS date_start
                             , source_data.session_time_start                                 AS time_start
                             , source_data.session_date_end                                   AS date_end
                             , source_data.session_time_end                                   AS time_end
                             , NULL                                                           AS data_file
                             , FALSE                                                          AS is_confidential
                             , string_agg(DISTINCT source_data.session_comment, ', ')         AS comment
                             , NULL                                                           AS bdsource
                             , NULL                                                           AS id_bdsource
                             , now()                                                          AS timestamp_create
                             , now()                                                          AS timestamp_update
                             , dicts_contact.id                                               AS contact_id
                             , coalesce(accounts_profile.id, (SELECT id
                                                              FROM dbchiro.accounts_profile
                                                              WHERE username LIKE 'dbadmin')) AS created_by_id
                             , coalesce(accounts_profile.id, (SELECT id
                                                              FROM dbchiro.accounts_profile
                                                              WHERE username LIKE 'dbadmin')) AS main_observer_id
                             , id_place                                                       AS place_id
                             , v_import_studies.id_study                                      AS study_id
                             , NULL::INT                                                      AS updated_by_id
               FROM imports.source_data
                        JOIN dbchiro.sights_place ON (source_data.place_name = sights_place.name AND
                                                      st_intersects(st_buffer(source_data.geom, 0.01),
                                                                    sights_place.geom))
                        JOIN uuids
                             ON (uuids.place_uuid, uuids.session_date_start,
                                 uuids.session_contact_code) =
                                (sights_place.uuid, source_data.session_date_start,
                                 source_data.session_contact_code)
--                 JOIN imports.v_import_place ON v_import_place.uuid = uuids.place_uuid
                        LEFT JOIN imports.v_import_studies ON session_study_name = v_import_studies.name
                        LEFT JOIN dbchiro.sights_session ON sights_session.uuid = uuids.uuid
                        LEFT JOIN dbchiro.dicts_contact ON source_data.session_contact_code = dicts_contact.code
                        LEFT JOIN dbchiro.accounts_profile
                                  ON source_data.sighting_main_observer = accounts_profile.username
               GROUP BY id_session
                      , id_place
                      , uuids.place_uuid
                      , uuids.session_contact_code
                      , v_import_studies.id_study
                      , uuids.uuid
                      , source_data.session_name
                      , source_data.session_date_start
                      , source_data.session_time_start
                      , source_data.session_date_end
                      , source_data.session_time_end
                      , accounts_profile.username
                      , dicts_contact.id
                      , coalesce(
                       accounts_profile.id, (SELECT id
                                             FROM dbchiro.accounts_profile
                                             WHERE username LIKE 'dbadmin'))
                      , coalesce(
                       accounts_profile.id, (SELECT id
                                             FROM dbchiro.accounts_profile
                                             WHERE username LIKE 'dbadmin'))
                      , sights_place.id_place)
SELECT
--     sights_session.id_session                                            AS id_session
    datas.*
     , json_build_object(
        'import_info',
        json_build_object(
                'comment',
                'Donnée importée directement en bdd par ' || current_user || ' le ' || now(),
                'type',
                'manual_import', 'date', now())) AS extra_data
FROM datas
         LEFT JOIN dbchiro.sights_session ON datas.uuid = sights_session.uuid
;


-- Observations import view


DROP VIEW IF EXISTS imports.v_import_sightings
;


CREATE OR REPLACE VIEW imports.v_import_sightings AS
WITH sightings AS (SELECT DISTINCT sepl.id_session
                                 , sepl.main_observer_id
                                 , sepl.uuid                                       AS session_uuid
                                 , session_date_start
                                 , session_contact_code
                                 , sighting_uuid
                                 , sighting_colo_repro
                                 , sighting_doubtful
                                 , string_agg(DISTINCT sighting_comment, ' ; ')    AS sighting_comment
                                 , string_agg(DISTINCT sighting_id_bdsource, ', ') AS sighting_id_bdsource
                                 , string_agg(DISTINCT sighting_bdsource, ', ')    AS sighting_bdsource
                                 , CASE
                                       WHEN session_contact_code ILIKE 'du' THEN 1
                                       ELSE sighting_total_count END               AS sighting_total_count
                                 , lower(sighting_codesp)                          AS sighting_codesp
                                 , dicts_specie.id                                 AS codesp_id
                   FROM imports.source_data
                            JOIN (
                       imports.v_import_session se
                           JOIN (SELECT id   AS id_contact
                                      , code AS code_contact
                                 FROM dbchiro.dicts_contact) con ON con.id_contact = se.contact_id
                           JOIN (SELECT name AS name_place
                                      , id_place
                                      , geom
                                      , type_id
                                 FROM imports.v_import_place) pl ON se.place_id = pl.id_place
                           LEFT JOIN (SELECT id   AS id_type
                                           , code AS code_type
                                      FROM dbchiro.dicts_typeplace) typ ON pl.type_id = typ.id_type
                       ) AS sepl
                                 ON (sepl.name_place, sepl.geom, sepl.date_start, sepl.code_contact) =
                                    (source_data.place_name, source_data.geom,
                                     source_data.session_date_start, source_data.session_contact_code)
                            JOIN dbchiro.dicts_specie ON lower(sighting_codesp) = dicts_specie.codesp
                   GROUP BY sepl.id_session
                          , sepl.uuid
                          , session_date_start
                          , session_contact_code
                          , sighting_uuid
                          , sighting_colo_repro
                          , sighting_doubtful
                          , sepl.main_observer_id
                          , lower(sighting_codesp)
                          , CASE WHEN session_contact_code ILIKE 'du' THEN 1 ELSE sighting_total_count END
                          , dicts_specie.id)
   , uuids AS (SELECT s.*
                    , sseswsp.id_sighting
                    , coalesce(sseswsp.uuid, coalesce(s.sighting_uuid, uuid_generate_v4())) AS new_uuid
               FROM sightings s
                        LEFT JOIN (
                   dbchiro.sights_sighting ss
                       JOIN (SELECT id_session
                                  , sights_session.uuid AS session_uuid
                             FROM dbchiro.sights_session) sses ON ss.session_id = sses.id_session
                   ) AS sseswsp
                                  ON (sseswsp.session_uuid, sseswsp.codesp_id) =
                                     (s.session_uuid, s.codesp_id))
SELECT uuids.id_sighting
     , uuids.new_uuid
     , CASE
           WHEN extract(DOY FROM session_date_start) >= 335 AND extract(DOY FROM session_date_start) < 60
               THEN 'Hivernant'
           WHEN extract(DOY FROM session_date_start) BETWEEN 60 AND 135 THEN 'Transit printanier'
           WHEN extract(DOY FROM session_date_start) BETWEEN 136 AND 227 THEN 'Estivage'
           ELSE 'Transit automnal'
    END                                                                        AS period
     , uuids.sighting_total_count                                              AS total_count
     , coalesce(uuids.sighting_colo_repro, FALSE)                              AS breed_colo
     , coalesce(uuids.sighting_doubtful, FALSE)                                AS is_doubtful
     , uuids.sighting_id_bdsource                                              AS id_bdsource
     , uuids.sighting_bdsource                                                 AS bdsource
     , uuids.sighting_comment                                                  AS comment
     , now()                                                                   AS timestamp_create
     , now()                                                                   AS timestamp_update
     , uuids.codesp_id                                                         AS codesp_id
     , (SELECT id FROM dbchiro.accounts_profile WHERE username LIKE 'dbadmin') AS created_by_id
     , uuids.main_observer_id                                                  AS observer_id
     , uuids.id_session                                                        AS session_id
     , NULL::INT                                                               AS updated_by_id
     , json_build_object('import_info',
                         json_build_object('comment', 'Donnée importée directement en bdd par fcloitre le ' || now(),
                                           'type',
                                           'manual_import', 'date', now()))    AS extra_data
FROM uuids
;


-- Countdetail import view


DROP VIEW IF EXISTS imports.v_import_sightings_with_single_countdetail
;

CREATE OR REPLACE VIEW imports.v_import_sightings_with_single_countdetail AS
    -- WITH
--     countdetails AS (
SELECT sssepl.id_sighting
     , sssepl.main_observer_id
     , sssepl.uuid                                                             AS sighting_uuid
     , (SELECT id
        FROM dbchiro.dicts_method
        WHERE code = lower(countdetail_method))                                AS method_id
     , (SELECT id
        FROM dbchiro.dicts_countprecision
        WHERE code = lower(countdetail_precision_code))                        AS precision_id
     , countdetail_transmetter_name
     , (SELECT id
        FROM dbchiro.dicts_countunit
        WHERE code = lower(countdetail_unit_code))                             AS unit_id
     , countdetail_count                                                       AS count
     , countdetail_time                                                        AS time
--           , countdetail_ab as ab
--           , countdetail_d5 as d5
--           , countdetail_d3 as d3
--           , countdetail_pouce as pouce
--           , countdetail_queue as queue
--           , countdetail_tibia as tibia
--           , countdetail_pied as pied
--           , countdetail_cm3 as cm3
--           , countdetail_tragus as tragus
--           , countdetail_poids as poids
     , countdetail_comment                                                     AS comment
--           , (select id from dbchiro.dicts_age where code = lower(countdetail_age_code))  as age_id
--           , (select id from dbchiro.dicts_sex where code = lower(countdetail_sex_code))  as sex_id
--           , (select id from dbchiro.dicts_biomgestation where code = lower(countdetail_gestation_code))  as gestation_id
--           , (select id from dbchiro.dicts_biomchinspot where code = lower(countdetail_chinspot_code))  as chinspot_id
--           , (select id from dbchiro.dicts_biomepipidyme where code = lower(countdetail_epid_code))  as epid_id
--           , (select id from dbchiro.dicts_biomepiphyse where code = lower(countdetail_epiph_code))  as epiph_id
--           , (select id from dbchiro.dicts_biomepiphyse where code = lower(countdetail_epiph_code))  as epiph_id
--           , null as countdetail_glandcoul_code
--           , null as countdetail_glandtaille_code
--           , null as countdetail_mamelle_code
--           , null as countdetail_testi_code
--           , null as countdetail_tunvag_code
--           , null as countdetail_usuredent_code
     , now()                                                                   AS timestamp_create
     , now()                                                                   AS timestamp_update
     , (SELECT id FROM dbchiro.accounts_profile WHERE username LIKE 'dbadmin') AS created_by_id
     , NULL::INT                                                               AS updated_by_id
     , json_build_object('import_info',
                         json_build_object('comment', 'Donnée importée directement en bdd par fcloitre le ' || now(),
                                           'type',
                                           'manual_import', 'date', now()))    AS extra_data
FROM imports.source_data
         JOIN (
    imports.v_import_sightings
        JOIN imports.v_import_session se ON v_import_sightings.session_id = se.id_session
        JOIN (SELECT id   AS id_contact
                   , code AS code_contact
              FROM dbchiro.dicts_contact) con
        ON con.id_contact = se.contact_id
        JOIN dbchiro.dicts_specie ON v_import_sightings.codesp_id = dicts_specie.id
        JOIN (SELECT name AS name_place
                   , id_place
                   , geom
                   , type_id
              FROM imports.v_import_place) pl ON se.place_id = pl.id_place
        LEFT JOIN (SELECT id   AS id_type
                        , code AS code_type
                   FROM dbchiro.dicts_typeplace) typ ON pl.type_id = typ.id_type
    ) AS sssepl
              ON (sssepl.name_place, sssepl.geom, sssepl.date_start, sssepl.code_contact,
                  sssepl.codesp) =
                 (source_data.place_name, source_data.geom, source_data.session_date_start,
                  source_data.session_contact_code, lower(source_data.sighting_codesp))
;

COMMIT
;


/***************************			3. intégration des données via le script de Fred Deuxième partie				**************************************/

/*
DDL script for creating table and views
---------------------------------------
Import data table used to import datas into dbchiroweb table
*/

