INSERT INTO dbchiro4testimport.imports.source_data ( metaplace_id, metaplace_name, place_id, place_uuid, place_name
                                                   , place_is_gite, place_alti, place_bdsource, place_id_bdsource
                                                   , place_x, place_y, geom, place_type_code, session_id, session_uuid
                                                   , session_study_name, session_name, session_contact_code
                                                   , session_date_start, session_time_start, session_date_end
                                                   , session_time_end, session_comment, sighting_main_observer
                                                   , sighting_id, sighting_uuid, sighting_codesp, sighting_total_count
                                                   , sighting_bdsource, sighting_id_bdsource, sighting_comment
                                                   , sighting_period, sighting_colo_repro, sighting_doubtful
                                                   , countdetail_method, countdetail_precision_code
                                                   , countdetail_transmetter_name, countdetail_unit_code
                                                   , countdetail_count, countdetail_time, countdetail_ab, countdetail_d5
                                                   , countdetail_d3, countdetail_pouce, countdetail_queue
                                                   , countdetail_tibia, countdetail_pied, countdetail_cm3
                                                   , countdetail_tragus, countdetail_poids, countdetail_comment
                                                   , countdetail_age_code, countdetail_sex_code
                                                   , countdetail_gestation_code, countdetail_chinspot_code
                                                   , countdetail_epid_code, countdetail_epiph_code
                                                   , countdetail_glandcoul_code, countdetail_glandtaille_code
                                                   , countdetail_mamelle_code, countdetail_testi_code
                                                   , countdetail_tunvag_code, countdetail_usuredent_code)
select metaplace_id::int, metaplace_name, place_id::int, place_uuid::uuid, place_name
                   , place_is_gite, place_alti::int, place_bdsource, place_id_bdsource
                   , place_x, place_y, geom, place_type_code, session_id::int, session_uuid::uuid
                   , session_study_name, session_name, session_contact_code
                   , session_date_start, session_time_start, session_date_end::date
                   , session_time_end, session_comment, sighting_main_observer
                   , sighting_id::int, sighting_uuid::uuid, sighting_codesp, sighting_total_count::int
                   , sighting_bdsource, sighting_id_bdsource, sighting_comment
                   , sighting_period, sighting_colo_repro::bool, sighting_doubtful::bool
                   , countdetail_method, countdetail_precision_code
                   , countdetail_transmetter_name, countdetail_unit_code
                   , countdetail_count, countdetail_time::time, countdetail_ab, countdetail_d5
                   , countdetail_d3, countdetail_pouce, countdetail_queue
                   , countdetail_tibia, countdetail_pied, countdetail_cm3
                   , countdetail_tragus::float, countdetail_poids::float, countdetail_comment
                   , countdetail_age_code, countdetail_sex_code
                   , countdetail_gestation_code, countdetail_chinspot_code
                   , countdetail_epid_code, countdetail_epiph_code
                   , countdetail_glandcoul_code, countdetail_glandtaille_code
                   , countdetail_mamelle_code, countdetail_testi_code
                   , countdetail_tunvag_code, countdetail_usuredent_code from imports.jdd_gclr;

BEGIN
;

-- generate and fix potentiel errors on geom (due to x/y inversion)

UPDATE imports.source_data
SET place_name=trim(place_name)
  , metaplace_name=trim(metaplace_name)
  , geom = st_setsrid(st_makepoint((CASE WHEN place_x > place_y THEN place_y ELSE place_x END),
                                   (CASE WHEN place_x > place_y THEN place_x ELSE place_y END)), 4326)
;

UPDATE imports.source_data
SET place_name=trim(place_name)
  , metaplace_name=trim(metaplace_name)
  , geom = st_setsrid(st_makepoint(place_x, place_y), 4326)
;

-- Import studies
-- bug sur des valeurs null de years
INSERT INTO dbchiro.management_study( name
                                    , year
                                    , public_funding
                                    , public_report
                                    , public_raw_data
                                    , confidential
                                    , confidential_end_date
                                    , type_etude
                                    , type_espace
                                    , comment
                                    , timestamp_create
                                    , timestamp_update
                                    , created_by_id
                                    , project_manager_id
                                    , updated_by_id
                                    , uuid)
SELECT name
     , year
     , public_funding
     , public_report
     , public_raw_data
     , confidential
     , confidential_end_date
     , type_etude
     , type_espace
     , comment
     , timestamp_create
     , timestamp_update
     , created_by_id
     , project_manager_id
     , updated_by_id
     , uuid
FROM imports.v_import_studies
ON CONFLICT(uuid) DO NOTHING
;

-- test de voir si les etudes sont déjà existante
SELECT *
FROM dbchiro.management_study ms;

ROLLBACK;
-- Import places

INSERT INTO dbchiro.sights_place( uuid
                                , name
                                , is_hidden
                                , is_gite
                                , is_managed
                                , proprietary
                                , convention
                                , convention_file
                                , map_file
                                , photo_file
                                , habitat
                                , altitude
                                , id_bdcavite
                                , plan_localite
                                , comment
                                , other_imported_data
                                , telemetric_crossaz
                                , bdsource
                                , id_bdsource
                                , x
                                , y
                                , geom
                                , timestamp_create
                                , timestamp_update
                                , created_by_id
                                , domain_id
                                , landcover_id
                                , metaplace_id
                                , municipality_id
                                , precision_id
                                , territory_id
                                , type_id
                                , updated_by_id
                                , extra_data)
SELECT uuid
     , name
     , is_hidden
     , is_gite
     , is_managed
     , proprietary
     , convention
     , convention_file
     , map_file
     , photo_file
     , habitat
     , altitude
     , id_bdcavite
     , plan_localite
     , comment
     , other_imported_data
     , telemetric_crossaz
     , bdsource
     , id_bdsource
     , x
     , y
     , geom
     , timestamp_create
     , timestamp_update
     , created_by_id
     , domain_id
     , landcover_id
     , metaplace_id
     , municipality_id
     , precision_id
     , territory_id
     , type_id
     , updated_by_id
     , extra_data
FROM imports.v_import_place
ON CONFLICT (uuid) DO NOTHING
;

-- import sessions


INSERT INTO dbchiro.sights_session( uuid
                                  , name
                                  , date_start
                                  , time_start
                                  , date_end
                                  , time_end
                                  , data_file
                                  , is_confidential
                                  , comment
                                  , bdsource
                                  , id_bdsource
                                  , timestamp_create
                                  , timestamp_update
                                  , contact_id
                                  , created_by_id
                                  , main_observer_id
                                  , place_id
                                  , study_id
                                  , updated_by_id
                                  , extra_data)
SELECT uuid
     , name
     , date_start
     , time_start
     , date_end
     , time_end
     , data_file
     , is_confidential
     , comment
     , bdsource
     , id_bdsource
     , timestamp_create
     , timestamp_update
     , contact_id
     , created_by_id
     , main_observer_id
     , place_id
     , study_id
     , updated_by_id
     , extra_data
FROM imports.v_import_session
WHERE id_session IS NULL
ON CONFLICT(uuid)
    DO NOTHING
;


-- Import observations


INSERT INTO dbchiro.sights_sighting( uuid
                                   , period
                                   , total_count
                                   , breed_colo
                                   , is_doubtful
                                   , id_bdsource
                                   , bdsource
                                   , comment
                                   , timestamp_create
                                   , timestamp_update
                                   , codesp_id
                                   , created_by_id
                                   , observer_id
                                   , session_id
                                   , updated_by_id
                                   , extra_data)
SELECT new_uuid
     , period
     , total_count
     , breed_colo
     , is_doubtful
     , id_bdsource
     , bdsource
     , comment
     , timestamp_create
     , timestamp_update
     , codesp_id
     , created_by_id
     , observer_id
     , session_id
     , updated_by_id
     , extra_data
FROM imports.v_import_sightings
ON CONFLICT (uuid) DO NOTHING
;


-- Import countdetail

INSERT INTO dbchiro.sights_countdetail( uuid
                                      , count
                                      , comment
                                      , timestamp_create
                                      , timestamp_update
                                      , method_id
                                      , precision_id
                                      , sighting_id
                                      , unit_id
                                      , updated_by_id
                                      , extra_data)
SELECT uuid_generate_v4()
     , count
     , comment
     , timestamp_create
     , timestamp_update
     , method_id
     , precision_id
     , id_sighting
     , unit_id
     , updated_by_id
     , extra_data
FROM imports.v_import_sightings_with_single_countdetail
;

COMMIT
;