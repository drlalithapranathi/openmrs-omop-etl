MODEL(
        name omop_db.VISIT_DETAIL,
        kind FULL,
        columns(
                visit_detail_id INT NOT NULL,
                person_id INT NOT NULL,
                visit_detail_concept_id INT NOT NULL,
                visit_detail_start_date DATE NOT NULL,
                visit_detail_start_datetime TIMESTAMP,
                visit_detail_end_date DATE NOT NULL,
                visit_detail_end_datetime TIMESTAMP,
                visit_detail_type_concept_id INT NOT NULL,
                provider_id INT,
                care_site_id INT,
                visit_detail_source_value VARCHAR(50),
                visit_detail_source_concept_id INT,
                admitted_from_concept_id INT,
                admitted_from_source_value VARCHAR(50),
                discharged_to_source_value VARCHAR(50),
                discharged_to_concept_id INT,
                preceding_visit_detail_id INT,
                parent_visit_detail_id INT,
                visit_occurrence_id INT NOT NULL
        )
);

SELECT e.encounter_id                                   AS visit_detail_id,
       e.patient_id                                     AS person_id,
       9202                                             AS visit_detail_concept_id,
       DATE(e.encounter_datetime)                       AS visit_detail_start_date,
       e.encounter_datetime                             AS visit_detail_start_datetime,
       COALESCE(DATE(e.encounter_datetime), CURDATE())  AS visit_detail_end_date,
       e.encounter_datetime                             AS visit_detail_end_datetime,
       44818518                                         AS visit_detail_type_concept_id,
       creator.person_id                                AS provider_id,
       e.location_id                                    AS care_site_id,
       LEFT(et.name, 50)                                AS visit_detail_source_value,
       NULL                                             AS visit_detail_source_concept_id,
       NULL                                             AS admitted_from_concept_id,
       NULL                                             AS admitted_from_source_value,
       NULL                                             AS discharged_to_source_value,
       NULL                                             AS discharged_to_concept_id,
       LAG(e.encounter_id) OVER (
           PARTITION BY e.patient_id
           ORDER BY e.encounter_datetime
       )                                                AS preceding_visit_detail_id,
       NULL                                             AS parent_visit_detail_id,
       e.visit_id                                       AS visit_occurrence_id
FROM openmrs.encounter AS e
         INNER JOIN openmrs.encounter_type et ON e.encounter_type = et.encounter_type_id
         INNER JOIN openmrs.users creator ON e.creator = creator.user_id
WHERE e.voided = 0
  AND e.visit_id IS NOT NULL;