MODEL(
        name omop_db.DRUG_EXPOSURE,
        kind FULL,
        columns(
                drug_exposure_id INT NOT NULL,
                person_id INT NOT NULL,
                drug_concept_id INT NOT NULL,
                drug_exposure_start_date DATE NOT NULL,
                drug_exposure_start_datetime TIMESTAMP,
                drug_exposure_end_date DATE NOT NULL,
                drug_exposure_end_datetime TIMESTAMP,
                verbatim_end_date DATE,
                drug_type_concept_id INT NOT NULL,
                stop_reason VARCHAR(20),
                refills INT,
                quantity NUMERIC,
                days_supply INT,
                sig TEXT,
                route_concept_id INT,
                lot_number VARCHAR(50),
                provider_id INT,
                visit_occurrence_id INT,
                visit_detail_id INT,
                drug_source_value VARCHAR(50),
                drug_source_concept_id INT,
                route_source_value VARCHAR(50),
                dose_unit_source_value VARCHAR(50)
        )
);

SELECT o.obs_id                                         AS drug_exposure_id,
       o.person_id                                      AS person_id,
       concept_mapping.conceptId                        AS drug_concept_id,
       DATE(o.obs_datetime)                             AS drug_exposure_start_date,
       o.obs_datetime                                   AS drug_exposure_start_datetime,
       COALESCE(DATE(o.obs_datetime), CURDATE())        AS drug_exposure_end_date,
       o.obs_datetime                                   AS drug_exposure_end_datetime,
       NULL                                             AS verbatim_end_date,
       38000177                                         AS drug_type_concept_id,
       NULL                                             AS stop_reason,
       NULL                                             AS refills,
       o.value_numeric                                  AS quantity,
       NULL                                             AS days_supply,
       o.value_text                                     AS sig,
       NULL                                             AS route_concept_id,
       NULL                                             AS lot_number,
       creator.person_id                                AS provider_id,
       e.visit_id                                       AS visit_occurrence_id,
       NULL                                             AS visit_detail_id,
       LEFT(c.name, 50)                                 AS drug_source_value,
       concept_mapping.conceptId                        AS drug_source_concept_id,
       NULL                                             AS route_source_value,
       cn.units                                         AS dose_unit_source_value
FROM openmrs.obs AS o
         INNER JOIN openmrs.encounter e ON o.encounter_id = e.encounter_id
         INNER JOIN openmrs.concept_name c ON o.concept_id = c.concept_id AND c.locale = 'en' AND c.voided = 0
         LEFT JOIN openmrs.concept_numeric cn ON o.concept_id = cn.concept_id
         LEFT JOIN raw.CONCEPT_MAPPING concept_mapping
                   ON o.concept_id = concept_mapping.sourceCode
         INNER JOIN openmrs.users creator ON o.creator = creator.user_id
WHERE o.voided = 0
  AND concept_mapping.domainId = 'Drug';