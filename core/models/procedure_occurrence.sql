MODEL(
        name omop_db.PROCEDURE_OCCURRENCE,
        kind FULL,
        columns(
                procedure_occurrence_id INT NOT NULL,
                person_id INT NOT NULL,
                procedure_concept_id INT NOT NULL,
                procedure_date DATE NOT NULL,
                procedure_datetime TIMESTAMP,
                procedure_end_date DATE,
                procedure_end_datetime TIMESTAMP,
                procedure_type_concept_id INT NOT NULL,
                modifier_concept_id INT,
                quantity INT,
                provider_id INT,
                visit_occurrence_id INT,
                visit_detail_id INT,
                procedure_source_value VARCHAR(50),
                procedure_source_concept_id INT,
                modifier_source_value VARCHAR(50)
        )
);

SELECT o.obs_id                                         AS procedure_occurrence_id,
       o.person_id                                      AS person_id,
       concept_mapping.conceptId                        AS procedure_concept_id,
       DATE(o.obs_datetime)                             AS procedure_date,
       o.obs_datetime                                   AS procedure_datetime,
       NULL                                             AS procedure_end_date,
       NULL                                             AS procedure_end_datetime,
       38000275                                         AS procedure_type_concept_id,
       NULL                                             AS modifier_concept_id,
       COALESCE(CAST(o.value_numeric AS INT), 1)        AS quantity,
       creator.person_id                                AS provider_id,
       e.visit_id                                       AS visit_occurrence_id,
       NULL                                             AS visit_detail_id,
       LEFT(c.name, 50)                                 AS procedure_source_value,
       concept_mapping.conceptId                        AS procedure_source_concept_id,
       NULL                                             AS modifier_source_value
FROM openmrs.obs AS o
         INNER JOIN openmrs.encounter e ON o.encounter_id = e.encounter_id
         INNER JOIN openmrs.concept_name c ON o.concept_id = c.concept_id AND c.locale = 'en' AND c.voided = 0
         LEFT JOIN raw.CONCEPT_MAPPING concept_mapping
                   ON o.concept_id = concept_mapping.sourceCode
         INNER JOIN openmrs.users creator ON o.creator = creator.user_id
WHERE o.voided = 0
  AND concept_mapping.domainId = 'Procedure';