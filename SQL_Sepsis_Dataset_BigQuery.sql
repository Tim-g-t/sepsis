CREATE SCHEMA IF NOT EXISTS `sepsis-model.Final_version_sepsis_data`;

-- ===================================================================
-- 1) BUILD 2 000-PATIENT COHORT (M ≥ 50 with ICU stay: 1 000 sepsis3+, 1 000 controls)
-- ===================================================================
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.sepsis_cohort` AS
WITH
  male_50_plus_with_icu AS (
    SELECT DISTINCT p.subject_id
    FROM `physionet-data.mimiciv_3_1_hosp.patients` AS p
    JOIN `physionet-data.mimiciv_3_1_icu.icustays` AS i
      USING(subject_id)
    WHERE p.anchor_age >= 50
      AND p.gender = 'M'
  ),
  sepsis_subjects AS (
    SELECT DISTINCT subject_id
    FROM `physionet-data.mimiciv_derived.sepsis3`
    WHERE sepsis3 = TRUE
  ),
  sepsis_sample AS (
    SELECT subject_id, 'sepsis' AS cohort
    FROM male_50_plus_with_icu
    WHERE subject_id IN (SELECT subject_id FROM sepsis_subjects)
    ORDER BY subject_id
    LIMIT 1000
  ),
  control_sample AS (
    SELECT subject_id, 'control' AS cohort
    FROM male_50_plus_with_icu
    WHERE subject_id NOT IN (SELECT subject_id FROM sepsis_subjects)
    ORDER BY subject_id
    LIMIT 1000
  )
SELECT * FROM sepsis_sample
UNION ALL
SELECT * FROM control_sample
;

-- ===================================================================
-- 2) ICU WINDOW (intime/outtime) FROM TRUE ICU STAYS
--    ← add hadm_id so downstream joins can use it
-- ===================================================================
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.icu_window` AS
SELECT
  c.cohort,
  i.subject_id,
  i.hadm_id,        -- newly added
  i.stay_id,
  i.intime,
  i.outtime
FROM
  `sepsis-model.Final_version_sepsis_data.sepsis_cohort` AS c
JOIN
  `physionet-data.mimiciv_3_1_icu.icustays` AS i
USING(subject_id)
;

-- ===================================================================
-- 3) RAW SIGNALS DURING ICU STAY
-- ===================================================================
-- 3a) Vitals
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.vitalsign_raw` AS
SELECT DISTINCT w.cohort, v.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.vitalsign` AS v
  ON v.subject_id = w.subject_id
 AND v.stay_id    = w.stay_id
WHERE v.charttime BETWEEN w.intime AND w.outtime
;

-- 3b) Arterial blood gases
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.bg_raw` AS
SELECT DISTINCT w.cohort, bg.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.bg` AS bg
  ON bg.subject_id = w.subject_id
 AND bg.hadm_id    = w.hadm_id
WHERE bg.charttime BETWEEN w.intime AND w.outtime
;

-- 3c) Complete blood count
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.complete_blood_count_raw` AS
SELECT DISTINCT w.cohort, cbc.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.complete_blood_count` AS cbc
  ON cbc.subject_id = w.subject_id
 AND cbc.hadm_id    = w.hadm_id
WHERE cbc.charttime BETWEEN w.intime AND w.outtime
;

-- 3d) Blood differential
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.blood_differential_raw` AS
SELECT DISTINCT w.cohort, bd.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.blood_differential` AS bd
  ON bd.subject_id = w.subject_id
 AND bd.hadm_id    = w.hadm_id
WHERE bd.charttime BETWEEN w.intime AND w.outtime
;

-- 3e) All lab events
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.labevents_raw` AS
SELECT DISTINCT w.cohort, le.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_3_1_hosp.labevents` AS le
  ON le.subject_id = w.subject_id
 AND le.hadm_id    = w.hadm_id
WHERE le.charttime BETWEEN w.intime AND w.outtime
;

-- 3f) Microbiology cultures
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.microbiologyevents_raw` AS
SELECT DISTINCT w.cohort, m.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_3_1_hosp.microbiologyevents` AS m
  ON m.subject_id = w.subject_id
 AND m.hadm_id    = w.hadm_id
WHERE m.charttime BETWEEN w.intime AND w.outtime
;

-- 3g) EMAR → EMAR_DETAIL (med administration) — fixed: no non-existent time filter
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.emar_detail_raw` AS
SELECT DISTINCT w.cohort, e.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_3_1_hosp.emar`         AS em
  ON em.subject_id = w.subject_id
 AND em.hadm_id    = w.hadm_id
JOIN `physionet-data.mimiciv_3_1_hosp.emar_detail`  AS e
  ON e.emar_id     = em.emar_id
;

-- 3h) Prescriptions (orders)
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.prescriptions_raw` AS
SELECT DISTINCT w.cohort, p.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` AS p
  ON p.subject_id = w.subject_id
 AND p.hadm_id    = w.hadm_id
WHERE p.starttime BETWEEN w.intime AND w.outtime
   OR p.stoptime  BETWEEN w.intime AND w.outtime
;

-- 3i) Pharmacy fills
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.pharmacy_raw` AS
SELECT DISTINCT w.cohort, ph.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_3_1_hosp.pharmacy` AS ph
  ON ph.subject_id = w.subject_id
 AND ph.hadm_id    = w.hadm_id
WHERE ph.starttime BETWEEN w.intime AND w.outtime
   OR ph.stoptime  BETWEEN w.intime AND w.outtime
;

-- ===================================================================
-- 4) DERIVED-PIPELINE FEATURE TABLES
-- ===================================================================

-- ===================================================================
-- 4) DERIVED-PIPELINE FEATURE TABLES
-- ===================================================================
-- 4a) Full time-stamped SOFA
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.sofa_raw` AS
SELECT DISTINCT
  w.cohort,
  s.*
FROM
  `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN
  `physionet-data.mimiciv_derived.sofa` AS s
  ON s.stay_id = w.stay_id
WHERE
  s.starttime >= w.intime
  AND s.endtime   <= w.outtime
;

-- 4b) First-24 h SOFA summary
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.first_day_sofa_raw` AS
SELECT DISTINCT w.cohort, f.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.first_day_sofa` AS f
  ON f.subject_id = w.subject_id
 AND f.stay_id    = w.stay_id
;

-- 4c) SIRS criteria
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.sirs_raw` AS
SELECT DISTINCT w.cohort, sr.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.sirs`     AS sr
  ON sr.subject_id = w.subject_id
 AND sr.stay_id    = w.stay_id
;

-- 4d) Suspicion‐of‐infection timestamps
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.suspicion_of_infection_raw` AS
SELECT DISTINCT w.cohort, soi.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.suspicion_of_infection` AS soi
  ON soi.subject_id = w.subject_id
 AND soi.stay_id    = w.stay_id
;

-- 4e) Sepsis-3 labels & timestamps
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.sepsis3_raw` AS
SELECT DISTINCT w.cohort, s3.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.sepsis3`    AS s3
  ON s3.subject_id = w.subject_id
 AND s3.stay_id    = w.stay_id
WHERE s3.sepsis3 = TRUE
;

-- 4f) Charlson comorbidity index
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.charlson_raw` AS
SELECT DISTINCT
  w.cohort,
  ch.*
FROM
  `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN
  `physionet-data.mimiciv_derived.charlson` AS ch
  ON ch.subject_id = w.subject_id
 AND ch.hadm_id     = w.hadm_id
;

-- 4g) Code-status flags
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.code_status_raw` AS
SELECT DISTINCT w.cohort, cs.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.code_status` AS cs
  ON cs.subject_id = w.subject_id
 AND cs.stay_id    = w.stay_id
;

-- 4h) Severity scores: APS III, SAPS II, OASIS, LODS, MELD
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.apsiii_raw` AS
SELECT DISTINCT w.cohort, a.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.apsiii` AS a
  ON a.subject_id = w.subject_id
 AND a.stay_id    = w.stay_id
;
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.sapsii_raw` AS
SELECT DISTINCT w.cohort, s.* 
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.sapsii` AS s
  ON s.subject_id = w.subject_id
 AND s.stay_id    = w.stay_id
;
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.oasis_raw` AS
SELECT DISTINCT w.cohort, o.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.oasis` AS o
  ON o.subject_id = w.subject_id
 AND o.stay_id    = w.stay_id
;
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.lods_raw` AS
SELECT DISTINCT w.cohort, l.* 
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.lods` AS l
  ON l.subject_id = w.subject_id
 AND l.stay_id    = w.stay_id
;
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.meld_raw` AS
SELECT DISTINCT w.cohort, m.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.meld` AS m
  ON m.subject_id = w.subject_id
 AND m.stay_id    = w.stay_id
;

-- 4i) GCS + first-24 h GCS
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.gcs_raw` AS
SELECT DISTINCT w.cohort, g.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.gcs` AS g
  ON g.subject_id = w.subject_id
 AND g.stay_id    = w.stay_id
;
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.first_day_gcs_raw` AS
SELECT DISTINCT w.cohort, f.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.first_day_gcs` AS f
  ON f.subject_id = w.subject_id
 AND f.stay_id    = w.stay_id
;

-- 4j) Urine output & AKI staging

-- 4j) Urine output & AKI staging
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.urine_output_rate_raw` AS
SELECT
  w.cohort,
  w.subject_id,            -- bring in subject_id from the window
  uor.*                    -- includes stay_id, charttime, output_rate, etc.
FROM
  `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN
  `physionet-data.mimiciv_derived.urine_output_rate` AS uor
  ON uor.stay_id = w.stay_id
WHERE
  uor.charttime BETWEEN w.intime AND w.outtime
;




-- 4k) KDIGO creatinine staging
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.kdigo_creatinine_raw` AS
SELECT
  w.cohort,
  w.subject_id,            -- from the ICU window
  kc.*                      -- stay_id, charttime, creatinine, stage, etc.
FROM
  `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN
  `physionet-data.mimiciv_derived.kdigo_creatinine` AS kc
  ON kc.stay_id = w.stay_id
WHERE
  kc.charttime BETWEEN w.intime AND w.outtime
;
-- 4k-continued) KDIGO urine‐output staging (fixed duplicate subject_id)
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.kdigo_stages_raw` AS
SELECT
  w.cohort,
  w.subject_id,
  ks.* EXCEPT(subject_id)   -- drop ks.subject_id to avoid the duplicate
FROM
  `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN
  `physionet-data.mimiciv_derived.kdigo_stages` AS ks
  ON ks.stay_id = w.stay_id
WHERE
  ks.charttime BETWEEN w.intime AND w.outtime
;

-- ===================================================================
-- 4l) Creatinine baseline
-- ===================================================================
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.creatinine_baseline_raw` AS
SELECT
  w.cohort,
  w.subject_id,
  w.hadm_id,
  cb.* EXCEPT(hadm_id)
FROM
  `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN
  `physionet-data.mimiciv_derived.creatinine_baseline` AS cb
  ON cb.hadm_id = w.hadm_id
;

-- ===================================================================
-- 4m) Vasopressors & inotropes
-- ===================================================================
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.vasoactive_agent_raw` AS
SELECT
  w.cohort,
  w.subject_id,
  w.hadm_id,
  va.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.vasoactive_agent` AS va
  ON va.stay_id = w.stay_id
WHERE
  va.starttime BETWEEN w.intime AND w.outtime
  OR va.endtime   BETWEEN w.intime AND w.outtime
;

CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.norepinephrine_equivalent_dose_raw` AS
SELECT
  w.cohort,
  w.subject_id,
  w.hadm_id,
  ne.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.norepinephrine_equivalent_dose` AS ne
  ON ne.stay_id = w.stay_id
WHERE
  ne.starttime BETWEEN w.intime AND w.outtime
  OR ne.endtime   BETWEEN w.intime AND w.outtime
;

-- ===================================================================
-- 4n) Respiratory support & lines
-- ===================================================================
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.oxygen_delivery_raw` AS
SELECT
  w.cohort,
  w.subject_id,
  w.hadm_id,
  od.* EXCEPT(subject_id)
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.oxygen_delivery` AS od
  ON od.stay_id = w.stay_id
WHERE
  od.charttime BETWEEN w.intime AND w.outtime
;

CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.ventilation_raw` AS
SELECT
  w.cohort,
  w.subject_id,
  w.hadm_id,
  vt.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.ventilation` AS vt
  ON vt.stay_id = w.stay_id
WHERE
  vt.starttime BETWEEN w.intime AND w.outtime
  OR vt.endtime   BETWEEN w.intime AND w.outtime
;

CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.ventilator_setting_raw` AS
SELECT
  w.cohort,
  w.subject_id,
  w.hadm_id,
  vs.* EXCEPT(subject_id)
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.ventilator_setting` AS vs
  ON vs.stay_id = w.stay_id
WHERE
  vs.charttime BETWEEN w.intime AND w.outtime
;

CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.invasive_line_raw` AS
SELECT
  w.cohort,
  w.subject_id,
  w.hadm_id,
  il.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.invasive_line` AS il
  ON il.stay_id = w.stay_id
WHERE
  il.starttime BETWEEN w.intime AND w.outtime
  OR il.endtime   BETWEEN w.intime AND w.outtime
;

-- ===================================================================
-- 4o) Antibiotic administration flags
-- ===================================================================
CREATE OR REPLACE TABLE `sepsis-model.Final_version_sepsis_data.antibiotic_raw` AS
SELECT DISTINCT w.cohort, ab.*
FROM `sepsis-model.Final_version_sepsis_data.icu_window` AS w
JOIN `physionet-data.mimiciv_derived.antibiotic` AS ab
  ON ab.subject_id = w.subject_id
 AND ab.stay_id    = w.stay_id
;



-- ===================================================================
-- 5) OPTIONAL: DEMOGRAPHICS
-- ===================================================================
CREATE OR REPLACE TABLE
  `sepsis-model.Final_version_sepsis_data.patients_raw` AS
SELECT DISTINCT w.cohort, p.*
FROM
  `sepsis-model.Final_version_sepsis_data.sepsis_cohort` AS w
JOIN
  `physionet-data.mimiciv_3_1_hosp.patients` AS p
USING(subject_id)
;