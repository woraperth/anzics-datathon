set search_path to mimiciii;

SELECT
-- Patient Identifying Information
  pvt.subject_id
, pvt.hadm_id
, pvt.icustay_id
-- Patient Demographics
, round((date_part('epoch'::text, adm.admittime - pat.dob) / ((60 * 60 * 24)::numeric * 365.242)::double precision)::numeric, 0) AS age
, pat.gender as gender
, adm.admittime as hospital_admission_date
, adm.dischtime as hospital_discharge_date
, round((date_part('epoch'::text, adm.dischtime - adm.admittime) / (60 * 60 * 24)::double precision)::numeric, 4) AS hospital_los
, ie.intime as icu_admission_date
, ie.outtime as icu_discharge_date
, round((date_part('epoch'::text, ie.outtime - ie.intime) / (60 * 60 * 24)::double precision)::numeric, 4) AS icu_los
, adm.ethnicity as ethnicity
, adm.admission_type as admission_type
--, adm.hospital_expire_flag
-- Record Details
, pvt.hr as hour
, ROW_NUMBER() over (PARTITION BY pvt.icustay_id ORDER BY pvt.hr) as rn
-- Haemodynamics
, avg(case when VitalID = 1 then valuenum else null end) as heart_rate
, avg(case when VitalID = 2 then valuenum else null end) as abp_sys
, avg(case when VitalID = 3 then valuenum else null end) as abp_dia
, avg(case when VitalID = 4 then valuenum else null end) as abp_mean
, avg(case when VitalID = 5 then valuenum else null end) as cardiac_output
, avg(case when VitalID = 6 then valuenum else null end) as cvp
, avg(case when VitalID = 7 then valuenum else null end) as respiratory_rate
, avg(case when VitalID = 8 then valuenum else null end) as temperature
, min(case when VitalID = 9 then valuenum else null end) as spo2
, min(case when VitalID = 10 then valuenum else null end) as glucose
-- Pathology
------FBE
, mlab.hemoglobin as haemoglobin
, mlab.hematocrit as haematocrit
, mlab.wbc as white_cell_count
, mlab.platelet as platelet_count
------UEC
, mlab.sodium as sodium
, mlab.potassium as potassium
, mlab.chloride as chlorine
, mlab.bun as urea
, mlab.bicarbonate as bicarbonate
, mlab.creatinine as creatinine
, mlab.lactate as lactate
, mlab.aniongap as aniongap
------LFT
, mlab.albumin
, mlab.bilirubin
------Coags
, mlab.inr as inr
, mlab.pt as pt
, mlab.ptt as ptt

------Others
, mlab.glucose as glucose
, mlab.bands
/*
, dense_rank() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) AS hospstay_seq,
    CASE
        WHEN dense_rank() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) = 1 THEN 'Y'::text
        ELSE 'N'::text
    END AS first_hosp_stay,
dense_rank() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_seq,
    CASE
        WHEN dense_rank() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1 THEN 'Y'::text
        ELSE 'N'::text
    END AS first_icu_stay
*/

--------------------------------
-- Haemodynamic View
--------------------------------
FROM (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , ceil(extract(EPOCH from ce.charttime - ie.intime)/60.0/60.0) as hour
  , case
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- heart_rate
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- abp_sys
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- abp_dia
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- abp_mean
    when itemid in (45037, 227543, 228178, 228370, 40909, 41946, 41717, 44970, 224842, 228369, 220088, 44920) and valuenum > 0 and valuenum < 15 then 5 -- cardiac_output
    when itemid in (113, 220074, 1103) and valuenum > 0 and valuenum < 35 then 6 -- cvp
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 7 -- respiratory_rate
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 8 -- temperature in F
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 8 -- temperature in C
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 9 -- spo2
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 10 -- glucose
    else null end as VitalID
      -- convert F to C
  , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum
  from icustays ie
  left join chartevents ce ON ie.icustay_id = ce.icustay_id
  
  and ce.charttime between ie.intime and ie.intime + interval '1' day
  -- exclude rows marked as error
  and ce.error IS DISTINCT FROM 1
  where ce.itemid in
  (
  -- HEART RATE
  211, --"Heart Rate"
  220045, --"Heart Rate"
  -- Systolic/diastolic
  51, --  Arterial BP [Systolic]
  442, -- Manual BP [Systolic]
  455, -- NBP [Systolic]
  6701, --  Arterial BP #2 [Systolic]
  220179, --  Non Invasive Blood Pressure systolic
  220050, --  Arterial Blood Pressure systolic
  8368, --  Arterial BP [Diastolic]
  8440, --  Manual BP [Diastolic]
  8441, --  NBP [Diastolic]
  8555, --  Arterial BP #2 [Diastolic]
  220180, --  Non Invasive Blood Pressure diastolic
  220051, --  Arterial Blood Pressure diastolic
  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  52, --"Arterial BP Mean"
  6702, --  Arterial BP Mean #2
  443, -- Manual BP Mean(calc)
  220052, --"Arterial Blood Pressure mean"
  220181, --"Non Invasive Blood Pressure mean"
  225312, --"ART BP mean"
  -- CARDIAC OUTPUT
  45037,
  227543,
  228178,
  228370,
  40909,
  41946,
  41717,
  44970,
  224842,
  228369,
  220088,
  44920,
  -- CVP
  113,
  220074,
  1103,
  -- RESPIRATORY RATE
  618,--  Respiratory Rate
  615,--  Resp Rate (Total)
  220210,-- Respiratory Rate
  224690, --  Respiratory Rate (Total)
  -- SPO2, peripheral
  646, 220277,
  -- GLUCOSE, both lab and fingerstick
  807,--  Fingerstick Glucose
  811,--  Glucose (70-105)
  1529,-- Glucose
  3745,-- BloodGlucose
  3744,-- Blood Glucose
  225664,-- Glucose finger stick
  220621,-- Glucose (serum)
  226537,-- Glucose (whole blood)
  -- TEMPERATURE
  223762, -- "Temperature Celsius"
  676,  -- "Temperature C"
  223761, -- "Temperature Fahrenheit"
  678 --  "Temperature F"
  )
  and ie.icustay_id < 200010
) pvt

--------------------------------
-- Pathology View
--------------------------------
JOIN mimiciii.admissions adm ON pvt.hadm_id = adm.hadm_id
JOIN mimiciii.patients pat ON pvt.subject_id = pat.subject_id
JOIN mimiciii.icustays ie ON pvt.icustay_id = ie.icustay_id
LEFT JOIN mp_lab mlab ON pvt.hadm_id = mlab.hadm_id

group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.hr, pat.gender, adm.admittime, adm.dischtime, pat.dob, adm.ethnicity, adm.admission_type,
adm.hospital_expire_flag, ie.intime, ie.outtime, adm.subject_id, ie.hadm_id,
mlab.hr,
mlab.aniongap,
mlab.albumin,
mlab.bands,
mlab.bicarbonate,
mlab.bilirubin,
mlab.creatinine,
mlab.chloride,
mlab.glucose,
mlab.hematocrit,
mlab.hemoglobin,
mlab.lactate,
mlab.platelet,
mlab.potassium,
mlab.ptt,
mlab.inr,
mlab.pt,
mlab.sodium,
mlab.bun,
mlab.wbc
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.hr, rn
LIMIT 100;