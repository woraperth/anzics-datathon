-- This query pivots the vital signs for the first 24 hours of a patient's stay
-- Vital signs include heart rate, blood pressure, respiration rate, and temperature
set search_path to mimiciii;

SELECT
  pvt.subject_id
, pvt.hadm_id
, pvt.icustay_id
, pvt.hr
, pat.gender
, ROW_NUMBER() over (PARTITION BY pvt.icustay_id ORDER BY pvt.hr) as rn
-- Easier names
, avg(case when VitalID = 1 then valuenum else null end) as heart_rate
, avg(case when VitalID = 2 then valuenum else null end) as abp_sys
, avg(case when VitalID = 3 then valuenum else null end) as abp_dia
, avg(case when VitalID = 4 then valuenum else null end) as abp_mean
, avg(case when VitalID = 5 then valuenum else null end) as respiratory_rate
, avg(case when VitalID = 6 then valuenum else null end) as temperature
, min(case when VitalID = 7 then valuenum else null end) as spo2
, min(case when VitalID = 8 then valuenum else null end) as glucose

-- Join from icustays
, adm.admittime,
adm.dischtime,
round((date_part('epoch'::text, adm.dischtime - adm.admittime) / (60 * 60 * 24)::double precision)::numeric, 4) AS los_hospital,
round((date_part('epoch'::text, adm.admittime - pat.dob) / ((60 * 60 * 24)::numeric * 365.242)::double precision)::numeric, 4) AS age,
adm.ethnicity,
adm.admission_type,
adm.hospital_expire_flag,
dense_rank() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) AS hospstay_seq,
    CASE
        WHEN dense_rank() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) = 1 THEN 'Y'::text
        ELSE 'N'::text
    END AS first_hosp_stay,
ie.intime,
ie.outtime,
round((date_part('epoch'::text, ie.outtime - ie.intime) / (60 * 60 * 24)::double precision)::numeric, 4) AS los_icu,
dense_rank() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_seq,
    CASE
        WHEN dense_rank() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1 THEN 'Y'::text
        ELSE 'N'::text
    END AS first_icu_stay

-- Add inputs
,(SELECT sum(iec.amount)
FROM inputevents_cv iec
JOIN mimiciii.icustays ie ON iec.icustay_id = ie.icustay_id
WHERE iec.icustay_id = pvt.icustay_id AND itemid IN (30018, 30020, 30143, 30160, 30168, 30353, 30352, 30296, 30321, 30186, 30185) AND
hr = ceil(extract(EPOCH from iec.charttime - ie.intime)/60.0/60.0)
GROUP BY iec.icustay_id, hr
) as in_saline

,(SELECT sum(iec.amount)
FROM inputevents_cv iec
JOIN mimiciii.icustays ie ON iec.icustay_id = ie.icustay_id
WHERE iec.icustay_id = pvt.icustay_id AND itemid IN (30021, 30159) AND
hr = ceil(extract(EPOCH from iec.charttime - ie.intime)/60.0/60.0)
GROUP BY iec.icustay_id, hr
) as in_csl

,(SELECT sum(iec.amount)
FROM inputevents_cv iec
JOIN mimiciii.icustays ie ON iec.icustay_id = ie.icustay_id
WHERE iec.icustay_id = pvt.icustay_id AND itemid IN (30009, 30008) AND
hr = ceil(extract(EPOCH from iec.charttime - ie.intime)/60.0/60.0)
GROUP BY iec.icustay_id, hr
) as in_albumin

,(SELECT sum(iec.amount)
FROM inputevents_cv iec
JOIN mimiciii.icustays ie ON iec.icustay_id = ie.icustay_id
WHERE iec.icustay_id = pvt.icustay_id AND itemid IN (30011, 30012) AND
hr = ceil(extract(EPOCH from iec.charttime - ie.intime)/60.0/60.0)
GROUP BY iec.icustay_id, hr
) as in_starch

-- Add output
,(SELECT sum(iec.value)
FROM mimiciii.outputevents iec
JOIN mimiciii.icustays ie ON iec.icustay_id = ie.icustay_id
WHERE iec.icustay_id = pvt.icustay_id AND itemid IN (50055, 50056, 50057, 50069, 50085, 50096, 40405, 40428, 40473, 40651, 40715) AND
hr = ceil(extract(EPOCH from iec.charttime - ie.intime)/60.0/60.0)
GROUP BY iec.icustay_id, hr
) as out_urine

,(SELECT sum(iec.value)
FROM mimiciii.outputevents iec
JOIN mimiciii.icustays ie ON iec.icustay_id = ie.icustay_id
WHERE iec.icustay_id = pvt.icustay_id AND
hr = ceil(extract(EPOCH from iec.charttime - ie.intime)/60.0/60.0)
GROUP BY iec.icustay_id, hr
) as out_total


FROM  (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , ceil(extract(EPOCH from ce.charttime - ie.intime)/60.0/60.0) as hr
  , case
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- heart_rate
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- abp_sys
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- abp_dia
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- abp_mean
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- respiratory_rate
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- temperature in F
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- temperature in C
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- spo2
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- glucose

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
  and ie.icustay_id < 200040
) pvt
JOIN mimiciii.admissions adm ON pvt.hadm_id = adm.hadm_id
JOIN mimiciii.patients pat ON pvt.subject_id = pat.subject_id
JOIN mimiciii.icustays ie ON pvt.icustay_id = ie.icustay_id
JOIN mimiciii.inputevents_cv iec ON pvt.icustay_id = iec.icustay_id
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.hr, pat.gender, adm.admittime, adm.dischtime, pat.dob, adm.ethnicity, adm.admission_type,
adm.hospital_expire_flag, ie.intime, ie.outtime, adm.subject_id, ie.hadm_id
order by pvt.icustay_id, pvt.hr, pvt.subject_id, pvt.hadm_id;