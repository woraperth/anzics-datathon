set search_path to public,mimiciii;
SELECT

-- Patient Identifying Information
  co.subject_id
, co.hadm_id
, co.icustay_id

-- Patient Demographics
, round((date_part('epoch'::text, ie.intime - pat.dob) / ((60 * 60 * 24)::numeric * 365.242)::double precision)::numeric, 4) AS age
, pat.gender as gender
, adm.admittime as hospital_admission
, adm.dischtime as hospital_discharge
, round((date_part('epoch'::text, adm.dischtime - adm.admittime) / (60 * 60 * 24)::double precision)::numeric, 4) AS hospital_los
, adm.ethnicity as ethnicity
, ie.intime as icu_admission
, ie.outtime as icu_discharge
, round((date_part('epoch'::text, ie.outtime - ie.intime) / (60 * 60 * 24)::double precision)::numeric, 4) AS icu_los
, adm.admission_type AS admission_type
--, adm.hospital_expire_flag

-- Record Details
, pvt.hr
, ROW_NUMBER() over (PARTITION BY pvt.icustay_id ORDER BY pvt.hr) as rn

-- Vitals & Haemodynamics
, heart_rate
, abp_sys
, abp_dia
, abp_mean
, cardiac_output
, cvp
, respiratory_rate
, temperature
, spo2

-- Pathology

-- --- FBE
, hemoglobin
, hematocrit
, white_cell_count
, platelet_count

-- --- UEC
, sodium
, potassium
, chloride
, bicarbonate
, urea
, creatinine
, aniongap
, lactate

-- --- LFT
, ast
, alt
, alp
, ggt
, albumin
, bilirubin

-- --- CMP
, calcium
, magnesium
, phosphate

-- --- Coags
, inr
, pt
, ptt

-- --- Others
, mp_labs_custom.glucose as glucose
--, bands

-- Ins
, in_saline
, in_csl
, in_albumin
, in_starch
, in_total

-- Outs
, out_urine
, out_total

-- Join from icustays
, dense_rank() OVER (PARTITION BY co.subject_id ORDER BY adm.admittime) AS hospstay_seq,
CASE
WHEN dense_rank() OVER (PARTITION BY co.subject_id ORDER BY adm.admittime) = 1 THEN 'Y'::text
ELSE 'N'::text
END AS first_hosp_stay,
ie.intime,
ie.outtime,
round((date_part('epoch'::text, ie.outtime - ie.intime) / (60 * 60 * 24)::double precision)::numeric, 4) AS los_icu,
dense_rank() OVER (PARTITION BY co.hadm_id ORDER BY ie.intime) AS icustay_seq,
CASE
WHEN dense_rank() OVER (PARTITION BY co.hadm_id ORDER BY ie.intime) = 1 THEN 'Y'::text
ELSE 'N'::text
END AS first_icu_stay


-- join mp_labs

from mp_hourly_cohort co
-- all ICUSTAY_IDs are in admissions / patients
-- so we can safely inner join
inner join icustays ie
  on co.icustay_id = ie.icustay_id
inner join admissions adm
  on co.hadm_id = adm.hadm_id
inner join patients pat
  on co.subject_id = pat.subject_id
left join vitals pvt
  on co.icustay_id = pvt.icustay_id
  and co.hr = pvt.hr
left join mp_labs_custom
  on co.icustay_id = mp_labs_custom.icustay_id
  and co.hr = mp_labs_custom.hr
left join inputs inp
  on co.icustay_id = inp.icustay_id
  and co.hr = inp.hr
left join outputs outp
  on co.icustay_id = outp.icustay_id
  and co.hr = outp.hr

order by co.subject_id, co.hadm_id, co.icustay_id, co.hr;