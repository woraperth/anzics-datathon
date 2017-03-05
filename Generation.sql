set search_path to public,mimiciii;

drop materialized view vitals;
create materialized view vitals as
with vitals as
(
    select ie.subject_id, ie.hadm_id, ie.icustay_id
    , ceil(extract(EPOCH from ce.charttime - ie.intime)/60.0/60.0) as hr
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
)
select icustay_id, hr
-- Vitals & Haemodynamics
, avg(case when VitalID = 1 then valuenum else null end) as heart_rate
, avg(case when VitalID = 2 then valuenum else null end) as abp_sys
, avg(case when VitalID = 3 then valuenum else null end) as abp_dia
, avg(case when VitalID = 4 then valuenum else null end) as abp_mean
, avg(case when VitalID = 5 then valuenum else null end) as cardiac_output
, avg(case when VitalID = 6 then valuenum else null end) as cvp
, avg(case when VitalID = 7 then valuenum else null end) as respiratory_rate
, avg(case when VitalID = 8 then valuenum else null end) as temperature
, avg(case when VitalID = 9 then valuenum else null end) as spo2
, avg(case when VitalID = 10 then valuenum else null end) as glucose
from Vitals
where hr >= 0 and hr < 30
group by icustay_id, hr;

set search_path to public,mimiciii;
drop materialized view mp_labs_custom;
create MATERIALIZED VIEW mp_labs_custom AS 
 WITH mp_labs_custom AS (
         SELECT ie.icustay_id,
            ceil(date_part('epoch'::text, le.charttime - ie.intime) / 60.0::double precision / 60.0::double precision)::smallint AS hr,
                CASE
                    WHEN le.itemid = 50868 THEN 'ANION GAP'::text
                    WHEN le.itemid = 50862 THEN 'ALBUMIN'::text
                    WHEN le.itemid = 51144 THEN 'BANDS'::text
                    WHEN le.itemid = 50882 THEN 'BICARBONATE'::text
                    WHEN le.itemid = 50885 THEN 'BILIRUBIN'::text
                    WHEN le.itemid = 50912 THEN 'CREATININE'::text
                    WHEN le.itemid = 50902 THEN 'CHLORIDE'::text
                    WHEN le.itemid = 50931 THEN 'GLUCOSE'::text
                    WHEN le.itemid = 51221 THEN 'HEMATOCRIT'::text
                    WHEN le.itemid = 51222 THEN 'HEMOGLOBIN'::text
                    WHEN le.itemid = 50813 THEN 'LACTATE'::text
                    WHEN le.itemid = 51265 THEN 'PLATELET'::text
                    WHEN le.itemid = 50971 THEN 'POTASSIUM'::text
                    WHEN le.itemid = 51275 THEN 'PTT'::text
                    WHEN le.itemid = 51237 THEN 'INR'::text
                    WHEN le.itemid = 51274 THEN 'PT'::text
                    WHEN le.itemid = 50983 THEN 'SODIUM'::text
                    WHEN le.itemid = 51006 THEN 'BUN'::text
                    WHEN le.itemid = 51300 THEN 'WBC'::text
                    WHEN le.itemid = 51301 THEN 'WBC'::text
                    WHEN le.itemid = 50878 THEN 'AST'::text
                    WHEN le.itemid = 50861 THEN 'ALT'::text
                    WHEN le.itemid = 50863 THEN 'ALP'::text
                    WHEN le.itemid = 50927 THEN 'GGT'::text
                    WHEN le.itemid = 50893 THEN 'CALCIUM'::text
                    WHEN le.itemid = 50960 THEN 'MAGNESIUM'::text
                    WHEN le.itemid = 50970 THEN 'PHOSPHATE'::text
                    ELSE NULL::text
                END AS label,
                CASE
                    WHEN le.itemid = 50862 AND le.valuenum > 10::double precision THEN NULL::double precision
                    WHEN le.itemid = 50868 AND le.valuenum > 10000::double precision THEN NULL::double precision
                    WHEN le.itemid = 51144 AND le.valuenum < 0::double precision THEN NULL::double precision
                    WHEN le.itemid = 51144 AND le.valuenum > 100::double precision THEN NULL::double precision
                    WHEN le.itemid = 50882 AND le.valuenum > 10000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50885 AND le.valuenum > 150::double precision THEN NULL::double precision
                    WHEN le.itemid = 50806 AND le.valuenum > 10000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50902 AND le.valuenum > 10000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50912 AND le.valuenum > 150::double precision THEN NULL::double precision
                    WHEN le.itemid = 50809 AND le.valuenum > 10000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50931 AND le.valuenum > 10000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50810 AND le.valuenum > 100::double precision THEN NULL::double precision
                    WHEN le.itemid = 51221 AND le.valuenum > 100::double precision THEN NULL::double precision
                    WHEN le.itemid = 50811 AND le.valuenum > 50::double precision THEN NULL::double precision
                    WHEN le.itemid = 51222 AND le.valuenum > 50::double precision THEN NULL::double precision
                    WHEN le.itemid = 50813 AND le.valuenum > 50::double precision THEN NULL::double precision
                    WHEN le.itemid = 51265 AND le.valuenum > 10000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50822 AND le.valuenum > 30::double precision THEN NULL::double precision
                    WHEN le.itemid = 50971 AND le.valuenum > 30::double precision THEN NULL::double precision
                    WHEN le.itemid = 51275 AND le.valuenum > 150::double precision THEN NULL::double precision
                    WHEN le.itemid = 51237 AND le.valuenum > 50::double precision THEN NULL::double precision
                    WHEN le.itemid = 51274 AND le.valuenum > 150::double precision THEN NULL::double precision
                    WHEN le.itemid = 50824 AND le.valuenum > 200::double precision THEN NULL::double precision
                    WHEN le.itemid = 50983 AND le.valuenum > 200::double precision THEN NULL::double precision
                    WHEN le.itemid = 51006 AND le.valuenum > 300::double precision THEN NULL::double precision
                    WHEN le.itemid = 51300 AND le.valuenum > 1000::double precision THEN NULL::double precision
                    WHEN le.itemid = 51301 AND le.valuenum > 1000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50878 AND le.valuenum > 100000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50861 AND le.valuenum > 100000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50863 AND le.valuenum > 100000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50927 AND le.valuenum > 100000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50893 AND le.valuenum > 100000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50960 AND le.valuenum > 100000::double precision THEN NULL::double precision
                    WHEN le.itemid = 50970 AND le.valuenum > 100000::double precision THEN NULL::double precision
                    ELSE le.valuenum
                END AS valuenum
           FROM mimiciii.icustays ie
             LEFT JOIN mimiciii.labevents le ON le.hadm_id = ie.hadm_id AND le.charttime >= ie.intime AND le.charttime <= ie.outtime
          WHERE (le.itemid = ANY (ARRAY[50868, 50862, 51144, 50882, 50885, 50912, 50902, 50931, 51221, 51222, 50813, 51265, 50971, 51275, 51237, 51274, 50983, 51006, 51301, 51300, 50878, 50861, 50863, 50927, 50893, 50960, 50970])) AND le.valuenum IS NOT NULL AND le.valuenum > 0::double precision
        )
 SELECT mp_labs_custom.icustay_id,
    mp_labs_custom.hr,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'HEMOGLOBIN'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS hemoglobin,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'HEMATOCRIT'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS hematocrit,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'WBC'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS white_cell_count,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'PLATELET'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS platelet_count,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'SODIUM'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS sodium,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'POTASSIUM'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS potassium,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'CHLORIDE'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS chloride,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'BICARBONATE'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS bicarbonate,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'BUN'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS urea,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'CREATININE'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS creatinine,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'ANION GAP'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS aniongap,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'LACTATE'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS lactate,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'AST'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS ast,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'ALT'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS alt,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'ALP'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS alp,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'GGT'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS ggt,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'ALBUMIN'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS albumin,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'BILIRUBIN'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS bilirubin,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'CALCIUM'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS calcium,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'MAGNESIUM'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS magnesium,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'PHOSPHATE'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS phosphate,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'INR'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS inr,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'PT'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS pt,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'PTT'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS ptt,
    avg(
        CASE
            WHEN mp_labs_custom.label = 'GLUCOSE'::text THEN mp_labs_custom.valuenum
            ELSE NULL::double precision
        END) AS glucose
   FROM mp_labs_custom
where hr >= 0 and hr < 30
  GROUP BY mp_labs_custom.icustay_id, mp_labs_custom.hr;


set search_path to public,mimiciii;
drop materialized view inputs;
create materialized view inputs as
with inputs_stg as
(
  select ie.icustay_id
    , ceil(extract(EPOCH from iec.charttime - ie.intime)/60.0/60.0) as hr
    , case when itemid in (30018, 30020, 30143, 30160, 30168, 30353, 30352, 30296, 30321, 30186, 30185) then iec.amount else null end as in_saline
    , case when itemid in (30021, 30159) then iec.amount else null end as in_csl
    , case when itemid in (30009, 30008) then iec.amount else null end as in_albumin
    , case when itemid in (30011, 30012) then iec.amount else null end as in_starch
    , case when amountuom = 'ml' then iec.amount else null end as in_total

  from icustays ie
  inner join inputevents_cv iec
    on ie.icustay_id = iec.icustay_id
)
  SELECT
    icustay_id, hr
    , sum(in_saline) as in_saline
    , sum(in_csl) as in_csl
    , sum(in_albumin) as in_albumin
    , sum(in_starch) as in_starch
    , sum(in_total) as in_total
  FROM inputs_stg
  where hr >= 0 and hr < 30
  group by icustay_id, hr;


-- Add output
set search_path to public,mimiciii;
drop materialized view outputs;
create materialized view outputs as
with outputs_stg as
(
  select ie.icustay_id
  , ceil(extract(EPOCH from oe.charttime - ie.intime)/60.0/60.0) as hr
  , case when itemid in (
  		40055, -- "Urine Out Foley"
		43175, -- "Urine ."
		40069, -- "Urine Out Void"
		40094, -- "Urine Out Condom Cath"
		40715, -- "Urine Out Suprapubic"
		40473, -- "Urine Out IleoConduit"
		40085, -- "Urine Out Incontinent"
		40057, -- "Urine Out Rt Nephrostomy"
		40056, -- "Urine Out Lt Nephrostomy"
		40405, -- "Urine Out Other"
		40428, -- "Urine Out Straight Cath"
		40086,--    Urine Out Incontinent
		40096, -- "Urine Out Ureteral Stent #1"
		40651 -- "Urine Out Ureteral Stent #2"
  ) then oe.value else null end as out_urine
  , case when valueuom = 'ml' then oe.value else null end as out_total
  from icustays ie
  inner join outputevents oe
    on ie.icustay_id = oe.icustay_id
)
  select icustay_id, hr
  , sum(out_urine) as out_urine
  , sum(out_total) as out_total
  from outputs_stg
  where hr >= 0 and hr < 30
  group by icustay_id, hr;