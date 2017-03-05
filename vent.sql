-- WIP

SELECT 
vd.icustay_id,

-- include in which hour
CASE
WHEN extract( hour from  starttime) > 0  THEN 1
ELSE 0 
END as isventilated
FROM mimiciii.ventdurations vd
join mimiciii.icustays ie ON
ie.icustay_id = vd.icustay_id