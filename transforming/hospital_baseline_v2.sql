with baseline as(
select 
	provider_id,
	hospital_name,
	cast(case when mortality_national_comparison like 'Above%' then 3 
		when mortality_national_comparison like 'Below%' then 1 
		else 2 end as int) as mortality_score,
	cast(case when safety_of_care_national_comparison like 'Above%' then 3 
		when safety_of_care_national_comparison like 'Below%' then 1 
		else 2 end as int) as safety_score,
	cast(case when readmission_national_comparison like 'Above%' then 3 
		when readmission_national_comparison like 'Below%' then 1 
		else 2 end as int) as readmission_score,
	cast(case when patient_experience_national_comparison like 'Above%' then 3 
		when patient_experience_national_comparison like 'Below%' then 1 
		else 2 end as int) as patient_experience_score,
	cast(case when effectiveness_of_care_national_comparison like 'Above%' then 3 
		when effectiveness_of_care_national_comparison like 'Below%' then 1 
		else 2 end as int) as effectiveness_score,
	cast(case when timeliness_of_care_national_comparison like 'Above%' then 3 
		when timeliness_of_care_national_comparison like 'Below%' then 1 
		else 2 end as int) as timeliness_score,
	cast(case when efficient_use_of_medical_imaging_national_comparisonn like 'Above%' then 3 
		when efficient_use_of_medical_imaging_national_comparison like 'Below%' then 1 
		else 2 end as int) as efficiency_score
from hospital_info
)
create table hospital_baseline as
select 
	provider_id,
	hospital_name,
	mortality_score,
	safety_score,
	readmission_score,
	patient_experience_score,
	effectiveness_score,
	timeliness_score,
	efficiency_score,
	mortality_score + safety_score + readmission_score + patient_experience_score + effectiveness_score + timeliness_score + efficiency_score as total_score
from hospital_info; 