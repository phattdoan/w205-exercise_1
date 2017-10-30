--create hospital_info table that serve as a reference table
-- include only acute care hospitals with emergency services
drop table hospital_info;
create table hospital_info as
select 
	provider_id,
	hospital_name,
	city,
	state,
	zip_code,
	hospital_type,
	hospital_ownership,
	emergency_services,
	meets_criteria,
	hospital_overall_rating,
	mortality_national_comparison,
	safety_of_care_national_comparison,
	readmission_national_comparison,
	patient_experience_national_comparison,
	effectiveness_of_care_national_comparison,
	timeliness_of_care_national_comparison,
	efficient_use_of_medical_imaging_national_comparison
from hospitals
where hospital_type like '%Acute%' and emergency_services = 'Yes';
