--create hospital_info table that serve as a reference table
-- include only acute care hospitals with emergency services
drop table hospital_compare;
create table hospital_compare as
select 
	provider_id,
	hospital_overall_rating,
	mortality_national_comparison,
	safety_of_care_national_comparison,
	readmission_national_comparison,
	patient_experience_national_comparison,
	effectiveness_of_care_national_comparison,
	timeliness_of_care_national_comparison,
	efficient_use_of_medical_imaging_national_comparison
from hospitals
where hospital_type like '%Acute%' 
	and emergency_services = 'Yes' and meets_criteria = 'Y';
