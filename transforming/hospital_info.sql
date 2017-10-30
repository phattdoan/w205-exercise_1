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
	meets_criteria
from hospitals
where hospital_type like '%Acute%' 
	and emergency_services = 'Yes' and meets_criteria = 'Y';
