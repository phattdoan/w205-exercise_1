--  Top Hospitals
drop table top_hospitals;
create table top_hospitals as
select provider_id,
	mortality_score + safety_score + readmission_score + patient_experience_score + effectiveness_score + timeliness_score + efficiency_score as total_score 
from hospital_baseline;
select i.hospital_name, i.city, i.state, i.hospital_ownership,
	top.total_score 
from top_hospitals top
left join hospital_info i
	on i.provider_id = top.provider_id
order by total_score desc
limit 10;