-- aggregating types of hospitals in the dataset
select hospital_type, count(*) from hospitals group by hospital_type;

-- hospital count by state for acute care hospitals only
select state, count(*) from hospitals where hospital_type like '%Acute%' 
group by state order by state;

-- count hospitals with and without emergency services
select emergency_services, count(*) from hospital_info group by emergency_services;

-- top hospitals based on CMS-aggregated metrics
select hospital_name, mortality_score + safety_score + readmission_score + patient_experience_score + effectiveness_score + timeliness_score + efficiency_score as total_score from hospital_baseline order by total_score desc limit 10; 


-- 
select c.hospital_name, c.provider_id, i.city, i.state, mortality_score + safety_score + readmission_score + patient_experience_score + effectiveness_score + timeliness_score + efficiency_score as total_score from hospital_baseline c left join hospital_info i on c.provider_id = i.provider_id order by total_score desc limit 10;