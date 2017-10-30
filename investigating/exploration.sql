-- aggregating types of hospitals in the dataset
select hospital_type, count(*) from hospitals group by hospital_type;

-- hospital count by state for acute care hospitals only
select state, count(*) from hospitals where hospital_type like '%Acute%' 
group by state order by state;

-- count hospitals with and without emergency services
select emergency_services, count(*) from hospital_info group by emergency_services;
