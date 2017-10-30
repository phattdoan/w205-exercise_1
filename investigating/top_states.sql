-- top states
select th50.state, cast(count(*)/hospital_count as float) as perct_top_50_hospitals
from (select i.hospital_name, i.city, i.state, i.hospital_ownership,
	top.total_score 
	from top_hospitals top
	left join hospital_info i
		on i.provider_id = top.provider_id
	order by total_score desc
	limit 50) top_50_hospitals th50
left join states_hospital_count c
	th50.state = c.state;