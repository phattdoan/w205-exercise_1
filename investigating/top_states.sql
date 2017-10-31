-- top states
drop table top_50_hospitals;
create table top_50_hospitals as
select i.hospital_name
	, i.city
	, i.state
	, i.hospital_ownership
	, top.total_score 
	from top_hospitals top
	left join hospital_info i
		on i.provider_id = top.provider_id
	order by total_score desc
	limit 50;
select th50.state, cast(count(*)/first(c.hospital_count) as float) as perct_top_50_hospitals
from top_50_hospitals th50
left join states_hospital_count c
	on th50.state = c.state
group by th50.state
order by perct_top_50_hospitals desc;