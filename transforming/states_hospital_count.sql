drop table states_hospital_count;
create table states_hospital_count as
select state, cast(count(*) as int) as hospital_count
from hospital_info 
group by state 
order by state;
select * from states_hospital_count;