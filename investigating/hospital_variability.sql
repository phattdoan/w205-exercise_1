drop table procedure_variability;
create table procedure_variability as
select measure_id, variance(score) var_id, count(*) count
from care_transformed
group by measure_id
having count > 30;
select h.measure_id,m.measure_name,h.var_id 
from procedure_variability pv
join measures m
on h.measure_id = m.measure_id
order by var_id desc
limit 10;