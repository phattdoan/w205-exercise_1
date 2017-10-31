drop table procedure_variability;
create table procedure_variability as
select measure_id, variance(score) var_id, count(*) count
from care_transformed
group by measure_id
having count > 30;
select m.measure_id, m.measure_name, pv.var_id 
from procedure_variability pv, measures m
where m.measure_id = m.measure_id
order by pv.var_id desc
limit 20;