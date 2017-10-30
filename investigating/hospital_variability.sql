drop table hospital_variability;
create table hospital_variability as
select measure_id, variance(score) var_id, count(*) count
from care_transformed
group by measure_id
having count > 30;
select h.measure_id,m.measure_name,h.var_id 
from hospital_variability h, transform_measures m
where h.measure_id = m.measure_id
order by var_id desc
limit 10;