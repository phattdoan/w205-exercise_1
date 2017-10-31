drop table care_transformed;
create table care_transformed as
select
	provider_id,
	condition,
	measure_id,
	measure_name,
	cast(score as float) score
from care
where (score not like 'Not%') or (score not like 'High%') or
	(score not like 'Med%') or (score is not null);