drop table care_tranformed;
create table care_tranformed as
	select
	provider_id,
	condition,
	measure_id,
	measure_name,
	cast(score as float) score
from effective_care;
--where (score not like 'Not%') or
--	(score not like 'High%') or
--	(score not like 'Med%')or
--	(score is not null);