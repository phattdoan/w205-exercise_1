drop table correlation;
create table correlation as
select b.provider_id, b.total_score
, s.nurse_rating, s.doctor_rating
, s.staff_rating,s.pain_rating
,s.comm_about_meds_rating, s.environment_rating
, s.discharge_rating, s.overall_performance_rating
from top_hospitals b, transform_surveys s
where b.provider_id = s.provider_id;
select 
concat('Correlation between hospital rating and nurse rating = ',corr(total_score, nurse_rating)) quality_nurse_corr,
concat('Correlation between hospital rating and doctor rating = ', corr(total_score,doctor_rating)) doctor_corr,
concat('Correlation between hospital rating and  staff rating = ',corr(total_score, staff_rating))  quality_staff_rating,
concat('Correlation between hospital rating and pain management=',corr(total_score, pain_rating)) quality_pain_rating,
concat('Correlation between hospital rating and quality communication =',corr(total_score, comm_about_meds_rating)) quality_comm_corr,
concat('Correlation between hospital rating and environment =',corr(total_score, environment_rating)) quality_environment_rating,
concat('Correlation between hospital rating and quality discharge =', corr(total_score, discharge_rating)) quality_discharge_rating,
concat('Correlation between hospital rating and overall hospital performance=',  corr(total_score, overall_performance_rating)) as quality_overall_performance_rating
from correlation;