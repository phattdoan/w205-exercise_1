drop table correlation;
create table correlation as
select b.provider_id, b.rank, s.nurse_rating, s.doctor_rating, s.staff_rating,s.pain_rating,
s.comm_about_meds_rating,s.environment_rating,s.discharge_rating, s.overall_performance_rating
from best_hospitals b, transform_surveys s
where b.provider_id = s.provider_id;
select 
concat('Correlation between hospital rating and nurse rating = ',-1*corr(rank, nurse_rating)) quality_nurse_corr,
concat('Correlation between hospital rating and doctor rating = ', -1*corr(rank,doctor_rating)) doctor_corr,
concat('Correlation between hospital rating and  staff rating = ',-1*corr(rank, staff_rating))  quality_staff_rating,
concat('Correlation between hospital rating and pain management=',-1*corr(rank, pain_rating)) quality_pain_rating,
concat('Correlation between hospital rating and quality communication =',-1*corr(rank, comm_about_meds_rating)) quality_comm_corr,
concat('Correlation between hospital rating and environment =',-1*corr(rank, environment_rating)) quality_environment_rating,
concat('Correlation between hospital rating and quality discharge =',-1* corr(rank, discharge_rating)) quality_discharge_rating,
concat('Correlation between hospital rating and overall hospital performance=', -1* corr(rank, overall_performance_rating)) as quality_overall_performance_rating
from correlation;