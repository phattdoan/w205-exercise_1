drop table transform_surveys;
create table transform_surveys as
select provider_number provider_id,
	cast(communication_with_nurses_performance_rate as float) as nurse_rating,
	cast(communication_with_doctors_performance_rate as float) as doctor_rating,
	cast(responsiveness_of_hospital_staff_performance_rate as float) as staff_rating,
	cast(pain_management_performance_rate as float) as pain_rating,
	cast(communication_about_medicines_performance_rate as float) as comm_about_meds_rating,
	cast(cleanliness_and_quietness_of_hospital_environment_performance_rate as float) as environment_rating,
	cast(discharge_information_performance_rate as float) as discharge_rating,
	cast(overall_rating_of_hospital_performance_rate as float) as overall_performance_rating
from surveys;
