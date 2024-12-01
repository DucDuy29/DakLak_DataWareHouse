select a."id" as Id_NCC, a."code" as NCC_Code, a."fullName" as fullName_NCC, a."gender" as gender, b."dateOfBirth" as dateOfBirth, b."ethnic" as ethnic, 
	b."identityCard" as identityCard_NCC, a."nativePlace" as nativePlace_NCC, a."province" as province_NCC, a."district" as district_NCC, a."ward" as ward_NCC,
	c."decided" as decided_NCC, c."medicalCondition" as medicalCondition
from public."profiles" a 
join public."infoNccs" b ON a.id = b.id
join public."infos" c ON a.id = c.id
order by a.id asc
