select a."id" as Id_Benef, a."fullName" as fullName_Benef, a."dateOfBirth" as dateOfBirth, a."gender" as gender, a."identityCard" as identityCard_Benef,
	a."nativePlace" as nativePlace, a."relation" as relation_NCC, a."beneficiary", a."isNcc" as isNCC, a."status" as status, a."province" as province,
	a."district" as district, a."ward" as ward
from public."infoBeneficiaries" a
order by id asc

