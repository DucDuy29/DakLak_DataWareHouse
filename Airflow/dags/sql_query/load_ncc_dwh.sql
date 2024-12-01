use LDTBXH_Dwh;
SET SQL_SAFE_UPDATES = 0;

start transaction;

-- Update DimNCC với SCD Type 2
UPDATE DimNCC
SET
	RowIsCurrent = FALSE,
    RowEndDate = current_timestamp
WHERE
	EXISTS (
        SELECT 1
        FROM LDTBXH_Stg.Stg_DimNCC a
        WHERE 
            a.NCC_Code = DimNCC.NCC_Code
            AND a.identityCard_NCC = DimNCC.identityCard_NCC
            AND DimNCC.RowEndDate IS NULL
            AND (
                a.nativePlace_NCC <> DimNCC.nativePlace_NCC OR
                a.province_NCC <> DimNCC.province_NCC OR
                a.district_NCC <> DimNCC.district_NCC OR
                a.ward_NCC <> DimNCC.ward_NCC 
				)
    );

-- Insert Data vào DimNCC nếu như chưa có dữ liệu
insert into DimNCC
	(Id_NCC, NCC_Code, fullName_NCC, gender, dateOfBirth, ethnic, identityCard_NCC, nativePlace_NCC, province_NCC,
		district_NCC, ward_NCC, decided, medicalcondition, RowIsCurrent, RowStartDate, RowEndDate)
select
	a.Id_NCC, a.NCC_Code, a.fullName_NCC, a.gender, a.dateOfBirth, a.ethnic, a.identityCard_NCC,
    a.nativePlace_NCC, a.province_NCC, a.district_NCC, a.ward_NCC, a.decided, a.medicalcondition,
    TRUE as RowIsCurrent, current_timestamp as RowStartDate, NULL as RowEndDate
from
	LDTBXH_Stg.Stg_DimNCC a
where
	(a.NCC_Code, a.identityCard_NCC, a.nativePlace_NCC, a.province_NCC, a.district_NCC, a.ward_NCC) not in
    (select NCC_Code, identityCard_NCC, nativePlace_NCC, province_NCC, district_NCC, ward_NCC from DimNCC);

-- Update DimBenef với SCD Type 2
UPDATE DimBenef
SET
	RowIsCurrent = FALSE,
    RowEndDate = current_timestamp
WHERE
	EXISTS (
        SELECT 1
        FROM LDTBXH_Stg.Stg_DimBenef b
        WHERE 
            b.fullName_Benef = DimBenef.fullName_Benef
            AND b.identityCard_Benef = DimBenef.identityCard_Benef
            AND DimBenef.RowEndDate IS NULL
            AND (
                b.nativePlace_Benef <> DimBenef.nativePlace_Benef OR
                b.province_NCC <> DimBenef.province_NCC OR
                b.district_NCC <> DimBenef.district_NCC OR
                b.ward_NCC <> DimBenef.ward_NCC 
				)
    );

-- Insert Data vào DimBenef nếu như chưa có dữ liệu
insert into DimBenef
	(Id_Benef, fullName_Benef, dateOfBirth, gender, identityCard_Benef, nativePlace_Benef, relation_NCC, beneficiary,
		IsNCC, status, province_NCC, district_NCC, ward_NCC, RowIsCurrent, RowStartDate, RowEndDate)
select
	b.Id_Benef, b.fullName_Benef, b.dateOfBirth, b.gender, b.identityCard_Benef, b.nativePlace_Benef,
    b.relation_NCC, b.beneficiary, b.IsNCC, b.status, b.province_NCC, b.district_NCC, b.ward_NCC,
    TRUE as RowIsCurrent, current_timestamp as RowStartDate, NULL as RowEndDate
from
	LDTBXH_Stg.Stg_DimBenef b
where
	(b.fullName_Benef, b.identityCard_Benef, b.nativePlace_Benef, b.province_NCC, b.district_NCC, b.ward_NCC) not in
    (select fullName_Benef, identityCard_Benef, nativePlace_Benef, province_NCC, district_NCC, ward_NCC from DimBenef);

-- Insert Data vào BaoCaoNCC_Fact
insert into BaoCaoNCC_Fact
    (NCC_Key, Benef_Key, NCC_Code, fullName_NCC, gender, dateOfBirth, ethnic, 
     identityCard_NCC, identityCard_Benef, nativePlace_NCC, decided, medicalCondition)
select
    (SELECT NCC_Key FROM DimNCC WHERE Id_NCC = src.Id_NCC LIMIT 1) AS NCC_Key,
    (SELECT Benef_Key FROM DimBenef WHERE Id_Benef = src.Id_Benef LIMIT 1) AS Benef_Key,
    src.NCC_Code, src.fullName_NCC, src.gender, src.dateOfBirth, src.ethnic,
    src.identityCard_NCC, src.identityCard_Benef, src.nativePlace_NCC, src.decided, src.medicalCondition
from
    LDTBXH_Stg.Stg_BaoCaoNCC_Fact src
where
    (SELECT NCC_Key FROM DimNCC WHERE Id_NCC = src.Id_NCC LIMIT 1) is not null
    and (SELECT Benef_Key FROM DimBenef WHERE Id_Benef = src.Id_Benef LIMIT 1) is not null
    and NOT EXISTS (
        SELECT 1
        FROM BaoCaoNCC_Fact
        WHERE NCC_Key = (SELECT NCC_Key FROM DimNCC WHERE Id_NCC = src.Id_NCC LIMIT 1)
        AND Benef_Key = (SELECT Benef_Key FROM DimBenef WHERE Id_Benef = src.Id_Benef LIMIT 1)
    );
commit;