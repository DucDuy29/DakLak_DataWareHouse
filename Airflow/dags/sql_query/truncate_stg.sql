USE LDTBXH_Stg;

TRUNCATE TABLE Stg_DimHoGiaDinh;
TRUNCATE TABLE Stg_DimRaSoat;
TRUNCATE TABLE Stg_HoGDRaSoat_Fact;
TRUNCATE TABLE Stg_Date;
TRUNCATE TABLE Stg_DimNCC;
TRUNCATE TABLE Stg_DimBenef;
TRUNCATE TABLE Stg_BaoCaoNCC_Fact;

START TRANSACTION;
    INSERT INTO DimAuditForeigned(process_name, start_at, finished_at, information, status) VALUES
    ('data integration', NOW(), NOW(), 'refreshed data', 'PENDING');
COMMIT;
