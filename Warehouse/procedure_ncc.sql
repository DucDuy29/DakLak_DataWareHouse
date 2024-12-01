use LDTBXH_Stg;

-- Tạo Procedure cho Stg_DimNCC
DELIMITER $$
CREATE PROCEDURE insert_Stg_DimNCC (
    IN p_Id_NCC INT,
    IN p_NCC_Code VARCHAR(50),
    IN p_fullName_NCC VARCHAR(50),
    IN p_gender VARCHAR(50),
    IN p_dateOfBirth VARCHAR(50),
    IN p_ethnic VARCHAR(50),
    IN p_identityCard_NCC VARCHAR(50),
    IN p_nativePlace_NCC VARCHAR(255),
    IN p_province_NCC varchar(255),
    IN p_district_NCC varchar(255),
    IN p_ward_NCC varchar(255),
    IN p_decided VARCHAR(50),
    IN p_medicalcondition VARCHAR(255),
    IN p_createdAt TIMESTAMP
)
BEGIN
    START TRANSACTION;

    IF p_createdAt >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS') THEN
        INSERT INTO Stg_DimNCC 
            (Id_NCC, NCC_Code, fullName_NCC, gender, dateOfBirth, ethnic, identityCard_NCC,
             nativePlace_NCC, province_NCC, district_NCC, ward_NCC, decided, medicalcondition, createdAt)
        VALUES 
            (p_Id_NCC, p_NCC_Code, p_fullName_NCC, p_gender, p_dateOfBirth, p_ethnic, p_identityCard_NCC,
             p_nativePlace_NCC, p_province_NCC, p_district_NCC, p_ward_NCC, p_decided, p_medicalcondition, p_createdAt);
    END IF;

    COMMIT;
END $$
DELIMITER ;

-- Tạo Procedure cho Stg_DimBenef
DELIMITER $$
CREATE PROCEDURE insert_Stg_DimBenef (
    IN p_Id_Benef INT,
    IN p_fullName_Benef VARCHAR(50),
    IN p_dateOfBirth VARCHAR(50),
    IN p_gender VARCHAR(50),
    IN p_identityCard_Benef VARCHAR(50),
    IN p_nativePlace_Benef VARCHAR(255),
    IN p_relation_NCC VARCHAR(50),
    IN p_beneficiary VARCHAR(50),
    IN p_IsNCC VARCHAR(50),
    IN p_status BOOLEAN,
    IN p_province_NCC varchar(255),
    IN p_district_NCC varchar(255),
    IN p_ward_NCC varchar(255),
    IN p_createdAt TIMESTAMP
)
BEGIN
    START TRANSACTION;

    IF p_createdAt >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS') THEN
        INSERT INTO Stg_DimBenef 
            (Id_Benef, fullName_Benef, dateOfBirth, gender, identityCard_Benef, nativePlace_Benef,
             relation_NCC, beneficiary, IsNCC, status, province_NCC, district_NCC, ward_NCC, createdAt)
        VALUES 
            (p_Id_Benef, p_fullName_Benef, p_dateOfBirth, p_gender, p_identityCard_Benef, p_nativePlace_Benef,
             p_relation_NCC, p_beneficiary, p_IsNCC, p_status, p_province_NCC, p_district_NCC, p_ward_NCC, p_createdAt);
    END IF;
    
    COMMIT;
END $$
DELIMITER ;

-- Tạo Procedure cho Stg_BaoCaoNCC_Fact
DELIMITER $$
CREATE PROCEDURE insert_Stg_BaoCaoNCC_Fact (
    IN p_Id_NCC INT,
    IN p_Id_Benef INT,
    IN p_NCC_Code VARCHAR(50),
    IN p_fullName_NCC VARCHAR(50),
    IN p_gender VARCHAR(50),
    IN p_dateOfBirth VARCHAR(50),
    IN p_ethnic VARCHAR(50),
    IN p_identityCard_NCC VARCHAR(50),
    IN p_identityCard_Benef VARCHAR(50),
    IN p_nativePlace_NCC VARCHAR(255),
    IN p_decided VARCHAR(50),
    IN p_medicalCondition VARCHAR(255)
)
BEGIN
    -- Declare variables first, followed by the exception handler
    DECLARE max_finished_at TIMESTAMP;
    -- Retrieve the latest 'finished_at' date with status 'SUCCESS'
    SELECT MAX(finished_at) INTO max_finished_at 
    FROM DimAuditForeigned 
    WHERE status = 'SUCCESS';

    -- Start the transaction after all declarations
    START TRANSACTION;

    -- Insert into Stg_BaoCaoNCC_Fact with a join on Stg_DimNCC and Stg_DimBenef
	INSERT INTO Stg_BaoCaoNCC_Fact (Id_NCC, Id_Benef, NCC_Code, fullName_NCC, gender, dateOfBirth, ethnic, identityCard_NCC, identityCard_Benef, nativePlace_NCC, decided, medicalCondition)
	SELECT n.Id_NCC, b.Id_Benef, n.NCC_Code, n.fullName_NCC, n.gender, n.dateOfBirth, n.ethnic, n.identityCard_NCC, b.identityCard_Benef, n.nativePlace_NCC, n.decided, n.medicalCondition
	FROM Stg_DimNCC n
	JOIN Stg_DimBenef b ON n.Id_NCC = b.Id_Benef
	WHERE (n.createdAt > max_finished_at OR b.createdAt > max_finished_at)
		AND NOT EXISTS (
			SELECT 1 FROM Stg_BaoCaoNCC_Fact f 
			WHERE f.Id_NCC = n.Id_NCC AND f.Id_Benef = b.Id_Benef
  );
    COMMIT;
END $$
DELIMITER ;





