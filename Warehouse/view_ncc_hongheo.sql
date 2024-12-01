use LDTBXH_Dwh;

CREATE OR REPLACE VIEW View_NCC_LoaiHo AS
SELECT 
    a.NCC_Code AS Mã_NCC,
    a.fullName_NCC AS Tên_NCC,
    a.dateOfBirth AS Ngày_Sinh,
    a.ethnic AS Dân_Tộc,
    CASE 
        WHEN b.KetQuaCuoiCung = 'Hộ nghèo' THEN 'Hộ nghèo'
        WHEN b.KetQuaCuoiCung = 'Hộ cận nghèo' THEN 'Hộ cận nghèo'
        ELSE 'Không thuộc hộ nghèo'
    END AS Loại_Hộ
FROM 
    BaoCaoNCC_Fact a
LEFT JOIN 
    HoGDRaSoat_Fact b ON a.identityCard_NCC = b.CCCD_ChuHo;



