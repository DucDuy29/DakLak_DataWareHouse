use LDTBXH_Dwh;
SET SQL_SAFE_UPDATES = 0;

start transaction;

-- Update DimHoGiaDinh với SCD Type 2
UPDATE DimHoGiaDinh
SET 
    RowIsCurrent = FALSE,
    RowEndDate = current_timestamp
WHERE 
    EXISTS (
        SELECT 1
        FROM LDTBXH_Stg.Stg_DimHoGiaDinh a
        WHERE 
            a.CCCD_ChuHo = DimHoGiaDinh.CCCD_ChuHo 
            AND DimHoGiaDinh.RowEndDate IS NULL
            AND (
                a.Ten_Thon <> DimHoGiaDinh.Ten_Thon OR
                a.Ten_Xa <> DimHoGiaDinh.Ten_Xa OR
                a.Ten_Huyen <> DimHoGiaDinh.Ten_Huyen OR
                a.Ten_Tinh <> DimHoGiaDinh.Ten_Tinh OR
                a.TenKhuVuc <> DimHoGiaDinh.TenKhuVuc
				)
    );
    
-- Insert Data vào DimHoGiaDinh nếu như chưa có dữ liệu Hộ GD đó
insert into DimHoGiaDinh
    (ID_Ho, HoTenChuHo, CCCD_ChuHo, GioiTinh, NgaySinhChuHo, 
    Ten_Thon, Ten_Xa, Ten_Huyen, Ten_Tinh, LoaiKhuVuc, TenKhuVuc, RowIsCurrent, RowStartDate, RowEndDate)
select 
    a.ID_Ho, a.HoTenChuHo, a.CCCD_ChuHo, a.GioiTinh, a.NgaySinhChuHo, 
    a.Ten_Thon, a.Ten_Xa, a.Ten_Huyen, a.Ten_Tinh, a.LoaiKhuVuc, a.TenKhuVuc,
    TRUE as RowIsCurrent, current_timestamp as RowStartDate, NULL as RowEndDate
from 
    LDTBXH_Stg.Stg_DimHoGiaDinh a
where
    (a.ID_Ho, a.CCCD_ChuHo, a.Ten_Thon, a.Ten_Xa, a.Ten_Huyen, a.Ten_Tinh, a.TenKhuVuc) not in 
    (select ID_Ho, CCCD_ChuHo, Ten_Thon, Ten_Xa, Ten_Huyen, Ten_Tinh, TenKhuVuc from DimHoGiaDinh);

-- Update DimRaSoat với SCD Type 2
UPDATE DimRaSoat
SET 
    RowIsCurrent = FALSE,
    RowEndDate = current_timestamp
WHERE 
    EXISTS (
        SELECT 1
        FROM LDTBXH_Stg.Stg_DimRaSoat b
        WHERE 
            b.ID_Ho = DimRaSoat.ID_Ho 
            AND (
				b.NgayRaSoat <> DimRaSoat.NgayRaSoat OR
                b.DiemB1 <> DimRaSoat.DiemB1 OR
                b.DiemB2 <> DimRaSoat.DiemB2
                )
            AND DimRaSoat.RowEndDate IS NULL
    );
    
-- Insert Data vào DimRaSoat nếu như chưa có dữ liệu đó
insert into DimRaSoat
    (ID_Ho, NgayRaSoat, ID_PhieuA, ID_PhieuB1, DiemB1, ID_PhieuB2, DiemB2, RowIsCurrent, RowStartDate, RowEndDate)
select 
	b.ID_Ho, b.NgayRaSoat, b.ID_PhieuA, b.ID_PhieuB1, b.DiemB1, b.ID_PhieuB2, b.DiemB2,
    TRUE as RowIsCurrent, current_timestamp as RowStartDate, NULL as RowEndDate
from 
    LDTBXH_Stg.Stg_DimRaSoat b
where
    (b.ID_Ho, b.NgayRaSoat, b.ID_PhieuA, b.ID_PhieuB1, b.ID_PhieuB2) not in 
    (select ID_Ho, NgayRaSoat, ID_PhieuA, ID_PhieuB1, ID_PhieuB2 from DimRaSoat);

-- Load Stg_HoGDRaSoat_Fact vào HoGDRaSoat_Fact
INSERT INTO HoGDRaSoat_Fact
    (HoGD_Key, RaSoat_Key, Date_Key, ID_HoGiaDinh, HoTenChuHo, CCCD_ChuHo, LoaiKhuVuc, 
     TenKhuVuc, NgayRaSoat, Diem_B1, Diem_B2, TongDiemB1B2, KetQuaCuoiCung)
SELECT
    (SELECT HoGD_Key FROM DimHoGiaDinh WHERE ID_HoGiaDinh = src.ID_HoGiaDinh LIMIT 1) AS HoGD_Key,
    (SELECT RaSoat_Key FROM DimRaSoat WHERE NgayRaSoat = src.NgayRaSoat LIMIT 1) AS RaSoat_Key,
    (YEAR(NgayRaSoat) * 10000 + MONTH(NgayRaSoat) * 100 + DAY(NgayRaSoat)) AS Date_Key,
    src.ID_HoGiaDinh, src.HoTenChuHo, src.CCCD_ChuHo, src.LoaiKhuVuc, src.TenKhuVuc, 
    src.NgayRaSoat, src.Diem_B1, src.Diem_B2, src.TongDiemB1B2, src.KetQuaCuoiCung
FROM
    LDTBXH_Stg.Stg_HoGDRaSoat_Fact src
WHERE
    (SELECT HoGD_Key FROM DimHoGiaDinh WHERE ID_HoGiaDinh = src.ID_HoGiaDinh LIMIT 1) IS NOT NULL
    AND (SELECT RaSoat_Key FROM DimRaSoat WHERE NgayRaSoat = src.NgayRaSoat LIMIT 1) IS NOT NULL
    AND (YEAR(NgayRaSoat) * 10000 + MONTH(NgayRaSoat) * 100 + DAY(NgayRaSoat)) NOT IN 
        (SELECT Date_Key FROM HoGDRaSoat_Fact
         WHERE HoGD_Key = (SELECT HoGD_Key FROM DimHoGiaDinh WHERE ID_HoGiaDinh = src.ID_HoGiaDinh LIMIT 1)
         AND RaSoat_Key = (SELECT RaSoat_Key FROM DimRaSoat WHERE NgayRaSoat = src.NgayRaSoat LIMIT 1));
commit;


