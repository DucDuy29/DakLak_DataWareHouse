Start transaction;

-- Load source vào Stg_DimHoGiaDinh
INSERT INTO Stg_DimHoGiaDinh 
    (ID_Ho, HoTenChuHo, CCCD_ChuHo, GioiTinh, NgaySinhChuHo,
     Ten_Thon, Ten_Xa, Ten_Huyen, Ten_Tinh, LoaiKhuVuc, TenKhuVuc)
SELECT 
    a.ID_Ho, a.HoTenChuHo, a.CCCD_ChuHo, a.GioiTinh, a.NgaySinhChuHo,
    c.Ten_Thon, c.Ten_Xa, c.Ten_Huyen, c.Ten_Tinh, 
    b.LoaiKhuVuc, b.TenKhuVuc
FROM 
    db_hongheo_daknong.HoGiaDinh a
JOIN 
    db_hongheo_daknong.KhuVuc b ON a.ID_KhuVuc = b.ID_KhuVuc
JOIN 
    db_hongheo_daknong.DonViHanhChinh c ON a.ID_DonVi = c.ID_DonVi
WHERE 
    a.Create_Date >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS');
    
-- Load source vào Stg_DimRaSoat
INSERT INTO Stg_DimRaSoat
	(ID_Ho, NgayRaSoat, ID_PhieuA, ID_PhieuB1, DiemB1, ID_PhieuB2, DiemB2)
SELECT 
	d.ID_Ho, d.NgayRaSoat, e.ID_PhieuA, f.ID_PhieuB1, f.DiemB1, g.ID_PhieuB2, g.Diem_B2
FROM 
	db_hongheo_daknong.RaSoat d
JOIN 
	db_hongheo_daknong.PhieuA e ON d.NgayRaSoat = e.NgayRaSoat
JOIN 
	db_hongheo_daknong.PhieuB1 f ON d.NgayRaSoat = f.NgayRaSoat
JOIN 
	db_hongheo_daknong.PhieuB2 g ON d.NgayRaSoat = g.NgayRaSoat
WHERE
	e.Create_Date_A >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS')
OR 
	f.Create_Date_B1 >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS')
OR
	g.Create_Date_B2 >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS');

-- Load source vào Stg_HoGDRaSoat_Fact
INSERT INTO Stg_HoGDRaSoat_Fact
	(ID_HoGiaDinh, HoTenChuHo, CCCD_ChuHo , LoaiKhuVuc, TenKhuVuc, NgayRaSoat, Diem_B1, Diem_B2)
SELECT 
	h.ID_Ho, h.HoTenChuHo, h.CCCD_ChuHo, j.LoaiKhuVuc, j.TenKhuVuc, max(m.NgayRaSoat) as NgayRaSoat, k.DiemB1, l.Diem_B2
FROM 
	db_hongheo_daknong.HoGiaDinh h
JOIN 
	db_hongheo_daknong.KhuVuc j ON h.ID_KhuVuc = j.ID_KhuVuc
JOIN 
	db_hongheo_daknong.PhieuB1 k ON h.ID_Ho = k.ID_Ho
JOIN 
	db_hongheo_daknong.PhieuB2 l ON h.ID_Ho = l.ID_Ho
JOIN 
	db_hongheo_daknong.RaSoat m ON h.ID_Ho = m.ID_Ho
WHERE
	h.Create_Date >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS')
OR 
	k.Create_Date_B1 >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS')
OR 
	l.Create_Date_B2 >= (SELECT MAX(finished_at) FROM DimAuditForeigned WHERE status='SUCCESS')
GROUP BY 
	h.ID_Ho, h.HoTenChuHo, h.CCCD_ChuHo, j.LoaiKhuVuc, j.TenKhuVuc, k.DiemB1, l.Diem_B2;

Commit;

