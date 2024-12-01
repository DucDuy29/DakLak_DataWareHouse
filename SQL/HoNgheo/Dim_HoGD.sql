SELECT a.ID_Ho, a.HoTenChuHo, a.CCCD_ChuHo, a.GioiTinh, a.NgaySinhChuHo, c.Ten_Huyen, c.Ten_Thon, c.Ten_Tinh, c.Ten_Xa, b.LoaiKhuVuc, b.TenKhuVuc
FROM db_hongheo_daknong.HoGiaDinh a
JOIN db_hongheo_daknong.KhuVuc b ON a.ID_KhuVuc = b.ID_KhuVuc
JOIN db_hongheo_daknong.DonViHanhChinh c ON a.ID_DonVi = c.ID_DonVi