SELECT d.ID_Ho, d.NgayRaSoat, e.ID_PhieuA, f.ID_PhieuB1, f.DiemB1, g.ID_PhieuB2, g.Diem_B2
FROM db_hongheo_daknong.RaSoat d
JOIN db_hongheo_daknong.PhieuA e ON d.NgayRaSoat = e.NgayRaSoat
JOIN db_hongheo_daknong.PhieuB1 f ON d.NgayRaSoat = f.NgayRaSoat
JOIN db_hongheo_daknong.PhieuB2 g ON d.NgayRaSoat = g.NgayRaSoat