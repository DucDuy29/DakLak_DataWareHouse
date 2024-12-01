SELECT h.ID_Ho, h.HoTenChuHo, j.LoaiKhuVuc, j.TenKhuVuc, max(m.NgayRaSoat) as NgayRaSoat, k.DiemB1, l.Diem_B2
FROM HoGiaDinh h
JOIN KhuVuc j ON h.ID_KhuVuc = j.ID_KhuVuc
JOIN PhieuB1 k ON h.ID_Ho = k.ID_Ho
JOIN PhieuB2 l ON h.ID_Ho = l.ID_Ho
JOIN RaSoat m ON h.ID_Ho = m.ID_Ho
GROUP BY h.ID_Ho, h.HoTenChuHo, j.LoaiKhuVuc, j.TenKhuVuc, k.DiemB1, l.Diem_B2;