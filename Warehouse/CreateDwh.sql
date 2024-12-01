use LDTBXH_Dwh;

create table if not exists DimHoGiaDinh (
	HoGD_Key int auto_increment,
    ID_Ho int,
    HoTenChuHo varchar(50),
    CCCD_ChuHo varchar(50),
    GioiTinh varchar(50),
    NgaySinhChuHo date,
    Ten_Thon varchar(50),
    Ten_Xa varchar(50),
    Ten_Huyen varchar(50),
    Ten_Tinh varchar(50),
    LoaiKhuVuc varchar(50),
    TenKhuVuc varchar(50),
    RowIsCurrent boolean not null,
    RowStartDate timestamp not null,
    RowEndDate timestamp,
    primary key (HoGD_Key)
);

create table if not exists DimRaSoat (
	RaSoat_Key int auto_increment,
    ID_Ho int,
    NgayRaSoat date,
	ID_PhieuA int,
    ID_PhieuB1 int,
    DiemB1 int,
    ID_PhieuB2 int,
    DiemB2 int,
    RowIsCurrent boolean not null,
    RowStartDate timestamp not null,
    RowEndDate timestamp,
    primary key (RaSoat_Key)
);

create table if not exists DimDate (
	Date_Key int,
    Full_Date date,
    Day_Of_Week int,
    Month int,
    Month_Num_Overall int,
    Month_Name varchar(50),
    Quarter int,
    Year int,
    Fiscal_Month int,
    Fiscal_Quarter int,
    Fiscal_Year int,
    primary key (Date_Key)
);

create table if not exists HoGDRaSoat_Fact (
	HoGD_Key int auto_increment,
    RaSoat_Key int,
    Date_Key int,
    ID_HoGiaDinh int,
    HoTenChuHo varchar(255),
    CCCD_ChuHo varchar(50),
    LoaiKhuVuc varchar(50),
    TenKhuVuc varchar(50),
    NgayRaSoat date,
    Diem_B1 int,
    Diem_B2 int,
    TongDiemB1B2 int,
    KetQuaCuoiCung varchar(50),
    primary key (HoGD_Key, RaSoat_Key, Date_Key),
    constraint FK_HoGD_HoGDRaSoatFact FOREIGN KEY(HoGD_Key) REFERENCES DimHoGiaDinh(HoGD_Key),
    constraint FK_RaSoat_HoGDRaSoatFact FOREIGN KEY(RaSoat_Key) REFERENCES DimRaSoat(RaSoat_Key),
    constraint FK_Date_HoGDRaSoatFact FOREIGN KEY(Date_Key) REFERENCES DimDate(Date_Key)
);

create table if not exists DimNCC (
	NCC_Key int auto_increment,
    Id_NCC int,
    NCC_Code varchar(50),
    fullName_NCC varchar(50),
    gender varchar(50),
    dateOfBirth varchar(50),
    ethnic varchar(50),
    identityCard_NCC varchar(50),
    nativePlace_NCC varchar(255),
    province_NCC varchar(255),
    district_NCC varchar(255),
    ward_NCC varchar(255),
    decided varchar(50),
    medicalcondition varchar(255),
    RowIsCurrent boolean not null,
    RowStartDate timestamp not null,
    RowEndDate timestamp,
    primary key (NCC_Key)
);

create table if not exists DimBenef (
	Benef_Key int auto_increment,
    Id_Benef int,
    fullName_Benef varchar(50),
    dateOfBirth varchar(50),
    gender varchar(50),
    identityCard_Benef varchar(50),
    nativePlace_Benef varchar(255),
    relation_NCC varchar(50),
    beneficiary varchar(50),
    IsNCC varchar(50),
    status boolean,
    province_NCC varchar(255),
    district_NCC varchar(255),
    ward_NCC varchar(255),
	RowIsCurrent boolean not null,
    RowStartDate timestamp not null,
    RowEndDate timestamp,
    primary key (Benef_Key)
);

create table if not exists BaoCaoNCC_Fact (
	NCC_Key int auto_increment,
    Benef_Key int,
    NCC_Code varchar(50),
    fullName_NCC varchar(50),
    gender varchar(50),
    dateOfBirth varchar(50),
    ethnic varchar(50),
    identityCard_NCC varchar(50),
    identityCard_Benef varchar(50),
    nativePlace_NCC varchar(255),
    decided varchar(50),
    medicalCondition varchar(255),
    primary key (NCC_Key, Benef_Key),
    constraint FK_NCC_BaoCaoNCC_Fact FOREIGN KEY(NCC_Key) REFERENCES DimNCC(NCC_Key),
    constraint FK_Benef_BaoCaoNCC_Fact FOREIGN KEY(Benef_Key) REFERENCES DimBenef(Benef_Key)
);

CREATE TABLE IF NOT EXISTS DimAudit
(
    id int auto_increment,
	process_name VARCHAR(35),
	start_at TIMESTAMP,
	finished_at TIMESTAMP,
	information VARCHAR(255),
	status VARCHAR(10) CHECK (status IN('SUCCESS', 'ERROR', 'PENDING')),
    PRIMARY KEY(id)
);
-- DROP TABLE DimAudit;
-- INSERT INTO DimAudit(process_name, start_at, finished_at, status) VALUES
-- ('data integration', NULL, '1999-12-31 00:00:00', 'SUCCESS');

drop table HoGDRaSoat_Fact;

