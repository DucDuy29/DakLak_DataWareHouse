use LDTBXH_Stg;

create table if not exists Stg_DimHoGiaDinh (
    ID_Ho int,
    HoTenChuHo varchar(50),
    CCCD_ChuHo varchar(50),
    GioiTinh varchar(50),
    NgaySinhChuHo date ,
    Ten_Thon varchar(50),
    Ten_Xa varchar(50),
    Ten_Huyen varchar(50),
    Ten_Tinh varchar(50),
    LoaiKhuVuc varchar(50),
    TenKhuVuc varchar(50),
    primary key (ID_Ho, CCCD_ChuHo)
);

create table if not exists Stg_DimRaSoat (
    ID_Ho int,
    NgayRaSoat date,
    ID_PhieuA int,
    ID_PhieuB1 int,
    DiemB1 int,
    ID_PhieuB2 int,
    DiemB2 int,
    primary key (ID_Ho, NgayRaSoat)
);

create table if not exists Stg_DimNCC (
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
    createdAt timestamp,
    primary key (Id_NCC)
);

create table if not exists Stg_DimBenef (
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
    createdAt timestamp,
    primary key (Id_Benef)
);

create table if not exists Stg_Date (
	date_key int,
    full_date Date,
    day_of_week int,
    day_num_in_month int,
    day_num_overall int,
    day_name varchar(255),
    day_abbrev varchar(255),
    weekday_flag varchar(255),
    week_num_in_year int,
    week_num_overall int,
    week_begin_date date,
    week_begin_date_key int,
    month int,
    month_num_overall int,
    month_name varchar(255),
    month_abbrev varchar(255),
    quarter int,
    year int,
    yearmo int,
    fiscal_month int,
    fiscal_quarter int,
    fiscal_year int,
    last_day_in_month_flag char(20),
    same_day_year_ago_date date,
    primary key (date_key)
);

create table if not exists Stg_HoGDRaSoat_Fact (
	ID_HoGiaDinh int,
    HoTenChuHo varchar(50),
    CCCD_ChuHo varchar(50),
    LoaiKhuVuc varchar(50),
    TenKhuVuc varchar(50),
    NgayRaSoat date,
    Diem_B1 int,
    Diem_B2 int,
    TongDiemB1B2 int,
    KetQuaCuoiCung varchar(50),
    primary key (ID_HoGiaDinh)
);

create table if not exists Stg_BaoCaoNCC_Fact (
	Id_NCC int,
    Id_Benef int,
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
    primary key (Id_NCC, Id_Benef, NCC_Code)
);

create table DimAuditForeigned (
    id int auto_increment,
    process_name VARCHAR(35),
    start_at TIMESTAMP,
    finished_at TIMESTAMP,
    information VARCHAR(255),
    status VARCHAR(10),
    primary key (id)
) engine=FEDERATED
connection='mysql://root:29122003@127.0.0.1:3306/LDTBXH_Dwh/DimAudit';

-- DROP TABLE DimAuditForeigned;
-- drop table Stg_DimRaSoat;
-- drop table Stg_DimNCC;
-- drop table Stg_DimBenef;
-- drop table Stg_BaoCaoNCC_Fact;
-- drop table Stg_DimHoGiaDinh;
drop table Stg_HoGDRaSoat_Fact;




