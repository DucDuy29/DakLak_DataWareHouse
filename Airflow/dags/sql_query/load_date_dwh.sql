use LDTBXH_Dwh;

insert into DimDate (Date_Key, Full_Date, Day_Of_Week, Month, Month_Num_Overall, Month_Name, Quarter, Year, Fiscal_Month, Fiscal_Quarter, Fiscal_Year)
SELECT DISTINCT
    a.date_key AS Date_Key,
    a.full_date AS Full_Date,
    a.day_of_week AS Day_Of_Week,
    a.month AS Month,
    a.month_num_overall AS Month_Num_Overall,
    a.month_name AS Month_Name,
    a.quarter AS Quarter,
    a.year AS Year,
    a.fiscal_month AS Fiscal_Month,
    a.fiscal_quarter AS Fiscal_Quarter,
    a.fiscal_year AS Fiscal_Year
FROM LDTBXH_Stg.Stg_Date a
WHERE NOT EXISTS (SELECT 1 FROM DimDate)
ORDER BY year, month;