-------------------------------------------------------------------------------------------------------------------
-------------------------------------------- Automating Excel to SQL ----------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- Step 1: Create the table

IF OBJECT_ID('Raw_Data_GDP') IS NOT NULL DROP TABLE Raw_Data_GDP

CREATE TABLE Raw_Data_GDP
(DEMO_IND NVARCHAR(200),
Indicator NVARCHAR(200),
[LOCATION] NVARCHAR(200),
Country NVARCHAR(200),
[TIME] NVARCHAR(200),
[Value] FLOAT,
[Flag Codes] NVARCHAR(200),
Flags NVARCHAR(200)
)

--SELECT * FROM Raw_Data_GDP

-- Step 2: Import the Data

BULK INSERT Raw_Data_GDP
FROM 'C:\Users\pitsi\Desktop\Python Tutorials\How to automate SQL to Excel\gdp_raw_data.csv'
WITH ( FORMAT='CSV');

--SELECT * FROM Raw_Data_GDP

-------------------------------------------------------------------------------------------------------------------


SELECT * FROM [dbo].[GDP_Forecast_Output] WHERE ForecastValue <> 0