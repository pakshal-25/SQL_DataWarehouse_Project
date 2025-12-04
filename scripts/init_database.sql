USE master

IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN   
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATELY
    DROP DATABASE DataWarehouse;
END;
GO
CREATE DATABASE DataWarehouse;

--CREATING THE 'DataWarehouse' database
USE DataWarehouse;

--CREATING SCHEMA
CREATE SCHEMA bronze;
go
CREATE SCHEMA silver;
go
CREATE SCHEMA gold;
go
