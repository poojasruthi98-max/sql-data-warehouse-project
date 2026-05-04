/* 
===========================================================
Create Database and Schemas
===========================================================
Script Purpose:
 This script creates a new database named ' DataWarwhouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
within the databse: 'bronze, 'silver', and 'gold.

WARNING:
Running this script will drop the entire 'Datawarehouse' database if it exists.
All data in the database will be permanently deleted. Proceed with caution
and ensure you have proper backups before running this script.
*/

USE master;
GO 

-- Drop and recreate the 'DataWarehouse' database
IF DB_ID('DataWarehouse') IS NOT NULL
BEGIN
    ALTER DATABASE DataWarehouse 
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Switch context
USE DataWarehouse;
GO 

-- Create Schemas

CREATE SCHEMA bronze;
GO 
CREATE SCHEMA silver;
GO 
CREATE SCHEMA gold;
GO 

--Verify
SELECT name
FROM sys.databases;
