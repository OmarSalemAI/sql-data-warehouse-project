/*
==================================================================
Build Database and Schema for Each Layer: Bronze, Silver, and Gold
==================================================================

Purpose:
---------
- Create a database called `data_warehouse`.
- First, check if a database with the same name exists.
- If it exists, delete it (after ensuring no active connections).
- Then, create the `data_warehouse` database.
- Create schemas for each layer: `bronze`, `silver`, and `gold`.

Caution:
---------
- Be careful when running this script.
- It will **delete** the existing `data_warehouse` database if found.
- Always take a backup before running this script.
*/

-- Step 1: Check if the database exists and drop it if necessary
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'data_warehouse')
BEGIN
    ALTER DATABASE data_warehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE data_warehouse;
END
GO

-- Step 2: Create the new database
CREATE DATABASE data_warehouse;
GO

-- Step 3: Switch to the newly created database
USE data_warehouse;
GO

-- Step 4: Create schemas for each layer
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

