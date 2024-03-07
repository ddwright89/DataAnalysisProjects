/*
ETL Process Design in SQL 
Skills used : CREATE, UPDATE, SELECT, MERGE, ALTER, INSERT, UNION
*/

USE [master]
GO

IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'northwind_dw')
    DROP DATABASE [northwind_dw]
GO

/****** Object:  Create Database [northwind_dw] - data warehouse ******/
USE [master]
GO

CREATE DATABASE [northwind_dw]
GO

/****** Change the compatibility level to support MSSQL server version above 2008  ******/
ALTER DATABASE [northwind_dw] SET COMPATIBILITY_LEVEL = 100
GO

/****** Enable the full text search ability ******/
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [northwind_dw].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO

/****** System configs ******/
ALTER DATABASE [northwind_dw] SET ANSI_NULL_DEFAULT OFF
GO

ALTER DATABASE [northwind_dw] SET ANSI_NULLS OFF
GO

ALTER DATABASE [northwind_dw] SET ANSI_PADDING OFF
GO

ALTER DATABASE [northwind_dw] SET ANSI_WARNINGS OFF
GO

ALTER DATABASE [northwind_dw] SET ARITHABORT OFF
GO

ALTER DATABASE [northwind_dw] SET AUTO_CLOSE OFF
GO

ALTER DATABASE [northwind_dw] SET AUTO_CREATE_STATISTICS ON
GO

ALTER DATABASE [northwind_dw] SET AUTO_SHRINK OFF
GO

ALTER DATABASE [northwind_dw] SET AUTO_UPDATE_STATISTICS ON
GO

ALTER DATABASE [northwind_dw] SET CURSOR_CLOSE_ON_COMMIT OFF
GO

ALTER DATABASE [northwind_dw] SET CURSOR_DEFAULT  GLOBAL
GO

ALTER DATABASE [northwind_dw] SET CONCAT_NULL_YIELDS_NULL OFF
GO

ALTER DATABASE [northwind_dw] SET NUMERIC_ROUNDABORT OFF
GO

ALTER DATABASE [northwind_dw] SET QUOTED_IDENTIFIER OFF
GO

ALTER DATABASE [northwind_dw] SET RECURSIVE_TRIGGERS OFF
GO

ALTER DATABASE [northwind_dw] SET  ENABLE_BROKER
GO

ALTER DATABASE [northwind_dw] SET AUTO_UPDATE_STATISTICS_ASYNC OFF
GO

ALTER DATABASE [northwind_dw] SET DATE_CORRELATION_OPTIMIZATION OFF
GO

ALTER DATABASE [northwind_dw] SET TRUSTWORTHY OFF
GO

ALTER DATABASE [northwind_dw] SET ALLOW_SNAPSHOT_ISOLATION OFF
GO

ALTER DATABASE [northwind_dw] SET PARAMETERIZATION SIMPLE
GO

ALTER DATABASE [northwind_dw] SET READ_COMMITTED_SNAPSHOT OFF
GO

ALTER DATABASE [northwind_dw] SET HONOR_BROKER_PRIORITY OFF
GO

ALTER DATABASE [northwind_dw] SET  READ_WRITE
GO

ALTER DATABASE [northwind_dw] SET RECOVERY FULL
GO

ALTER DATABASE [northwind_dw] SET  MULTI_USER
GO

ALTER DATABASE [northwind_dw] SET PAGE_VERIFY CHECKSUM
GO

ALTER DATABASE [northwind_dw] SET DB_CHAINING OFF
GO

/****** Object:  Create Database [northwind_dw] - data warehouse ******/

USE [northwind_dw]
GO

IF  EXISTS (SELECT name FROM sys.tables WHERE name = N'dimTime')
    DROP TABLE [dimTime]
GO

/****** Object:  Create the dimTime dimension Table ******/
CREATE TABLE [dimTime]
(
    [TimeKey] INT primary key,
    [Date] DATETIME,
    [Day] VARCHAR(9), -- Contains name of the day, Sunday, Monday
    [DayOfWeek] CHAR(1),-- First Day Sunday=1 and Saturday=7
    [DayOfMonth] VARCHAR(2), -- Field will hold day number of Month
    [DayOfYear] VARCHAR(3),
    [WeekOfYear] VARCHAR(2),--Week Number of the Year
    [Month] VARCHAR(9),--January, February etc
    [MonthOfYear] VARCHAR(2), --Number of the Month 1 to 12
    [QuarterOfYear] VARCHAR(9),--First,Second..
    [Year] CHAR(4),-- Year value of Date stored in Row
)
GO
--=========================================================================================
--Specify Start Date and End date here
--Value of Start Date Must be Less than Your End Date
--=========================================================================================

DECLARE @StartDate DATETIME = '12/29/1970' --Starting value of Date Range
DECLARE @EndDate DATETIME = '01/01/2050' --End Value of Date Range

--Temporary Variables To Hold the Values During Processing of Each Date of Year
DECLARE
    @DayOfWeekInMonth INT,
    @DayOfWeekInYear INT,
    @DayOfQuarter INT,
    @WeekOfMonth INT,
    @CurrentYear INT,
    @CurrentMonth INT,
    @CurrentQuarter INT

/*Table Data type to store the day of week count for the month and year*/
DECLARE @DayOfWeek TABLE
(
    DOW INT,
    MonthCount INT,
    QuarterCount INT,
    YearCount INT
)

INSERT INTO @DayOfWeek VALUES (1, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (2, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (3, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (4, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (5, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (6, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (7, 0, 0, 0)

--Extract and assign various parts of Values from Current Date to Variable

DECLARE @CurrentDate AS DATETIME = @StartDate
SET @CurrentMonth = DATEPART(MM, @CurrentDate)
SET @CurrentYear = DATEPART(YY, @CurrentDate)
SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)

/********************************************************************************************/
--Proceed only if Start Date(Current date) is less than End date you specified above

WHILE @CurrentDate < @EndDate
/*Begin day of week logic*/
BEGIN
    /*Check for Change in Month of the Current date if Month changed then
    Change variable value*/
    IF @CurrentMonth != DATEPART(MM, @CurrentDate)
    BEGIN
        UPDATE @DayOfWeek
        SET [MonthCount] = 0
        SET @CurrentMonth = DATEPART(MM, @CurrentDate)
    END

    /* Check for Change in Quarter of the Current date if Quarter changed then change
        Variable value*/
    IF @CurrentQuarter != DATEPART(QQ, @CurrentDate)
    BEGIN
        UPDATE @DayOfWeek
        SET [QuarterCount] = 0
        SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)
    END

    /* Check for Change in Year of the Current date if Year changed then change
        Variable value*/
    IF @CurrentYear != DATEPART(YY, @CurrentDate)
    BEGIN
        UPDATE @DayOfWeek
        SET YearCount = 0
        SET @CurrentYear = DATEPART(YY, @CurrentDate)
    END

    -- Set values in table data type created above from variables
    UPDATE @DayOfWeek
    SET
        MonthCount = MonthCount + 1,
        QuarterCount = QuarterCount + 1,
        YearCount = YearCount + 1
    WHERE DOW = DATEPART(DW, @CurrentDate)

    SELECT
        @DayOfWeekInMonth = MonthCount,
        @DayOfQuarter = QuarterCount,
        @DayOfWeekInYear = YearCount
    FROM @DayOfWeek
    WHERE DOW = DATEPART(DW, @CurrentDate)

/*End day of week logic*/

/* Populate Your Dimension Table with values*/

    INSERT INTO [dimTime]
    SELECT

        CONVERT (char(8),@CurrentDate,112) as 'TimeKey',
        @CurrentDate AS 'Date',
        DATENAME(DW, @CurrentDate) AS 'Day',
        DATEPART(DW, @CurrentDate) AS 'DayOfWeek',
        DATEPART(DD, @CurrentDate) AS 'DayOfMonth',
        DATEPART(DY, @CurrentDate) AS 'DayOfYear',
        DATEPART(WW, @CurrentDate) AS 'WeekOfYear',
        DATENAME(MM, @CurrentDate) AS 'Month',
        DATEPART(MM, @CurrentDate) AS 'MonthOfYear',
        CASE DATEPART(QQ, @CurrentDate)
            WHEN 1 THEN 'First'
            WHEN 2 THEN 'Second'
            WHEN 3 THEN 'Third'
            WHEN 4 THEN 'Fourth'
        END AS 'QuarterOfYear',
        DATEPART(YEAR, @CurrentDate) AS 'Year'

    SET @CurrentDate = DATEADD(DD, 1, @CurrentDate)
END

-- Step 1: create dimensional tables and fact tables

-- step 1.1: create dimProducts:
CREATE TABLE [dbo].[dimProducts](
    [ProductKey] [int] IDENTITY(1, 1) PRIMARY KEY, 
    [ProductID] [int] NOT NULL,
    [ProductName] [nvarchar](40) NOT NULL,
    [QuantityPerUnit] [nvarchar](20) NULL,
    [UnitPrice] [money] NULL,
    [UnitsInStock] [smallint] NULL,
    [UnitsOnOrder] [smallint] NULL,
    [ReorderLevel] [smallint] NULL,
    [Discontinued] [bit] NOT NULL,
    [CategoryName] [nvarchar](15) NOT NULL,
    [Description] [ntext] NULL,
    [Picture] [image] NULL
)
GO

--step 1.2: create dimCustomers
CREATE TABLE [dimCustomers]
(
    [CustomerKey] INT IDENTITY(1,1) primary key,
    [CustomerID] [nchar](5) NOT NULL,
    [CompanyName] [nvarchar](40) NOT NULL,
    [ContactName] [nvarchar](30) NULL,
    [ContactTitle] [nvarchar](30) NULL,
    [Address] [nvarchar](60) NULL,
    [City] [nvarchar](15) NULL,
    [Region] [nvarchar](15) NULL,
    [PostalCode] [nvarchar](10) NULL,
    [Country] [nvarchar](15) NULL,
    [Phone] [nvarchar](24) NULL,
    [Fax] [nvarchar](24) NULL
    
)
GO

--step 1.3: create dimSuppliers
CREATE TABLE [dimSuppliers]
(
    [SupplierKey] INT IDENTITY(1,1) primary key,
    [SupplierID] [nchar](5) NOT NULL,
    [CompanyName] [nvarchar](40) NOT NULL,
    [ContactName] [nvarchar](30) NULL,
    [ContactTitle] [nvarchar](30) NULL,
    [Address] [nvarchar](60) NULL,
    [City] [nvarchar](15) NULL,
    [Region] [nvarchar](15) NULL,
    [PostalCode] [nvarchar](10) NULL,
    [Country] [nvarchar](15) NULL,
    [Phone] [nvarchar](24) NULL,
    [Fax] [nvarchar](24) NULL,
    [Homepage] [ntext] NULL
)
GO

--step 1.4: create factOrders
CREATE TABLE [factOrders]
(
    [ProductKey] INT FOREIGN KEY REFERENCES dimProducts(ProductKey),
    [CustomerKey] INT FOREIGN KEY REFERENCES dimCustomers(CustomerKey),
    [SupplierKey] INT FOREIGN KEY REFERENCES dimSuppliers(SupplierKey),
    [OrderDateKey] INT FOREIGN KEY REFERENCES dimTime(TimeKey), 
    [RequiredDateKey] INT FOREIGN KEY REFERENCES dimTime(TimeKey), 
    [ShippedDateKey] INT FOREIGN KEY REFERENCES dimTime(TimeKey), 
    [OrderID] [int] NOT NULL,
    [UnitPrice] [money] NOT NULL,
    [Quantity] [smallint] NOT NULL,
    [Discount] [real] NOT NULL,
    [TotalPrice] [money] NOT NULL, -- this is a derived (calculated) field from [UnitPrice] * [Qty] * (1 - [Discount])
    [ShipperCompany] [nvarchar] (40) NOT NULL,
    [ShipperPhone] [nvarchar] (40) NOT NULL,
    CONSTRAINT [pk_factOrders] PRIMARY KEY ([ProductKey], [CustomerKey], [SupplierKey], [OrderDateKey])
)
GO

-- Step 2: Populating the dimensions and facts:
-- Step 2.1: populating dimProducts:

-- Populating dimProducts from northwind3
MERGE INTO dimProducts dp USING
(
    SELECT 
        ProductID, 
        ProductName,
        QuantityPerUnit,
        UnitPrice, 
        UnitsInStock,
        UnitsOnOrder,
        ReorderLevel,
        Discontinued,
        CategoryName,
        Description,
        Picture
    FROM 
        Northwind3.dbo.Products p1, northwind3.dbo.Categories c1 
    WHERE p1.CategoryID=c1.CategoryID
) pc ON (dp.ProductID = pc.ProductID)-- Assume ProductID is unique
WHEN MATCHED THEN -- if ProductID matched, do nothing 
    UPDATE SET dp.ProductName = pc.ProductName -- Dummy update
WHEN NOT MATCHED THEN -- Otherwise, insert a new product 
    INSERT(ProductID, ProductName, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, 
    ReorderLevel, Discontinued, CategoryName, Description, Picture)
    VALUES(pc.ProductID, pc.ProductName, pc.QuantityPerUnit, pc.UnitPrice, pc.UnitsInStock, pc.UnitsOnOrder, pc.ReorderLevel,
    pc.Discontinued, pc.CategoryName, pc.Description, pc.Picture);

    MERGE INTO dimProducts dp USING
(
    SELECT 
        ProductID, 
        ProductName,
        QuantityPerUnit,
        UnitPrice, 
        UnitsInStock,
        UnitsOnOrder,
        ReorderLevel,
        Discontinued,
        CategoryName,
        Description,
        Picture
    FROM 
        Northwind4.dbo.Products p2, northwind4.dbo.Categories c2
    WHERE p2.CategoryID=c2.CategoryID
) pc ON (dp.ProductID = pc.ProductID)-- Assume ProductID is unique
WHEN MATCHED THEN -- if ProductID matched, do nothing 
    UPDATE SET dp.ProductName = pc.ProductName -- Dummy update
WHEN NOT MATCHED THEN -- Otherwise, insert a new product 
    INSERT(ProductID, ProductName, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, 
    ReorderLevel, Discontinued, CategoryName, Description, Picture)
    VALUES(pc.ProductID, pc.ProductName, pc.QuantityPerUnit, pc.UnitPrice, pc.UnitsInStock, pc.UnitsOnOrder, pc.ReorderLevel,
    pc.Discontinued, pc.CategoryName, pc.Description, pc.Picture);

-- Step 2.2 Validation
Select 'OLTP' as [SourceType], 'northwind3.Products' as [TableName], count(*) as [RowCounts] from northwind3.dbo.Products
Union
Select 'OLTP' as [SourceType], 'northwind4.Products' as [TableName], count(*) as [RowCounts] from northwind4.dbo.Products
Union
Select 'OLAP' as [SourceType], 'northwind_dw.dimProducts' as [TableName], count(*) as [RowCounts] from northwind_dw.dbo.dimProducts;

-- Step 2.3 populating dimCustomers
MERGE INTO dimCustomers dc
USING
(
    SELECT 
        CustomerID, 
        CompanyName, 
        ContactName, 
        ContactTitle, 
        Address,
        City, 
        Region, 
        PostalCode, 
        Country, 
        Phone, 
        Fax
    FROM northwind3.dbo.Customers
) c ON (dc.CustomerID = c.CustomerID) -- Assume CustomerID is unique
    WHEN MATCHED THEN -- if CustomerID matched, do nothing
    UPDATE SET dc.CompanyName = c.CompanyName -- Dummy update
    WHEN NOT MATCHED THEN -- Otherwise, insert a new customer
    INSERT(CustomerID, CompanyName, ContactName, ContactTitle, Address,
    City, Region, PostalCode, Country, Phone, Fax)
    VALUES(c.CustomerID, c.CompanyName, c.ContactName, c.ContactTitle,
    c.Address, c.City, C.Region, c.PostalCode, c.Country, c.Phone,
    c.Fax);

    MERGE INTO dimCustomers dc
USING
(
    SELECT 
        CustomerID, 
        CompanyName, 
        ContactName, 
        ContactTitle, 
        Address,
        City, 
        Region, 
        PostalCode, 
        Country, 
        Phone, 
        Fax
    FROM northwind4.dbo.Customers
) c ON (dc.CustomerID = c.CustomerID) -- Assume CustomerID is unique
    WHEN MATCHED THEN -- if CustomerID matched, do nothing
    UPDATE SET dc.CompanyName = c.CompanyName -- Dummy update
    WHEN NOT MATCHED THEN -- Otherwise, insert a new customer
    INSERT(CustomerID, CompanyName, ContactName, ContactTitle, Address,
    City, Region, PostalCode, Country, Phone, Fax)
    VALUES(c.CustomerID, c.CompanyName, c.ContactName, c.ContactTitle,
    c.Address, c.City, C.Region, c.PostalCode, c.Country, c.Phone,
    c.Fax);

-- step 2.4: Validation for dimCustomer
Select 'OLTP' as [SourceType], 'northwind3.Customers' as [TableName], count(*) as [RowCounts] from northwind3.dbo.Customers
Union
Select 'OLTP' as [SourceType], 'northwind4.Customers' as [TableName], count(*) as [RowCounts] from northwind4.dbo.Customers
Union
Select 'OLAP' as [SourceType], 'northwind_dw.dimCustomers' as [TableName], count(*) as [RowCounts] from northwind_dw.dbo.dimCustomers;

-- Step 2.4 populating dimSuppliers
MERGE INTO dimSuppliers ds
USING
(
    SELECT 
        SupplierID, 
        CompanyName, 
        ContactName, 
        ContactTitle, 
        Address,
        City, 
        Region, 
        PostalCode, 
        Country, 
        Phone, 
        Fax,
        Homepage
    FROM northwind3.dbo.Suppliers
) s ON (ds.SupplierID = s.SupplierID) -- Assume SupplierID is unique
    WHEN MATCHED THEN -- if SupplierID matched, do nothing
    UPDATE SET ds.CompanyName = s.CompanyName -- Dummy update
    WHEN NOT MATCHED THEN -- Otherwise, insert a new customer
    INSERT(SupplierID, CompanyName, ContactName, ContactTitle, Address,
    City, Region, PostalCode, Country, Phone, Fax, Homepage)
    VALUES(s.SupplierID, s.CompanyName, s.ContactName, s.ContactTitle,
    s.Address, s.City, s.Region, s.PostalCode, s.Country, s.Phone,
    s.Fax, s.Homepage);

    MERGE INTO dimSuppliers ds
USING
(
    SELECT 
        SupplierID, 
        CompanyName, 
        ContactName, 
        ContactTitle, 
        Address,
        City, 
        Region, 
        PostalCode, 
        Country, 
        Phone, 
        Fax,
        Homepage
    FROM northwind4.dbo.Suppliers
) s ON (ds.SupplierID = s.SupplierID) -- Assume SupplierID is unique
    WHEN MATCHED THEN -- if SupplierID matched, do nothing
    UPDATE SET ds.CompanyName = s.CompanyName -- Dummy update
    WHEN NOT MATCHED THEN -- Otherwise, insert a new customer
    INSERT(SupplierID, CompanyName, ContactName, ContactTitle, Address,
    City, Region, PostalCode, Country, Phone, Fax, Homepage)
    VALUES(s.SupplierID, s.CompanyName, s.ContactName, s.ContactTitle,
    s.Address, s.City, s.Region, s.PostalCode, s.Country, s.Phone,
    s.Fax, s.Homepage);

-- step 2.4: Validation for dimCustomer
select 'OLTP' as [SourceType], 'northwind3.Suppliers' as [TableName], count(*) as [RowCounts] from northwind3.dbo.Suppliers
Union
select 'OLTP' as [SourceType], 'northwind4.Suppliers' as [TableName], count(*) as [RowCounts] from northwind4.dbo.Suppliers
Union
select 'OLAP' as [SourceType], 'northwind_dw.dimSuppliers' as [TableName], count(*) as [RowCounts] from northwind_dw.dbo.dimSuppliers;

-- step 2.5. Populating the factOrders
MERGE INTO factOrders fo
USING
    (
        SELECT ProductKey,
               CustomerKey,
               SupplierKey,
               dt1.TimeKey                as [OrderDatekey],    -- from dimTime
               dt2.TimeKey                as [RequiredDatekey], -- from dimTime
               o.OrderID                  as [OrderID],
               od.UnitPrice               as [UnitPrice],
               Quantity                   as [Qty],
               Discount,
               od.UnitPrice * od.Quantity as [TotalPrice],       -- Calculation!
               s.CompanyName              as [ShipperCompany],
               s.Phone              as [ShipperPhone]
        FROM northwind3.dbo.Orders o,
             northwind3.dbo.[Order Details] od,
             northwind3.dbo.[Shippers] s,
             northwind3.dbo.[Products] [p],
             dimCustomers dc,
             dimProducts dp,
             dimSuppliers ds,
             dimTime dt1,
             dimTime dt2 -- Two dimTime tables!!
        WHERE od.OrderID = o.OrderID
          AND dp.ProductID = od.ProductID
          AND dp.ProductID = p.ProductID
          AND o.CustomerID = dc.CustomerID
          AND dt1.Date = o.OrderDate and -- Each dt1,dt2 needs join!
            dt2.Date=o.RequiredDate
          AND s.ShipperID = o.ShipVia
          AND ds.SupplierID = p.SupplierID
    ) o
ON (o.ProductKey = fo.ProductKey -- Assume All Keys are unique
    AND o.CustomerKey = fo.CustomerKey
    AND o.OrderDateKey = fo.OrderDateKey)
WHEN MATCHED THEN -- if they matched, do nothing
    UPDATE
    SET fo.OrderID = o.OrderID -- Dummy update
WHEN NOT MATCHED THEN -- Otherwise, insert a new row
    INSERT (ProductKey, CustomerKey, SupplierKey, OrderDateKey, RequiredDateKey,
            OrderID, UnitPrice, Qty, Discount, TotalPrice, ShipperCompany, ShipperPhone)
    VALUES (o.ProductKey, o.CustomerKey, o.SupplierKey, o.OrderDateKey, o.RequiredDateKey, o.
        OrderID, o.UnitPrice, o.Qty, o.Discount, o.TotalPrice, o.ShipperCompany, o.ShipperPhone);

MERGE INTO factOrders fo
USING
    (
        SELECT ProductKey,
               CustomerKey,
               SupplierKey,
               dt1.TimeKey                as [OrderDatekey],    -- from dimTime
               dt2.TimeKey                as [RequiredDatekey], -- from dimTime
               o.OrderID                  as [OrderID],
               od.UnitPrice               as [UnitPrice],
               Quantity                   as [Qty],
               Discount,
               od.UnitPrice * od.Quantity as [TotalPrice],       -- Calculation!
               s.CompanyName              as [ShipperCompany],
               s.Phone              as [ShipperPhone]
        FROM northwind4.dbo.Orders o,
             northwind4.dbo.[Order Details] od,
             northwind4.dbo.[Shippers] s,
             northwind4.dbo.[Products] [p],
             dimCustomers dc,
             dimProducts dp,
             dimSuppliers ds,
             dimTime dt1,
             dimTime dt2 -- Two dimTime tables!!
        WHERE od.OrderID = o.OrderID
          AND dp.ProductID = od.ProductID
          AND dp.ProductID = p.ProductID
          AND o.CustomerID = dc.CustomerID
          AND dt1.Date = o.OrderDate and -- Each dt1,dt2 needs join!
            dt2.Date=o.RequiredDate
          AND s.ShipperID = o.ShipVia
          AND ds.SupplierID = p.SupplierID
    ) o
ON (o.ProductKey = fo.ProductKey -- Assume All Keys are unique
    AND o.CustomerKey = fo.CustomerKey
    AND o.OrderDateKey = fo.OrderDateKey)
WHEN MATCHED THEN -- if they matched, do nothing
    UPDATE
    SET fo.OrderID = o.OrderID -- Dummy update
WHEN NOT MATCHED THEN -- Otherwise, insert a new row
    INSERT (ProductKey, CustomerKey, SupplierKey, OrderDateKey, RequiredDateKey,
            OrderID, UnitPrice, Qty, Discount, TotalPrice, ShipperCompany, ShipperPhone)
    VALUES (o.ProductKey, o.CustomerKey, o.SupplierKey, o.OrderDateKey, o.RequiredDateKey, o.
        OrderID, o.UnitPrice, o.Qty, o.Discount, o.TotalPrice, o.ShipperCompany, o.ShipperPhone);


-- step 2.6 Validation
Select 'OLTP' as [SourceType], 'northwind3.dbo.Order Details' as [TableName], count(*) as [RowCounts] from northwind3.dbo.[Order Details]
Union
Select 'OLTP' as [SourceType], 'northwind4.dbo.Order Details' as [TableName], count(*) as [RowCounts] from northwind4.dbo.[Order Details]
Union
Select 'OLAP' as [SourceType], 'northwind_dw.factOrders' as [TableName], count(*) as [RowCounts] from northwind_dw.dbo.factOrders;

