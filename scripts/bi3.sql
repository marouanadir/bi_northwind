USE Northwind_BI3;
GO




CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,      -- YYYYMMDD
    DateValue DATE NOT NULL,
    [Year] INT,
    [Quarter] INT,
    [Month] INT,
    MonthName NVARCHAR(20),
    [Day] INT,
    DayOfWeek INT,
    IsWeekend BIT
);
GO



-- Dimension Customer
CREATE TABLE DimCustomer (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,    
    CustomerCode NVARCHAR(50),                   
    Company NVARCHAR(255),
    LastName NVARCHAR(100),
    FirstName NVARCHAR(100),
    City NVARCHAR(100),
    StateProvince NVARCHAR(100),
    CountryRegion NVARCHAR(100)
);
GO

-- Dimension Employee
CREATE TABLE DimEmployee (
    EmployeeID INT IDENTITY(1,1) PRIMARY KEY,    
    EmployeeCode NVARCHAR(50),                   
    LastName NVARCHAR(100),
    FirstName NVARCHAR(100),
    JobTitle NVARCHAR(100),
    City NVARCHAR(100),
    CountryRegion NVARCHAR(100)
);
GO

-- Dimension Order (ici on garde OrderID comme natural key d'origine)
CREATE TABLE DimOrder (
    OrderID INT PRIMARY KEY,                      
    CustomerCode NVARCHAR(50),                    -- Natural key pour mapping
    EmployeeCode NVARCHAR(50),                    -- Natural key pour mapping
    OrderDate DATE,
    ShippedDate DATE,
    StatusID INT
);
GO

-- Dimension Region
CREATE TABLE DimRegion (
  RegionID INT IDENTITY(1,1) PRIMARY KEY,
  RegionCode NVARCHAR(20),
  RegionName NVARCHAR(100)
);
GO

-- Dimension Territory (liaison vers Region)
CREATE TABLE DimTerritory (
  TerritoryID INT IDENTITY(1,1) PRIMARY KEY,
  TerritoryCode NVARCHAR(20),
  TerritoryName NVARCHAR(150),
  RegionID INT NULL,
  CONSTRAINT FK_Territory_Region FOREIGN KEY (RegionID) REFERENCES DimRegion(RegionID)
);
GO

-- Bridge Employee <-> Territory (many-to-many)
CREATE TABLE EmployeeTerritoryBridge (
  EmployeeID INT NOT NULL,
  TerritoryID INT NOT NULL,
  CONSTRAINT PK_EmployeeTerritory PRIMARY KEY(EmployeeID, TerritoryID),
  CONSTRAINT FK_EmpTerr_Employee FOREIGN KEY(EmployeeID) REFERENCES DimEmployee(EmployeeID),
  CONSTRAINT FK_EmpTerr_Territory FOREIGN KEY(TerritoryID) REFERENCES DimTerritory(TerritoryID)
);
GO

-- Table de faits (Fact)
CREATE TABLE Tabledefait (
    FactID INT IDENTITY(1,1) PRIMARY KEY,       
    OrderID INT,                                 
    CustomerID INT,                              
    EmployeeID INT,                              
    OrdersDelivered INT,                         
    OrdersNotDelivered INT, 
    RegionID INT NULL, 
    TerritoryID INT NULL,
    DateKey INT NULL,
    CONSTRAINT FK_Fact_Region FOREIGN KEY (RegionID) REFERENCES DimRegion(RegionID),
    CONSTRAINT FK_Fact_Territory FOREIGN KEY (TerritoryID) REFERENCES DimTerritory(TerritoryID),
    CONSTRAINT FK_Fact_Order FOREIGN KEY (OrderID) REFERENCES DimOrder(OrderID),
    CONSTRAINT FK_Fact_Customer FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
    CONSTRAINT FK_Fact_Employee FOREIGN KEY (EmployeeID) REFERENCES DimEmployee(EmployeeID),
    CONSTRAINT FK_Fact_Date FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);
GO
