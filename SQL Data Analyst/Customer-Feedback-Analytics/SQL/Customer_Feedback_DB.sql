
-- SQL Data Analyst Portfolio – Customer Feedback DB
   
-- 1️ Create database (drop if exists)

IF EXISTS (SELECT * FROM sys.databases WHERE name = 'SQL_Data_Analyst')
BEGIN
    PRINT 'Database exists, dropping it...'
    DROP DATABASE SQL_Data_Analyst;
END
GO

PRINT 'Creating database SQL_Data_Analyst...'
CREATE DATABASE SQL_Data_Analyst;
GO

USE SQL_Data_Analyst;
GO


-- 2️ Create tables (if not exists)

-- Customers table
IF OBJECT_ID('Customers', 'U') IS NULL
BEGIN
    CREATE TABLE Customers (
        customer_id INT PRIMARY KEY,  -- Unique customer identifier
        name NVARCHAR(100),
        email NVARCHAR(100),
        region NVARCHAR(50)
    );
    PRINT 'Customers table created.'
END

-- Products table
IF OBJECT_ID('Products', 'U') IS NULL
BEGIN
    CREATE TABLE Products (
        product_id INT PRIMARY KEY,   -- Unique product identifier
        name NVARCHAR(100),
        category NVARCHAR(50),
        price DECIMAL(10,2)
    );
    PRINT 'Products table created.'
END

-- Reviews table
IF OBJECT_ID('Reviews', 'U') IS NULL
BEGIN
    CREATE TABLE Reviews (
        review_id INT PRIMARY KEY,          -- Unique review identifier
        customer_id INT,
        product_id INT,
        rating INT CHECK (rating BETWEEN 1 AND 5),  -- Rating from 1 to 5
        comments NVARCHAR(MAX),
        review_date DATE,
        FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id)
    );
    PRINT 'Reviews table created.'
END


-- 3️ Staging tables for CSV / JSON / XML imports
IF OBJECT_ID('Staging_CustomerSurvey', 'U') IS NULL
BEGIN
    CREATE TABLE Staging_CustomerSurvey (
        review_id INT,
        customer_id INT,
        product_id INT,
        rating INT,
        comments NVARCHAR(MAX),
        review_date NVARCHAR(20)
    );
    PRINT 'Staging_CustomerSurvey table created.'
END

IF OBJECT_ID('Staging_WebFeedback', 'U') IS NULL
BEGIN
    CREATE TABLE Staging_WebFeedback (
        review_id INT,
        customer_id INT,
        product_id INT,
        rating INT,
        comments NVARCHAR(MAX),
        review_date NVARCHAR(20)
    );
    PRINT 'Staging_WebFeedback table created.'
END

IF OBJECT_ID('Staging_ExternalReviews', 'U') IS NULL
BEGIN
    CREATE TABLE Staging_ExternalReviews (
        review_id INT,
        customer_id INT,
        product_id INT,
        rating INT,
        comments NVARCHAR(MAX),
        review_date NVARCHAR(20)
    );
    PRINT 'Staging_ExternalReviews table created.'
END


-- 4️ Create views for analysis
-- View: product reviews summary
IF OBJECT_ID('vw_ProductReviews', 'V') IS NULL
BEGIN
    CREATE VIEW vw_ProductReviews AS
    SELECT 
        p.product_id,
        p.name AS product_name,
        p.category,
        AVG(r.rating) AS avg_rating,
        COUNT(r.review_id) AS total_reviews
    FROM Products p
    LEFT JOIN Reviews r ON p.product_id = r.product_id
    GROUP BY p.product_id, p.name, p.category;
    PRINT 'View vw_ProductReviews created.'
END

-- View: top 10 products
-- Check if view exists, then drop it
-- Drop if exists
IF OBJECT_ID('vw_ProductReviews', 'V') IS NOT NULL
    DROP VIEW vw_ProductReviews;
GO

-- Create the product reviews summary view

CREATE VIEW vw_TopProducts AS
SELECT TOP 10 *
FROM vw_ProductReviews
ORDER BY avg_rating DESC, total_reviews DESC;
GO  -- MUST separate batches


-- 5️ Example analysis queries

PRINT '=== Top 10 Products ==='
SELECT * FROM vw_TopProducts;

PRINT '=== Common Complaints by Category ==='
SELECT 
    p.category,
    SUM(CASE WHEN r.comments LIKE '%damaged%' THEN 1 ELSE 0 END) AS Damaged,
    SUM(CASE WHEN r.comments LIKE '%late%' THEN 1 ELSE 0 END) AS Late,
    SUM(CASE WHEN r.comments LIKE '%defective%' THEN 1 ELSE 0 END) AS Defective
FROM Reviews r
JOIN Products p ON r.product_id = p.product_id
GROUP BY p.category;

PRINT '=== Customer Sentiment ==='
SELECT 
    review_id, product_id, rating, comments,
    CASE 
        WHEN rating >= 4 THEN 'Positive'
        WHEN rating = 3 THEN 'Neutral'
        ELSE 'Negative'
    END AS sentiment
FROM Reviews;

PRINT '=== Trend Analysis by Month ==='
SELECT 
    FORMAT(review_date, 'yyyy-MM') AS Month,
    COUNT(*) AS ReviewCount,
    AVG(rating) AS AvgRating
FROM Reviews
GROUP BY FORMAT(review_date, 'yyyy-MM')
ORDER BY Month;


-- 6️ Stored procedure example (automated CSV import)

-- Step 1: Drop the procedure if it exists
IF OBJECT_ID('ImportCustomerSurvey', 'P') IS NOT NULL
    DROP PROCEDURE ImportCustomerSurvey;
GO  -- MUST separate batches before CREATE PROCEDURE

-- Step 2: Create procedure (only statement in this batch)
CREATE PROCEDURE ImportCustomerSurvey
    @FilePath NVARCHAR(200)
AS
BEGIN
    -- Import data from CSV to staging table
    BULK INSERT Staging_CustomerSurvey
    FROM @FilePath
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        CODEPAGE = '65001'
    );

    -- Insert into main Reviews table
    INSERT INTO Reviews (review_id, customer_id, product_id, rating, comments, review_date)
    SELECT review_id, customer_id, product_id, rating, comments, CAST(review_date AS DATE)
    FROM Staging_CustomerSurvey;
END;
GO  -- end of procedure batch

-- Optional: print confirmation after creation
PRINT 'Stored procedure ImportCustomerSurvey created successfully.';
GO

