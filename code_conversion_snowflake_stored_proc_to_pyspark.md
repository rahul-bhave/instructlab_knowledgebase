# code_conversion_snowflake_stored_proc_to_pyspark

## Example Conversions:
In this section, we will demonstrate a number of sample conversions from stored procedures to PySpark.

## Temporary Tables:
Stored procedures will often use temporary tables to keep data for further processing, which can be especially useful when a source dataset is referenced multiple times within the same session. By design Apache Spark will keep data in memory unless the data size is too large then the dataset will spill to disk. With data caching and the fact that Spark holds data in memory, the need to materialize data as a temporary table is typically not required. It is recommended to use temporary views instead. Temporary views are conceptually the same as temporary tables in other systems. 
One of the major advantages of using a temporary view is the ability to reference a Spark DataFrame within a SQL statement.
Assume we have a process that selects data from a source table and writes to multiple output tables. 
To do so, we read our source table into a temporary table, execute different transformations, and write to multiple target tables.

#### Stored prodeure
CREATE PROCEDURE WriteMultipleTables
AS
-- select into temp table
SELECT Id, Name, qty, ModifiedDate
INTO #tempTable
FROM my_schema.my_staging_source_table;

-- write to table 1 from staging
INSERT INTO my_schema.my_target_table1
SELECT * 
FROM #tempTable
WHERE Name IS NOT NULL;

-- write to table 2 from staging with aggregates
INSERT INTO my_schema.my_target_table2
SELECT Id, Name, ModifiedDate, sum(qty) as qty_sum
FROM #tempTable
WHERE Name IS NOT NULL
GROUP BY Id, Name, ModifiedDate;

#### PySpark Example:

df = spark.read.table('my_schema.my_staging_source_table')

df.createOrReplaceTempView('tempTable')

spark.sql("""
  INSERT INTO my_schema.my_target_table1
  SELECT * 
  FROM tempTable
  WHERE Name IS NOT NULL
""")

spark.sql("""
  INSERT INTO my_schema.my_target_table1
  SELECT Id, Name, ModifiedDate, sum(qty) as qty_sum
  FROM tempTable
  WHERE Name IS NOT NULL
  GROUP BY Id, Name, ModifiedDate
""")

As you can see in both examples above there is little variation between the core SQL logic that was expressed in the source systems. The Python syntax drifts slightly from the original code but engineers will have more functionality available to them using Python than purely SQL alone.

### Common Table Expressions

One of the top questions we receive from customers that are migrating to a lakehouse is whether or not Databricks supports common table expressions (CTEs). Common table expressions are extremely common in data warehousing and allow engineers to write complex queries while maintaining high readability of the code. Engineers use CTEs to create datasets that can be referenced within a session in a subsequent SQL query.

#### Stored Procedure example:

WITH cte as (
  SELECT *
  FROM my_schema.my_staging_source_table
)

SELECT * FROM cte

#### Pyspark example:

WITH cte as (
  SELECT *
  FROM my_schema.my_staging_source_table
)

SELECT * FROM cte

the two examples above are exactly the same! That is because common table expressions are supported in Spark SQL. One area that becomes a bit more complicated is recursive CTEs. At the time of writing Spark SQL does not support recursive CTEs, however, using PySpark developers can refactor their code to achieve this.

### Recursive Common Table Expression:

#### Stored procedure sql:

CREATE PROCEDURE [dbo].[uspGetBillOfMaterials]
    @StartProductID [int],
    @CheckDate [datetime]
AS
BEGIN
    SET NOCOUNT ON;

    -- Use recursive query to generate a multi-level Bill of Material (i.e. all level 1 
    -- components of a level 0 assembly, all level 2 components of a level 1 assembly)
    -- The CheckDate eliminates any components that are no longer used in the product on this date.
    WITH [BOM_cte]([ProductAssemblyID], [ComponentID], [ComponentDesc], [PerAssemblyQty], [StandardCost], [ListPrice], [BOMLevel], [RecursionLevel]) -- CTE name and columns
    AS (
        SELECT b.[ProductAssemblyID], b.[ComponentID], p.[Name], b.[PerAssemblyQty], p.[StandardCost], p.[ListPrice], b.[BOMLevel], 0 -- Get the initial list of components for the bike assembly
        FROM [Production].[BillOfMaterials] b
            INNER JOIN [Production].[Product] p 
            ON b.[ComponentID] = p.[ProductID] 
        WHERE b.[ProductAssemblyID] = @StartProductID 
            AND @CheckDate >= b.[StartDate] 
            AND @CheckDate <= ISNULL(b.[EndDate], @CheckDate)
        UNION ALL
        SELECT b.[ProductAssemblyID], b.[ComponentID], p.[Name], b.[PerAssemblyQty], p.[StandardCost], p.[ListPrice], b.[BOMLevel], [RecursionLevel] + 1 -- Join recursive member to anchor
        FROM [BOM_cte] cte
            INNER JOIN [Production].[BillOfMaterials] b 
            ON b.[ProductAssemblyID] = cte.[ComponentID]
            INNER JOIN [Production].[Product] p 
            ON b.[ComponentID] = p.[ProductID] 
        WHERE @CheckDate >= b.[StartDate] 
            AND @CheckDate <= ISNULL(b.[EndDate], @CheckDate)
        )
    -- Outer select from the CTE
    SELECT b.[ProductAssemblyID], b.[ComponentID], b.[ComponentDesc], SUM(b.[PerAssemblyQty]) AS [TotalQuantity] , b.[StandardCost], b.[ListPrice], b.[BOMLevel], b.[RecursionLevel]
    FROM [BOM_cte] b
    GROUP BY b.[ComponentID], b.[ComponentDesc], b.[ProductAssemblyID], b.[BOMLevel], b.[RecursionLevel], b.[StandardCost], b.[ListPrice]
    ORDER BY b.[BOMLevel], b.[ProductAssemblyID], b.[ComponentID]
    OPTION (MAXRECURSION 25) 
END;
GO

Recursive CTEs are most commonly used to model hierarchical data. In the case above, we are looking to get all the parts associated with a specific assembly item. Another common use case is organizational structures.

#### Pyspark Equivalent code:

i = 1
check_date = '2010-12-23'
start_product_id = 972 # provide a specific id 

df = spark.sql(f"""
SELECT b.ProductAssemblyID
  , b.ComponentID
  , p.Name
  , b.PerAssemblyQty
  , p.StandardCost
  , p.ListPrice
  , b.BOMLevel
  , 0 as RecursionLevel 
FROM BillOfMaterials b
    INNER JOIN Product p ON b.ComponentID = p.ProductID
WHERE b.ProductAssemblyID = {start_product_id} AND '{check_date}' >= b.StartDate AND '{check_date}' <= IFNULL(b.EndDate, '{check_date}')
""")


df.createOrReplaceTempView('recursion_df')

while True:
  
  bill_df = spark.sql(f"""
  SELECT b.ProductAssemblyID
    , b.ComponentID
    , p.Name
    , b.PerAssemblyQty
    , p.StandardCost
    , p.ListPrice
    , b.BOMLevel
    , {i} as RecursionLevel 
  FROM recursion_df cte
      INNER JOIN BillOfMaterials b ON b.ProductAssemblyID = cte.ComponentID
      INNER JOIN Product p ON b.ComponentID = p.ProductID
  WHERE '{check_date}' >= b.StartDate AND '{check_date}' <= IFNULL(b.EndDate, '{check_date}')
  """)
  
  
  bill_df.createOrReplaceTempView('recursion_df')
 
  df = df.union(bill_df)
  
  if bill_df.count() == 0:
      df.createOrReplaceTempView("final_df")
      break
  else:
      i += 1

 ### Parameters and Variables   

 A best practice in programming is the DRY Principle — Don’t repeat yourself! — which reduces the amount of repetitive code in software. Parameters and variables allow data engineers to reduce the number of code objects required by allowing for more dynamic execution depending on the values provided at runtime. Enterprise data warehouse systems can be very large so reducing the amount of code in the system by making it reusable is key for governance.

Let’s take the following example that uses both parameters and variables.

#### Stored procedure

CREATE PROCEDURE my_stored_procedure @CostCenter INT
AS

DECLARE @year_variable INT;
DECLARE @total_qty BIGINT;
SET @year_variable = YEAR(GETDATE());

SET @total_qty = (
  SELECT sum(qty)
  FROM my_schema.my_staging_source_table
  WHERE YEAR(ModifiedDate) = @year_variable and CostCenter = @CostCenter;
);

CREATE TABLE my_schema.cost_center_qty_agg
AS

WITH cte as (
  SELECT year, sum(qty) as summed_qty
  FROM my_schema.my_staging_source_table
  WHERE CostCenter = @CostCenter
  GROUP BY YEAR
)

SELECT *
FROM cte
WHERE summed_qty > @total_qty ;

#### Pyspark example:

import datetime

dbutils.widgets.text('CostCenter', '')
cost_center = dbutils.widgets.get('CostCenter')

year_variable = datetime.date.today().year

total_qty = spark.sql(f"""
  SELECT sum(qty)
  FROM my_schema.my_staging_source_table
  WHERE YEAR(ModifiedDate) = {year_variable} and CostCenter = {cost_center}
""").collect()[0][0]

spark.sql(f"""
  CREATE TABLE my_schema.cost_center_qty_agg
  AS
  WITH cte as (
  SELECT year, sum(qty) as summed_qty
  FROM my_schema.my_staging_source_table
  WHERE CostCenter = {cost_center}
  GROUP BY YEAR
  )
  SELECT *
  FROM cte
  WHERE summed_qty > {total_qty}
""")

Converting SQL is fairly straightforward as it is mostly small syntax changes between systems. One recommendation to make conversion slightly easier would be related to variable naming. Naming objects in programming is difficult for engineers, but when you start translating code from system to system with different style conventions it can be near impossible.

Thinking strictly about migration scenarios, one way to simplify the effort would be keeping the names of your existing variables as is. If you have a variable CostCenter and you want to convert the script to Python, then leave it as camel case instead of trying to follow the Python style guide and change every single reference to that object. While reading the code may be a little strange due to inconsistent style, functionally there is no difference. By ignoring stylistic conversions the migration will go faster and save engineers the headache of trying to find every reference to the variable in the code to simply change the way it looks. Every engineer who has spent time searching for a missing comma or incorrect indentation in Python would agree!

it is important to note the following:

Variables use the following syntax: <prefix>.<variable_name> where <prefix> can be any string value.
- It is a best practice to be consistent with your prefix for readability purposes. In the examples in this blog, we have used var as a standard to indicate it is a variable.
- Assigning a value to a variable is done as follows: SET <prefix>.<variable_name> = …
- Using a variable in a SQL statement is done as follows: ${<prefix>.<variable_name>}
- If you assign a variable the result set of a query, it is done so lazily and is not evaluated until there is an action.
Widgets are parameters that are passed in as string values so you may need to convert the data type.
- Widgets can be referenced with a preceding dollar sign ($) e.g. $my_widget_name.

### Conditional Statements

Conditional statements (if, else, etc.) are used to define when a set of code should be executed based on the specified condition. When developing ETL pipelines there are many scenarios where conditional flow is used, and these pipelines will need to be converted to PySpark.

Using the following example of IF ELSE from Microsoft’s AdventureWorks database, let’s see how we would translate it into PySpark:

#### SparkSQL

CREATE PROCEDURE CalculateWeight
AS

DECLARE @maxWeight FLOAT, @productKey INTEGER ;
SET @maxWeight = 100.00 ;
SET @productKey = 424 ;

IF @maxWeight <= (SELECT Weight from DimProduct WHERE ProductKey = @productKey)
  SELECT @productKey AS ProductKey
    , EnglishDescription
    , Weight
    , 'This product is too heavy to ship and is only available for pickup.' AS ShippingStatus
  
  FROM my_schema.DimProduct WHERE ProductKey = @productKey
  
ELSE
  SELECT @productKey AS ProductKey
    , EnglishDescription
    , Weight
    , 'This product is available for shipping or pickup.' AS ShippingStatus
  FROM my_schema.DimProduct WHERE ProductKey = @productKey

#### Pyspark equivalent

maxWeight = 100
productKey = 424

w = spark.sql(f"SELECT Weight from DimProduct WHERE ProductKey = {productKey}").collect()[0][0]

if maxWeight <= w:
  spark.sql(f"""
    SELECT {productKey} AS ProductKey, EnglishDescription, Weight, 'This product is too heavy to ship and is only available for pickup.' AS ShippingStatus
    FROM DimProduct WHERE ProductKey = {productKey}
  """)
else :
  spark.sql(f"""
    SELECT {productKey} AS ProductKey, EnglishDescription, Weight, 'This product is available for shipping or pickup.' AS ShippingStatus
    FROM DimProduct WHERE ProductKey = {productKey}
  """)

### Loops:

Loops are used to iteratively execute a set of code until a breaking condition is met. While loops are primarily used in data warehousing for two reasons:

- Breaking down the batch processing of a large amount of data into smaller amounts due to limited resources
- Repeating the same pipeline steps with different parameters
 
Let’s use an example of breaking down a larger data set to complete smaller batch inserts into a target table.

#### Stored procedure

CREATE PROCEDURE BatchInserts
AS

DECLARE @counter int;
DECLARE @rowcount int ;
DECLARE @batchsize int;

SET @batchsize = 10000;
SET @counter = 0;
SET @rowcount = SELECT count(1) FROM my_source_table;

WHILE @counter <= @rowcount
  BEGIN
    INSERT INTO my_schema.my_target_table
    SELECT TOP (@batchsize) * FROM my_schema.my_source_table WHERE (id > @counter and id <= (@counter+@batchsize)
    
    SET @counter = @counter + @batchsize
  END

#### Pyspark example

this is the same as spark.sql("SELECT * FROM my_source_table")

df = spark.read.table('my_source_table') 

df.write.mode("append").saveAsTable("my_target_table")

### Exception Handling

Ideally, exception handling would not be required. We all would be able to envision every possible scenario for processing data and gracefully determine how best to operate in that situation. For us mere mortals, logging errors and exceptions is required to allow individuals to investigate issues and remediate them.

Legacy data warehouses allowed the ability to capture state and exception information that could be saved to relational databases. Spark SQL does not have a try/catch block, but all the other languages supported in Databricks do! There are two different ways to convert a SQL block to PySpark.

#### Stored Procedure example:

CREATE PROCEDURE MyExceptionProcedure
AS

BEGIN TRY
  INSERT INTO my_schema.my_target_table
  SELECT *
  FROM my_schema.my_source_table
END TRY

BEGIN CATCH
  INSERT INTO dbo.DB_Errors
  VALUES
  (SUSER_SNAME(),
  ERROR_NUMBER(),
  ERROR_STATE(),
  ERROR_SEVERITY(),
  ERROR_LINE(),
  ERROR_PROCEDURE(),
  ERROR_MESSAGE(),
  GETDATE());
END CATCH

#### Pyspark example:

try:

spark.sql("""
  INSERT INTO my_schema.my_target_table
  SELECT * FROM my_schema.my_source_table
""")

except Exception as e:

spark.sql(f"""
  INSERT INTO dbo.DB_ERRORS
  VALUES({str(e)}, {current_timestamp(}
""")

##### Pyspark example using Logging library

try:

spark.sql("""
  INSERT INTO my_schema.my_target_table
  SELECT * FROM my_schema.my_source_table
""")

except Exception as e:

spark.sql(f"""
  INSERT INTO dbo.DB_ERRORS
  VALUES({str(e)}, {current_timestamp(}
""")

  


  
  






