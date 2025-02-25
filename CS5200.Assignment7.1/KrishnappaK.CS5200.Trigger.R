#*****************************************
#* title: ASSIGNMENT 07.1 
#* subtitle: Build Triggers in SQLite
#* author: Krishnappa, Kushal
#* date: Spring 2025
#*****************************************

#*****************************************
#* Install Packages
#* @param packages - list of packages required for script to run
#*****************************************
installPackagesOnDemand <- function(packages) {
  # install packages not yet installed
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
  # load all packages by applying 'library' function
  invisible(lapply(packages, library, character.only = TRUE))
}

#*****************************************
#* Create a SQLite Database Connection
#* @param dbName - name of the database
#* @param dbPath - path to the database. default is current directory.
#*****************************************
createSQLiteDBConnection <- function(dbName, dbPath = "") {
  # create a full db path
  dbFile = paste0(dbPath,dbName)
  # create a connection to the database
  dbCon <- dbConnect(RSQLite::SQLite(), dbFile)
  return(dbCon)
}

#*****************************************
#* Task 1: Function to Alter Employees Table
#* @param dbCon - database connection
#*****************************************
alterEmployeesTable <- function(dbCon) {
  # drop if 'TotalSold' column already exists in Employees table
  if ("TotalSold" %in% dbListFields(dbCon, "Employees")) {
    query <- "ALTER TABLE Employees DROP COLUMN TotalSold;"
    dbExecute(dbCon, query)
  }
  # add a new column "TotalSold" to the Employees table
  query <- "
  ALTER TABLE Employees 
  ADD COLUMN TotalSold 
  NUMERIC DEFAULT 0.0;
  "
  dbExecute(dbCon, query)
}

#*****************************************
#* Task 2: Function to Update the 'TotalSold' Column in Employees Table
#* @param dbCon - database connection
#*****************************************
updateTotalSoldColumnInEmployeesTable <- function(dbCon) {
  # update the 'TotalSold' column in Employees table
  query <- "
    UPDATE Employees
    SET TotalSold = (
        SELECT COALESCE(SUM(od.Quantity * p.Price), 0)
        FROM Orders o
        JOIN OrderDetails od ON o.OrderID = od.OrderID
        JOIN Products p ON od.ProductID = p.ProductID
        WHERE o.EmployeeID = Employees.EmployeeID
    );
  "
  dbExecute(dbCon, query)
}

#*****************************************
#* Task 3: After Insert Trigger On "OrderDetails" table
#* @param dbCon - database connection
#*****************************************
insertTriggerOnOrderDetailsTable <- function(dbCon) {
  # drop the trigger if it already exists
  query <- "DROP TRIGGER IF EXISTS update_totalsold_on_insert_orderdetails;"
  dbExecute(dbCon, query)
  
  # create a new insert trigger
  query <- "
  CREATE TRIGGER update_totalsold_on_insert_orderdetails
  AFTER INSERT ON OrderDetails
  FOR EACH ROW
  BEGIN
      UPDATE Employees
      SET TotalSold = (
          SELECT COALESCE(SUM(od.Quantity * p.Price), 0)
          FROM Orders o
          JOIN OrderDetails od ON o.OrderID = od.OrderID
          JOIN Products p ON od.ProductID = p.ProductID
          WHERE o.EmployeeID = Employees.EmployeeID
      )
      WHERE EmployeeID = (
          SELECT o.EmployeeID
          FROM Orders o
          WHERE o.OrderID = NEW.OrderID
      );
  END;
  "
  dbExecute(dbCon, query)
}

#*****************************************
#* Task 4: After Update Trigger On "OrderDetails" Table
#* @param dbCon - database connection
#*****************************************
updateTriggerOnOrderDetailsTable <- function(dbCon) {
  # drop the trigger if it already exists
  query <- "DROP TRIGGER IF EXISTS update_totalsold_on_update_orderdetails;"
  dbExecute(dbCon, query)
  
  # create a new udpate trigger
  query <- "
  CREATE TRIGGER update_totalsold_on_update_orderdetails
  AFTER UPDATE ON OrderDetails
  FOR EACH ROW
  BEGIN
      UPDATE Employees
      SET TotalSold = (
          SELECT COALESCE(SUM(od.Quantity * p.Price), 0)
          FROM Orders o
          JOIN OrderDetails od ON o.OrderID = od.OrderID
          JOIN Products p ON od.ProductID = p.ProductID
          WHERE o.EmployeeID = Employees.EmployeeID
      )
      WHERE EmployeeID = (
          SELECT o.EmployeeID
          FROM Orders o
          WHERE o.OrderID = NEW.OrderID
      );
  END;
  "
  dbExecute(dbCon, query)
}

#*****************************************
#* Task 5: After Delete Trigger On "OrderDetails" table
#* @param dbCon - database connection
#*****************************************
deleteTriggerOnOrderDetailsTable <- function(dbCon) {
  # drop the trigger if it already exists
  query <- "DROP TRIGGER IF EXISTS update_totalsold_on_delete_orderdetails;"
  dbExecute(dbCon, query)
  
  # create a new delete trigger
  query <- "
  CREATE TRIGGER update_totalsold_on_delete_orderdetails
  AFTER DELETE ON OrderDetails
  FOR EACH ROW
  BEGIN
      UPDATE Employees
      SET TotalSold = (
          SELECT COALESCE(SUM(od.Quantity * p.Price), 0)
          FROM Orders o
          JOIN OrderDetails od ON o.OrderID = od.OrderID
          JOIN Products p ON od.ProductID = p.ProductID
          WHERE o.EmployeeID = Employees.EmployeeID
      )
      WHERE EmployeeID = (
          SELECT o.EmployeeID
          FROM Orders o
          WHERE o.OrderID = OLD.OrderID
      );
  END;
  "
  dbExecute(dbCon, query)
}

#*****************************************
#* Task 6: Validating the Triggers
#* @param dbCon - database connection
#*****************************************
validateInsertTrigger <- function(dbCon) {
  
  # validate the insert trigger using the testthat package
  test_that("Test Insert Trigger: experience_level table should contain 3 rows", {
    experience_level <- dbReadTable(dbCon, "experience_level")
    expect_equal(nrow(experience_level), 3)
  })
  
}

validateUpdateTrigger <- function(dbCon) {
  # validate the update trigger using the testthat package
  test_that("Test Update Trigger: author table should contain 4 rows", {
    author <- dbReadTable(dbCon, "author")
    expect_equal(nrow(author), 4)
  })
}

validateDeleteTrigger <- function(dbCon) {
  # validate the delete trigger using the testthat package
  test_that("Test Delete Trigger: author table should contain 4 rows", {
    author <- dbReadTable(dbCon, "author")
    expect_equal(nrow(author), 4)
  })
}

validateTriggers <- function(dbCon) {
  # validate the insert trigger
  validateInsertTrigger(dbCon)
  # validate the update trigger
  validateUpdateTrigger(dbCon)
  # validate the delete trigger
  validateDeleteTrigger(dbCon)
}

#*****************************************
#* Main Method
#*****************************************
main <- function() {
  # packages needed for R program to run
  packages <- c("RSQLite", "testthat")
  installPackagesOnDemand(packages)
  
  # create a database connection
  dbCon <- createSQLiteDBConnection("OrdersDB.sqlitedb.db")
  # enable foreign key constraints
  # Reference: http://artificium.us/lessons/06.r/l-6-300-create-sqlitedb-in-r/l-6-300.html
  dbExecute(dbCon, "PRAGMA foreign_keys = ON")
  
  # alter the Employees table to add a new column 'TotalSold'
  alterEmployeesTable(dbCon)
  # update the 'TotalSold' column in Employees table
  updateTotalSoldColumnInEmployeesTable(dbCon)
  # create trigger on OrderDetails table for insert
  insertTriggerOnOrderDetailsTable(dbCon)
  # create trigger on OrderDetails table for update
  updateTriggerOnOrderDetailsTable(dbCon)
  # create trigger on OrderDetails table for delete
  deleteTriggerOnOrderDetailsTable(dbCon)
  # validate the triggers
  validateTriggers(dbCon)
  
  # close the database connection
  dbDisconnect(dbCon)
}

main()
