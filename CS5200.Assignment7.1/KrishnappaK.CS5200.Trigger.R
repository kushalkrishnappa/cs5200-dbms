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
  tryCatch({
    cat("Validating insert trigger\n")
    # sample data for validation
    employeeIdToTest <- 3
    productIdToTest <- 3
    orderIdToTest <- 10253
    
    # get the total sold of employee before inserting a row in OrderDetails
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee for id %s: %s\n", employeeIdToTest, employee$TotalSold))
    
    # get the price of the product
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of product for id %s: %s\n", productIdToTest, productPrice$Price))
    
    # insert a row in OrderDetails with quantity 1
    dbExecute(dbCon, sprintf("INSERT INTO OrderDetails (OrderID, ProductID, Quantity) VALUES (%s, %s, 1);", orderIdToTest, productIdToTest))
    
    # get the total sold of employee after inserting a row in OrderDetails
    employeeAfterInsert <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee after inserting 1 quantity for id %s: %s\n", employeeIdToTest, employeeAfterInsert$TotalSold))
    
    # validate the trigger by checking if the TotalSold column in Employees table is updated
    cat(sprintf("Expected increase in price: %s, actual:%s\n", productPrice$Price * 1, employeeAfterInsert$TotalSold - employee$TotalSold))
    test_that("Test Insert Trigger:", {
      expect_equal(employeeAfterInsert$TotalSold - employee$TotalSold, productPrice$Price * 1)
    })
    cat("________________________________________________\n")
  }, error = function(e) {
    cat("Error during validation of insert trigger: ", e$message, "\n")
  })
}


validateUpdateTrigger <- function(dbCon) {
  tryCatch({
    cat("Validating update trigger\n")
    # sample data for validation
    employeeIdToTest <- 3
    productIdToTest <- 3
    orderIdToTest <- 10253
    
    # get the total sold of employee before updating a row in OrderDetails
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee for id %s: %s\n", employeeIdToTest, employee$TotalSold))
    
    # get the price of the product
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of product for id %s: %s\n", productIdToTest, productPrice$Price))
    
    # update the quantity of a row in OrderDetails to 2
    dbExecute(dbCon, sprintf("UPDATE OrderDetails SET Quantity = 2 WHERE OrderID = %s AND ProductID = %s;", orderIdToTest, productIdToTest))
    
    # get the total sold of employee after updating a row in OrderDetails
    employeeAfterUpdate <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee after updating quantity to 2 for id %s: %s\n", employeeIdToTest, employeeAfterUpdate$TotalSold))
    
    # validate the trigger by checking if the TotalSold column in Employees table is updated
    cat(sprintf("Expected increase in price: %s, actual:%s\n", productPrice$Price * 1, employeeAfterUpdate$TotalSold - employee$TotalSold))
    
    # assert the expected and actual values
    test_that("Test Insert Trigger:", {
      expect_equal(employeeAfterUpdate$TotalSold - employee$TotalSold, productPrice$Price * 1)
    })
    cat("________________________________________________\n")
  }, error = function(e) {
    cat("Error during validation of update trigger: ", e$message, "\n")
  })
}

validateDeleteTrigger <- function(dbCon) {
  tryCatch({
    cat("Validating delete trigger\n")
    # sample data for validation
    employeeIdToTest <- 3
    productIdToTest <- 3
    orderIdToTest <- 10253
    
    # get the total sold of employee before deleting a row in OrderDetails
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee for id %s: %s\n", employeeIdToTest, employee$TotalSold))
    
    # get the price of the product
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of product for id %s: %s\n", productIdToTest, productPrice$Price))
    
    # delete the row in OrderDetails
    dbExecute(dbCon, sprintf("DELETE FROM OrderDetails WHERE OrderID = %s AND ProductID = %s;", orderIdToTest, productIdToTest))
    
    # get the total sold of employee after deleting a row in OrderDetails
    employeeAfterDelete <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee after deleting quantity for id %s: %s\n", employeeIdToTest, employeeAfterDelete$TotalSold))
    
    # validate the trigger by checking if the TotalSold column in Employees table is updated
    cat(sprintf("Expected decrease in price: %s, actual:%s\n", productPrice$Price * 2, employee$TotalSold - employeeAfterDelete$TotalSold))
    
    # assert the expected and actual values
    test_that("Test Insert Trigger:", {
      expect_equal(employee$TotalSold - employeeAfterDelete$TotalSold, productPrice$Price * 2)
    })
    cat("________________________________________________\n")
  }, error = function(e) {
    cat("Error during validation of delete trigger: ", e$message, "\n")
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
