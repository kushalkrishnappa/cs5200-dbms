#*****************************************
#* title: Part F / Test Data Loading Process
#* author: Krishnappa, Kushal
#* date: Spring 2025
#*****************************************

#*****************************************
#* Install Required Packages
#* @param packages - list of packages
#*****************************************
installPackagesOnDemand <- function(packages) {
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
}

#*****************************************
#* Load Required Packages
#* @param packages - list of packages
#*****************************************
loadRequiredPackages <- function(packages) {
  # load required packages
  for (package in packages) {
    suppressMessages({
      library(package, character.only = TRUE)
    })
  }
}

#*****************************************
#* Close Db Connections Over Threshold
#*****************************************
closeDbConnectionsOverThreshold <- function() {
  threshold <- 10 # Aiven allows 16 open connections, threshold is set to 10
  currentOpenConnections <- dbListConnections(MySQL())
  if (length(currentOpenConnections) > threshold) {
    for (conn in currentOpenConnections) {
      dbDisconnect(conn)
    }
  }
}

#*****************************************
#* Connect to MySQL Database
#*****************************************
connectToDatabase <- function() {
  dbName <- "defaultdb"
  dbUser <- "avnadmin"
  dbPassword <- "AVNS_-yK2PI98zqMdU4P-eud"
  dbHost <- "krishnappak-db-cs5200-dbms.d.aivencloud.com"
  dbPort <- 20057
  
  tryCatch({
    closeDbConnectionsOverThreshold()
    dbCon <- dbConnect(
      RMySQL::MySQL(),
      user = dbUser,
      password = dbPassword,
      dbname = dbName,
      host = dbHost,
      port = dbPort
    )
    cat("Connected to database successfully.\n")
    return(dbCon)
  }, error = function(err) {
    cat("Error connecting to database:", err$message, "\n")
    stop("Database connection failed.")
  })
}

#*****************************************
#* Print Line
#*****************************************
printLine <- function() {
  print("----------------------------------------")
}

#*****************************************
#* Load Data to a Dataframe
#*****************************************
loadDataToDataframe <- function() {
  print("Loading data from CSV...")
  df.orig <- read.csv("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv", 
                      header = TRUE, stringsAsFactors = FALSE)
  print("Data loaded successfully.")
  cat("Rows Loaded:", nrow(df.orig), "\n")
  printLine()
  return(df.orig)
}

#*****************************************
#* Clean the Dataframe
#* @param df.orig - original dataframe
#*****************************************
cleanTheDataframe <- function(df.orig) {
  print("Cleaning data...")
  # make the null values to 0 for numeric columns
  df.orig$FoodBill[is.na(df.orig$FoodBill)] <- 0
  df.orig$AlcoholBill[is.na(df.orig$AlcoholBill)] <- 0
  df.orig$TipAmount[is.na(df.orig$TipAmount)] <- 0
  
  # handle sentinel values
  df.orig$Restaurant[is.na(df.orig$Restaurant)] <- "Unknown"
  df.orig$CustomerName[is.na(df.orig$CustomerName)] <- "Unknown"
  df.orig$ServerName[is.na(df.orig$ServerName)] <- "Unknown"
  df.orig$ServerName[df.orig$ServerName == "N/A"] <- NA
  
  print("Data cleaned for testing.")
  printLine()
  return(df.orig)
}

#*****************************************
#* Test Counts of Unique Entities
#* @param dbCon - database connection
#* @param df - dataframe
#*****************************************
runTests <- function(dbCon, df) {
  
  # count unique entities in CSV
  uniqueRestaurantsCsv <- length(unique(df$Restaurant[df$Restaurant != ""]))
  uniqueCustomersCsv <- length(unique(df$CustomerName[df$CustomerName != ""]))
  uniqueServersCsv <- length(unique(df$ServerName[df$ServerName != ""]))
  uniqueVisitsCsv <- nrow(df)
  
  print("Querying data from DB...")
  
  # count unique entities in Restaurant table
  uniqueRestaurantsDb <- dbGetQuery(dbCon, "SELECT COUNT(*) FROM Restaurant;")[1, 1]
  
  # count unique entities in Customer table
  uniqueCustomersDb <- dbGetQuery(dbCon, "SELECT * FROM Customer;")
  # handle NULL values and default entries from Customer DB query
  uniqueCustomersDb <- uniqueCustomersDb[uniqueCustomersDb$customerName != "Unknown", ]
  uniqueCustomersDb <- uniqueCustomersDb[!is.na(uniqueCustomersDb$customerName), ]
  uniqueCustomersDb <- nrow(uniqueCustomersDb)
  
  # count unique entities in Server table
  # write sql query and get unique serverName from Server into a dataframe
  uniqueServersDb <- dbGetQuery(dbCon, "SELECT DISTINCT serverName FROM Server;")
  # handle default entries from Server DB query
  uniqueServersDb <- nrow(uniqueServersDb != "Unknown")
  
  # count unique entities in Visit table
  uniqueVisitsDb <- dbGetQuery(dbCon, "SELECT COUNT(*) FROM Visit")[1, 1]
  
  # sum amounts in CSV
  totalFoodBillCsv <- sum(df$FoodBill, na.rm = TRUE)
  totalAlcoholBillCsv <- sum(df$AlcoholBill, na.rm = TRUE)
  totalTipAmountCsv <- sum(df$TipAmount, na.rm = TRUE)
  
  # sum amounts in database
  totalFoodBillDb <- suppressWarnings(as.numeric(dbGetQuery(dbCon, "SELECT SUM(foodBill) FROM Billing")[1, 1]))
  totalAlcoholBillDb <- suppressWarnings(as.numeric(dbGetQuery(dbCon, "SELECT SUM(alcoholBill) FROM AlcoholBilling")[1, 1]))
  totalTipAmountDb <- suppressWarnings(as.numeric(dbGetQuery(dbCon, "SELECT SUM(tipAmount) FROM Billing")[1, 1]))
  
  # handle NULL values from DB queries
  totalFoodBillDb <- ifelse(is.na(totalFoodBillDb), 0, totalFoodBillDb)
  totalAlcoholBillDb <- ifelse(is.na(totalAlcoholBillDb), 0, totalAlcoholBillDb)
  totalTipAmountDb <- ifelse(is.na(totalTipAmountDb), 0, totalTipAmountDb)
  
  printLine()
  
  # run tests
  print("Running tests...")
  
  # test 1: test unique restaurants
  print("Number of unique restaurants matches")
  outputMsg <- paste("CSV:", uniqueRestaurantsCsv, "DB:", uniqueRestaurantsDb)
  test_that("Number of unique restaurants matches", {
    expect_equal(uniqueRestaurantsCsv, uniqueRestaurantsDb, info = outputMsg)
    print(outputMsg)
  })
  printLine()
  
  # test 2: test unique customers
  print("Number of unique customers matches")
  outputMsg <- paste("CSV:", uniqueCustomersCsv, "DB:", uniqueCustomersDb)
  test_that("Number of unique customers matches", {
    expect_equal(uniqueCustomersCsv, uniqueCustomersDb, info = outputMsg)
    print(outputMsg)
  })
  printLine()
  
  # test 3: test unique servers
  print("Number of unique servers matches")
  outputMsg <- paste("CSV:", uniqueServersCsv, "DB:", uniqueServersDb)
  test_that("Number of unique servers matches", {
    expect_equal(uniqueServersCsv, uniqueServersDb, info = outputMsg)
    print(outputMsg)
  })
  printLine()
  
  # test 4: test unique visits
  print("Number of unique visits matches")
  outputMsg <- paste("CSV:", uniqueVisitsCsv, "DB:", uniqueVisitsDb)
  test_that("Number of unique visits matches", {
    expect_equal(uniqueVisitsCsv, uniqueVisitsDb, info = outputMsg)
    print(outputMsg)
  })
  printLine()
  
  # test 5: test total food bill
  print("Total food bill matches")
  outputMsg <- paste("CSV:", totalFoodBillCsv, "DB:", totalFoodBillDb)
  test_that("Total food bill matches", {
    expect_equal(totalFoodBillCsv, totalFoodBillDb, tolerance = 0.01, info = outputMsg)
    print(outputMsg)
  })
  printLine()
  
  # test 6: test total alcohol bill
  print("Total alcohol bill matches")
  outputMsg <- paste("CSV:", totalAlcoholBillCsv, "DB:", totalAlcoholBillDb)
  test_that("Total alcohol bill matches", {
    expect_equal(totalAlcoholBillCsv, totalAlcoholBillDb, tolerance = 0.01, info = outputMsg)
    print(outputMsg)
  })
  printLine()
  
  # test 7: test total tip amount
  print("Total tip amount matches")
  outputMsg <- paste("CSV:", totalTipAmountCsv, "DB:", totalTipAmountDb)
  test_that("Total tip amount matches", {
    expect_equal(totalTipAmountCsv, totalTipAmountDb, tolerance = 0.01, info = outputMsg)
    print(outputMsg)
  })
  printLine()
  
  cat("All tests completed.\n")
}

#*****************************************
#* Main Method
#*****************************************
main <- function() {
  # required packages
  packages <- c("RMySQL", "DBI", "testthat")
  
  # install and load required packages
  installPackagesOnDemand(packages)
  loadRequiredPackages(packages)
  
  # connect to database
  dbCon <- connectToDatabase()
  
  # load data
  df.orig <- loadDataToDataframe()
  
  # clean data for testing
  df.clean <- cleanTheDataframe(df.orig)
  
  # run tests
  runTests(dbCon, df.clean)
  
  # disconnect from database
  dbDisconnect(dbCon)
}

# execute the script
main()