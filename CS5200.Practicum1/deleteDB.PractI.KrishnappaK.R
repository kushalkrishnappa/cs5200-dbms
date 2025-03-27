#*****************************************
#* title: Part D / Delete Database
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
  # db credentials
  dbName <- "defaultdb"
  dbUser <- "avnadmin"
  dbPassword <- "AVNS_-yK2PI98zqMdU4P-eud"
  dbHost <- "krishnappak-db-cs5200-dbms.d.aivencloud.com"
  dbPort <- 20057
  
  tryCatch({
    closeDbConnectionsOverThreshold() # Aiven allows 16 open connections
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
#* Drop All Tables
#* @param dbCon - database connection
#*****************************************
dropAllTables <- function(dbCon) {
  # define the drop order of the table based on the foreign key constraints
  tablesOrderToDrop <- c(
    "AlcoholBilling",
    "Billing",
    "Visit",
    "Server",
    "Customer",
    "Restaurant",
    "MealType",
    "PaymentMethod"
  )
  
  # drop tables in the order
  for (table in tablesOrderToDrop) {
    sqlQueryToDropTable <- paste("DROP TABLE IF EXISTS", table, ";")
    dbExecute(dbCon, sqlQueryToDropTable)
    cat("Dropped table:", table, "\n")
  }
}

#*****************************************
#* Main Method
#*****************************************
main <- function() {
  # required packages
  packages <- c("RMySQL")
  
  # install and load required packages
  installPackagesOnDemand(packages)
  loadRequiredPackages(packages)
  
  # connect to database
  dbCon <- connectToDatabase()
  
  # drop all tables
  dropAllTables(dbCon)
  
  # check if some tables remain
  remainingTables <- dbListTables(dbCon)
  if (length(remainingTables) > 0) {
    cat("Remaining tables:", paste(remainingTables, collapse = ", "), "\n")
  } else {
    cat("All tables dropped successfully.\n")
  }
  
  # disconnect from database
  dbDisconnect(dbCon)
}

# execute the script
main()
