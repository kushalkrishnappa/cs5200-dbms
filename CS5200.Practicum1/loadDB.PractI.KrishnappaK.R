#*****************************************
#* title: Part E / Populate Database
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
  # DB credentials
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
#* Load Data to a Dataframe
#*****************************************
loadDataToDataframe <- function() {
  df.orig <- read.csv("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv", 
                      header = TRUE, stringsAsFactors = FALSE)
  print("Data loaded successfully.")
  return (df.orig)
}

#*****************************************
#* Clean the Dataframe
#*****************************************
cleanTheDataframe <- function(df.orig) {
  # handle sentinel values
  df.orig$StartDateHired[df.orig$StartDateHired == "0000-00-00"] <- NA
  df.orig$EndDateHired[df.orig$EndDateHired == "9999-99-99"] <- NA
  df.orig$ServerName[df.orig$ServerName == "N/A"] <- NA
  df.orig$PartySize[df.orig$PartySize == 99] <- NA
  df.orig$WaitTime[df.orig$WaitTime < 0] <- 0
  
  # convert dates to proper format
  df.orig$ServerBirthDate <- as.Date(df.orig$ServerBirthDate, format = "%m/%d/%y")
  df.orig$StartDateHired <- as.Date(df.orig$StartDateHired, format = "%m/%d/%y")
  df.orig$EndDateHired <- as.Date(df.orig$EndDateHired, format = "%m/%d/%y")
  
  # convert orderedAlcohol to boolean
  df.orig$orderedAlcohol <- ifelse(tolower(df.orig$orderedAlcohol) == "yes", TRUE, FALSE)
  
  # convert LoyaltyMember to boolean
  df.orig$LoyaltyMember <- ifelse(tolower(df.orig$LoyaltyMember) == "true", 1, 0)
  
  print("Data cleaned successfully.")
  return(df.orig)
}

#*****************************************
#* Format Value Based on Data Type
#* @param value - value to format
#*****************************************
formatValue <- function(value) {
  if (is.na(value) || value == "") {
    "NULL"
  } else if (is.character(value)) {
    paste0("'", gsub("'", "''", value), "'")
  } else if (is.logical(value)) {
    toupper(as.character(value))
  } else {
    as.character(value)
  }
}

#*****************************************
#* Insert Into Database in Batches
#* @param dbCon - database connection
#* @param tableName - table name
#* @param columns - columns
#* @param data - data to insert
#* @param batchSize - batch size
#*****************************************
insertDataIntoTablesInBatches <- function(dbCon, tableName, columns, data, batchSize) {
  # number of batches to insert
  numberOfBatches <- ceiling(nrow(data) / batchSize)
  
  # insert data in batches
  for (i in 1:numberOfBatches) {
    startIndex <- (i - 1) * batchSize + 1
    endIndex <- min(i * batchSize, nrow(data))
    batchData <- data[startIndex:endIndex, , drop = FALSE]
    
    # create value string for each row of inserting batch
    values <- apply(batchData, 1, function(row) {
      rowValues <- sapply(columns, function(col) formatValue(row[[col]]))
      paste("(", paste(rowValues, collapse = ", "), ")")
    })
    valuesStr <- paste(values, collapse = ", ")
    
    # create INSERT query
    sqlQueryToInsert <- paste("INSERT INTO", tableName, "(", paste(columns, collapse = ", "), ") VALUES", valuesStr)

    dbExecute(dbCon, sqlQueryToInsert)
  }
}

#*****************************************
#* Insert Default Records
#* @param dbCon - database connection
#*****************************************
# insert default customer record
insertDefaultCustomer <- function(dbCon) {
  sqlForDefaultCustomer <- "
  INSERT INTO Customer (customerName, loyaltyMember) 
  VALUES ('Unknown', FALSE);"
  dbExecute(dbCon, sqlForDefaultCustomer)
  return(dbGetQuery(dbCon, "SELECT LAST_INSERT_ID();")[[1]])
}

# insert default server record
insertDefaultServer <- function(dbCon) {
  sqlForDefaultServer <- "
  INSERT INTO Server 
  (ServerEmpID, ServerName, StartDateHired, EndDateHired, HourlyRate, ServerBirthDate, ServerTIN) 
  VALUES ('0', 'Unknown', NULL, NULL, NULL, NULL, NULL);
  "
  dbExecute(dbCon, sqlForDefaultServer)
  return(dbGetQuery(dbCon, "SELECT LAST_INSERT_ID();")[[1]])
}

#*****************************************
#* Insert Into Lookup Tables
#*****************************************
insertIntoRestaurantTable <- function(dbCon, df, batchSize) {
  restaurants <- data.frame(restaurantName = unique(df$Restaurant[!is.na(df$Restaurant) & df$Restaurant != ""]))
  if (nrow(restaurants) > 0) {
    insertDataIntoTablesInBatches(dbCon, "Restaurant", c("restaurantName"), restaurants, batchSize)
  }
  print("Inserted into Restaurant")
}

insertIntoMealTypeTable <- function(dbCon, df, batchSize) {
  mealTypes <- data.frame(mealType = unique(df$MealType[!is.na(df$MealType) & df$MealType != ""]))
  if (nrow(mealTypes) > 0) {
    insertDataIntoTablesInBatches(dbCon, "MealType", c("mealType"), mealTypes, batchSize)
  }
  print("Inserted into MealType")
}

insertIntoPaymentMethodTable <- function(dbCon, df, batchSize) {
  paymentMethods <- data.frame(paymentMethod = unique(df$PaymentMethod[!is.na(df$PaymentMethod) & df$PaymentMethod != ""]))
  if (nrow(paymentMethods) > 0) {
    insertDataIntoTablesInBatches(dbCon, "PaymentMethod", c("paymentMethod"), paymentMethods, batchSize)
  }
  print("Inserted into PaymentMethod")
}

#*****************************************
#* Insert Into Customer Table
#*****************************************
insertIntoCustomerTable <- function(dbCon, df, defaultCustomerId, batchSize) {
  customers <- unique(df[, c("CustomerName", "CustomerPhone", "CustomerEmail", "LoyaltyMember")])
  customers <- customers[!is.na(customers$CustomerName) & customers$CustomerName != "", ]
  
  # rename columns to match database table schema
  columns <- c("customerName", "customerPhoneNumber", "customerEmail", "loyaltyMember")
  names(customers) <- columns
  if (nrow(customers) > 0) {
    insertDataIntoTablesInBatches(dbCon, "Customer", columns, customers, batchSize)
  }
  print("Inserted into Customer")
}

#*****************************************
#* Insert Into Server Table
#*****************************************
insertIntoServerTable <- function(dbCon, df, defaultServerId, batchSize) {
  # insert into Server table
  servers <- unique(df[, c("ServerEmpID", "ServerName", "ServerBirthDate", "ServerTIN", "StartDateHired", "EndDateHired", "HourlyRate")])
  servers <- servers[!is.na(servers$ServerEmpID) & servers$ServerEmpID != "", ]
  
  # rename columns to match database table schema
  columnsServer <- c("serverEmpID", "serverName", "serverBirthDate", "serverTIN", "startDateHired", "endDateHired", "hourlyRate")
  names(servers) <- columnsServer
  
  if (nrow(servers) > 0) {
    insertDataIntoTablesInBatches(dbCon, "Server", columnsServer, servers, batchSize)
  }
  print("Inserted into Server")
}

#*****************************************
#* Get ID Mapping from Lookup Tables
#*****************************************
getIdMapping <- function(dbCon, tableName, idColumn, nameColumn) {
  query <- sprintf("SELECT %s, %s FROM %s", idColumn, nameColumn, tableName)
  return(dbGetQuery(dbCon, query))
}

#*****************************************
#* Insert Into Visit Table
#*****************************************
insertIntoVisitTable <- function(dbCon, df, defaultCustomerId, defaultServerId, batchSize) {
  # get mappings for foreign keys from related tables
  restaurantMap <- getIdMapping(dbCon, "Restaurant", "restaurantId", "restaurantName")
  mealTypeMap <- getIdMapping(dbCon, "MealType", "mtId", "mealType")
  serverMap <- getIdMapping(dbCon, "Server", "serverId", "serverName")
  customerMap <- getIdMapping(dbCon, "Customer", "customerId", "customerName")
  
  # map foreign keys to the dataframe
  df$restaurantId <- restaurantMap$restaurantId[match(df$Restaurant, restaurantMap$restaurantName)]
  df$mtId <- mealTypeMap$mtId[match(df$MealType, mealTypeMap$mealType)]
  df$serverId <- serverMap$serverId[match(df$ServerName, serverMap$serverName)]
  df$customerId <- customerMap$customerId[match(df$CustomerName, customerMap$customerName)]
  
  # use default IDs for missing mappings
  df$customerId[is.na(df$customerId)] <- defaultCustomerId
  df$serverId[is.na(df$serverId)] <- defaultServerId
  
  # insert data into Visit table
  visitData <- df[, c("customerId", "restaurantId", "VisitDate", "VisitTime", "mtId", "PartySize", "Genders", "WaitTime", "serverId")]
  # rename columns to match database table schema
  columnsVisit <- c("customerId", "restaurantId", "visitDate", "visitTime", "mealType", "partySize", "genders", "waitTime", "serverId")
  names(visitData) <- columnsVisit
  
  # handle missing values
  visitData$partySize[is.na(visitData$partySize)] <- 1
  visitData$waitTime[is.na(visitData$waitTime)] <- 0
  
  # insert into Visit
  if (nrow(visitData) > 0) {
    insertDataIntoTablesInBatches(dbCon, "Visit", names(visitData), visitData, batchSize)
  }
  print("Inserted into Visit")
}

#*****************************************
#* Insert Into Billing and AlcoholBilling Tables
#*****************************************
insertIntoBillingTables <- function(dbCon, df, batchSize) {
  # get mappings for foreign keys from related tables
  paymentMethodMap <- getIdMapping(dbCon, "PaymentMethod", "pmId", "paymentMethod")
  visitMap <- getIdMapping(dbCon, "Visit", "visitId", "visitDate")
  
  # map foreign keys to the dataframe
  df$pmId <- paymentMethodMap$pmId[match(df$PaymentMethod, paymentMethodMap$paymentMethod)]
  df$visitId <- visitMap$visitId[match(df$VisitDate, visitMap$visitDate)]
  
  # insert data into Billing table
  billingData <- df[, c("visitId", "FoodBill", "TipAmount", "DiscountApplied", "orderedAlcohol", "pmId")]
  columnsBilling <- c("visitId", "foodBill", "tipAmount", "discountApplied", "orderedAlcohol", "paymentMethod")
  names(billingData) <- columnsBilling
  billingData$foodBill[is.na(billingData$foodBill)] <- 0
  billingData$tipAmount[is.na(billingData$tipAmount)] <- 0
  billingData$discountApplied[is.na(billingData$discountApplied)] <- 0
  if (nrow(billingData) > 0) {
    insertDataIntoTablesInBatches(dbCon, "Billing", names(billingData), billingData, batchSize)
  }
  
  # insert data into AlcoholBilling table
  billingMap <- dbGetQuery(dbCon, "SELECT billingId, visitId FROM Billing")
  df$billingId <- billingMap$billingId[match(df$visitId, billingMap$visitId)]
  
  alcoholData <- df[!is.na(df$billingId), c("AlcoholBill"), drop = FALSE]
  columnsAlcohol <- c("alcoholBill")
  names(alcoholData) <- columnsAlcohol
  alcoholData$alcoholBill[is.na(alcoholData$alcoholBill)] <- 0
  if (nrow(alcoholData) > 0) {
    insertDataIntoTablesInBatches(dbCon, "AlcoholBilling", names(alcoholData), alcoholData, batchSize)
  }
  print("Inserted into Billing")
  print("Inserted into AlcoholBilling")
}

#*****************************************
#* Main Method
#*****************************************
main <- function() {
  # required packages
  packages <- c("RMySQL", "DBI")
  
  # install and load required packages
  installPackagesOnDemand(packages)
  loadRequiredPackages(packages)
  
  # connect to database
  dbCon <- connectToDatabase()
  
  # insert default records into Customer, ServerEmployment, and Server tables
  defaultCustomerId <- insertDefaultCustomer(dbCon)
  defaultServerId <- insertDefaultServer(dbCon)
  
  # load data
  df.orig <- loadDataToDataframe()
  
  # clean data
  df.clean <- cleanTheDataframe(df.orig)
  
  # set batch size for inserts
  batchSize <- 1000
  
  # insert into lookup tables
  insertIntoRestaurantTable(dbCon, df.clean, batchSize)
  insertIntoMealTypeTable(dbCon, df.clean, batchSize)
  insertIntoPaymentMethodTable(dbCon, df.clean, batchSize)
  
  # insert into Customer and Server tables
  insertIntoCustomerTable(dbCon, df.clean, defaultCustomerId, batchSize)
  insertIntoServerTable(dbCon, df.clean, defaultServerId, batchSize)
  
  # insert into Visit table
  insertIntoVisitTable(dbCon, df.clean, defaultCustomerId, defaultServerId, batchSize)
  
  # insert into Billing and AlcoholBilling tables
  insertIntoBillingTables(dbCon, df.clean, batchSize)
  
  # disconnect from database
  dbDisconnect(dbCon)
  cat("Database population completed successfully.\n")
}

# execute the script
main()
