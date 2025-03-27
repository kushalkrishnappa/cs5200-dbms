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
  threshold <- 10 # Aiven allows 16 open connections, threshold is 10
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
  # TODO: Load the date from the URL
  df.orig <- read.csv(file = "test_data.csv", header = TRUE, stringsAsFactors = FALSE)
  return (df.orig)
}

#*****************************************
#* Clean the  Dataframe
#*****************************************
cleanTheDataframe <- function(df.orig) {
  
  # StartDateHired has missing values, fill them with sentinal value of 0000-00-00
  df.orig$StartDateHired[df.orig$StartDateHired == "0000-00-00"] <- NA
  
  # EndDateHired has missing values, fill them with sentinal value of 9999-99-99
  df.orig$EndDateHired[df.orig$EndDateHired == "9999-99-99"] <- NA
  
  # PartySize has missing values, fill them with 99
  df.orig$PartySize[is.na(df.orig$PartySize)] <- 99
  
  # VisitDate, ServerBirthDate, StartDateHired, and EndDateHired should be converted to proper date format.
  df.orig$VisitDate <- as.Date(df.orig$VisitDate, format = "%m/%d/%y")
  df.orig$ServerBirthDate <- as.Date(df.orig$ServerBirthDate, format = "%m/%d/%y")
  df.orig$StartDateHired <- as.Date(df.orig$StartDateHired, format = "%m/%d/%y")
  df.orig$EndDateHired <- as.Date(df.orig$EndDateHired, format = "%m/%d/%y")
  
  # make the orderedAlcohol column TRUE for yes and FALSE for no
  df.orig$orderedAlcohol <- ifelse(df.orig$orderedAlcohol == "yes", TRUE, FALSE)
  
  # WaitTime table with -ve values should be set to 0
  df.orig$WaitTime[df.orig$WaitTime < 0] <- 0
  
  return (df.orig)
}

#*****************************************
#* Insert Into Databse in Batches
#*****************************************
insertIntoDatabaseInBatches <- function(dbCon, batchSize, initialQuery, values) {
  numberOfBatches <- ceiling(length(values) / batchSize)
  for (i in 1:numberOfBatches) {
    startIndex <- (i - 1) * batchSize + 1
    endIndex <- min(i * batchSize, length(values))
    batchValues <- values[startIndex:endIndex]
    completeQuery <- paste(initialQuery, paste(batchValues, collapse = ","))
    dbSendQuery(dbCon, completeQuery)
  }
}

#*****************************************
#* Insert Into Restaurant Table
#*****************************************
insertIntoRestaurantTable <- function(dbCon, df.restaurant, batchSize, restaurantTableName) {
  restaurants <- unique(df.restaurant)
  print(restaurants)
  if (nrow(restaurants) > 0) {
    query <- sprintf("INSERT INTO %s (restaurantName) VALUES ", restaurantTableName)
    
    values <- apply(restaurants, 1, function(row) {
      sprintf("('%s')",
              # Replacing single quote with double quote to avoid SQL errors.
              gsub("'", "''", row["restaurantName"]))
    })

    insertIntoDatabaseInBatches(dbCon, batchSize, query, values)
  }
}

#*****************************************
#* Insert Into MealType Table
#*****************************************
insertIntoMealTypeTable <- function(dbCon, df.mealType, batchSize, mealTypeTableName) {
  mealTypes <- unique(df.mealType)  # Extract unique meal types
  print(mealTypes)  # Debugging: Print extracted values
  
  if (nrow(mealTypes) > 0) {
    query <- sprintf("INSERT INTO %s (mealType) VALUES ", mealTypeTableName)
    
    values <- apply(mealTypes, 1, function(row) {
      sprintf("('%s')",
              # Replacing single quote with double quote to avoid SQL errors.
              gsub("'", "''", row["mealType"]))
    })
    
    insertIntoDatabaseInBatches(dbCon, batchSize, query, values)
  }
}

#*****************************************
#* Insert Into PaymentMethod Table
#*****************************************
insertIntoPaymentMethodTable <- function(dbCon, df.paymentMethod, batchSize, paymentMethodTableName) {
  paymentMethods <- unique(df.paymentMethod)  # Extract unique payment methods
  print(paymentMethods)  # Debugging: Print extracted values
  
  if (nrow(paymentMethods) > 0) {
    query <- sprintf("INSERT INTO %s (paymentMethod) VALUES ", paymentMethodTableName)
    
    values <- apply(paymentMethods, 1, function(row) {
      sprintf("('%s')",
              # Replacing single quote with double quote to avoid SQL errors.
              gsub("'", "''", row["paymentMethod"]))
    })
    
    insertIntoDatabaseInBatches(dbCon, batchSize, query, values)
  }
}

#*****************************************
#* Insert Into ServerEmployment Table
#*****************************************
insertIntoServerEmploymentTable <- function(dbCon, df.serverEmployment, batchSize, serverEmploymentTableName) {
  serverEmployments <- unique(df.serverEmployment)  # Extract unique employment records
  print(serverEmployments)  # Debugging: Print extracted values
  
  if (nrow(serverEmployments) > 0) {
    query <- sprintf("INSERT INTO %s (serverEmpID, startDateHired, endDateHired, hourlyRate) VALUES ", serverEmploymentTableName)
    
    values <- apply(serverEmployments, 1, function(row) {
      sprintf("(%s, '%s', %s, %s)",
              row["serverEmpID"],
              gsub("'", "''", row["startDateHired"]),
              ifelse(is.na(row["endDateHired"]), "NULL", sprintf("'%s'", gsub("'", "''", row["endDateHired"]))),
              row["hourlyRate"])
    })
    
    insertIntoDatabaseInBatches(dbCon, batchSize, query, values)
  }
}

#*****************************************
#* Insert Into Server Table
#*****************************************
insertIntoServerTable <- function(dbCon, df.server, batchSize, serverTableName) {
  servers <- unique(df.server)  # Extract unique server records
  print(servers)  # Debugging: Print extracted values
  
  if (nrow(servers) > 0) {
    query <- sprintf("INSERT INTO %s (serverEmpID, serverName, serverBirthDate, serverTIN) VALUES ", serverTableName)
    
    values <- apply(servers, 1, function(row) {
      sprintf("(%s, '%s', %s, %s)",
              row["serverEmpID"],
              gsub("'", "''", row["serverName"]),
              ifelse(is.na(row["serverBirthDate"]) | row["serverBirthDate"] == "", "NULL", sprintf("'%s'", gsub("'", "''", row["serverBirthDate"]))),
              ifelse(is.na(row["serverTIN"]) | row["serverTIN"] == "", "NULL", sprintf("'%s'", gsub("'", "''", row["serverTIN"]))))
    })
    
    insertIntoDatabaseInBatches(dbCon, batchSize, query, values)
  }
}

#*****************************************
#* Insert Into Customer Table
#*****************************************
insertIntoCustomerTable <- function(dbCon, df.customer, batchSize, customerTableName) {
  customers <- unique(df.customer)  # Extract unique customer records
  print(customers)  # Debugging: Print extracted values
  
  if (nrow(customers) > 0) {
    query <- sprintf("INSERT INTO %s (customerName, customerPhoneNumber, customerEmail, loyaltyMember) VALUES ", customerTableName)
    
    values <- apply(customers, 1, function(row) {
      sprintf("('%s', %s, %s, %s)",
              gsub("'", "''", row["customerName"]),
              ifelse(is.na(row["customerPhoneNumber"]) | row["customerPhoneNumber"] == "", "NULL", sprintf("'%s'", gsub("'", "''", row["customerPhoneNumber"]))),
              ifelse(is.na(row["customerEmail"]) | row["customerEmail"] == "", "NULL", sprintf("'%s'", gsub("'", "''", row["customerEmail"]))),
              ifelse(is.na(row["loyaltyMember"]) | row["loyaltyMember"] == "", "FALSE", row["loyaltyMember"])
      )
    })
    
    insertIntoDatabaseInBatches(dbCon, batchSize, query, values)
  }
}

# Fetch inserted Customer IDs
getCustomerIDMapping <- function(dbCon) {
  dbGetQuery(dbCon, "SELECT customerId, customerName FROM Customer")
}

# Fetch inserted Restaurant IDs
getRestaurantIDMapping <- function(dbCon) {
  dbGetQuery(dbCon, "SELECT restaurantId, restaurantName FROM Restaurant")
}

# Fetch inserted Server IDs
getServerIDMapping <- function(dbCon) {
  dbGetQuery(dbCon, "SELECT serverEmpID, serverId FROM Server")
}

getMealTypeMapping <- function(dbCon) {
  dbGetQuery(dbCon, "SELECT mtId, mealType FROM MealType")
}


# Function to update df.clean with actual FK values
updateForeignKeyMappings <- function(dbCon, df.clean) {
  # Get mappings from DB
  customerMapping <- getCustomerIDMapping(dbCon)
  restaurantMapping <- getRestaurantIDMapping(dbCon)
  serverMapping <- getServerIDMapping(dbCon)
  mealTypeMapping <- getMealTypeMapping(dbCon)
  
  # Merge dataframes to replace names with IDs
  df.clean <- merge(df.clean, customerMapping, by.x = "CustomerName", by.y = "customerName", all.x = TRUE)
  df.clean <- merge(df.clean, restaurantMapping, by.x = "Restaurant", by.y = "restaurantName", all.x = TRUE)
  df.clean <- merge(df.clean, serverMapping, by.x = "ServerEmpID", by.y = "serverEmpID", all.x = TRUE)
  df.clean <- merge(df.clean, mealTypeMapping, by.x = "MealType", by.y = "mealType", all.x = TRUE)
  
  # Rename columns to match FK names
  colnames(df.clean)[colnames(df.clean) == "customerId"] <- "CustomerID"
  colnames(df.clean)[colnames(df.clean) == "restaurantId"] <- "RestaurantID"
  colnames(df.clean)[colnames(df.clean) == "serverId"] <- "ServerID"
  colnames(df.clean)[colnames(df.clean) == "mtId"] <- "MealTypeID"
  
  return(df.clean)
}

#*****************************************
#* Insert Into Visit Table
#*****************************************
insertIntoVisitTable <- function(dbCon, df.visit, batchSize, visitTableName) {
  visits <- unique(df.visit)  # Extract unique visit records
  print(visits)  # Debugging: Print extracted values
  
  if (nrow(visits) > 0) {
    query <- sprintf("INSERT INTO %s (customerId, restaurantId, visitDate, visitTime, mealType, partySize, genders, waitTime, serverId) VALUES ", visitTableName)
    
    values <- apply(visits, 1, function(row) {
      sprintf("(%s, %s, '%s', '%s', %s, %s, %s, %s, %s)",
              row["customerId"],
              row["restaurantId"],
              gsub("'", "''", row["visitDate"]),   
              gsub("'", "''", row["visitTime"]),   
              row["MealTypeID"],  # 🔹 Use MealTypeID instead of mealType string
              row["partySize"],
              ifelse(is.na(row["genders"]) | row["genders"] == "", "NULL", sprintf("'%s'", gsub("'", "''", row["genders"]))),
              ifelse(is.na(row["waitTime"]) | row["waitTime"] < 0, 0, row["waitTime"]),
              ifelse(is.na(row["serverId"]), "NULL", row["serverId"])  
      )
    })
    
    insertIntoDatabaseInBatches(dbCon, batchSize, query, values)
  }
}


#*****************************************
#* Insert Into Tables
#*****************************************
insertIntoTables <- function(dbCon, df.clean) {
  batchSize <- 10
  
  # get the restaurant column from the df.clean dataframe
  df.restaurant <- data.frame(restaurantName = df.clean[, c("Restaurant")], stringsAsFactors = FALSE)
  # insert into the restaurant table
  insertIntoRestaurantTable(dbCon, df.restaurant, batchSize, "Restaurant")
  
  # get the mealType column from the df.clean dataframe
  df.mealType <- data.frame(mealType = df.clean[, c("MealType")], stringsAsFactors = FALSE)
  # insert into the mealType table
  insertIntoMealTypeTable(dbCon, df.mealType, batchSize, "MealType")
  
  # get the paymentMethod column from the df.clean dataframe
  df.paymentMethod <- data.frame(paymentMethod = df.clean[, c("PaymentMethod")], stringsAsFactors = FALSE)
  # insert into the paymentMethod table
  insertIntoPaymentMethodTable(dbCon, df.paymentMethod, batchSize, "PaymentMethod")
  
  # get the serverEmployment column from the df.clean dataframe
  df.serverEmployment <- data.frame(serverEmpID = df.clean[, c("ServerEmpID")],
                                    startDateHired = df.clean[, c("StartDateHired")],
                                    endDateHired = df.clean[, c("EndDateHired")],
                                    hourlyRate = df.clean[, c("HourlyRate")],
                                    stringsAsFactors = FALSE)
  # remove the rows with ServerEmpID as NA
  df.serverEmployment <- df.serverEmployment[!is.na(df.serverEmployment$serverEmpID), ]
  # insert into the serverEmployment table
  insertIntoServerEmploymentTable(dbCon, df.serverEmployment, batchSize, "ServerEmployment")
  
  # get the server column from the df.clean dataframe
  df.server <- data.frame(serverEmpID = df.clean[, c("ServerEmpID")],
                          serverName = df.clean[, c("ServerName")],
                          serverBirthDate = df.clean[, c("ServerBirthDate")],
                          serverTIN = df.clean[, c("ServerTIN")],
                          stringsAsFactors = FALSE)
  # remove the rows with ServerEmpID as NA
  df.server <- df.server[!is.na(df.server$serverEmpID), ]
  # insert into the server table
  insertIntoServerTable(dbCon, df.server, batchSize, "Server")
  
  # get the customer column from the df.clean dataframe
  df.customer <- data.frame(customerName = df.clean[, c("CustomerName")],
                            customerPhoneNumber = df.clean[, c("CustomerPhone")],
                            customerEmail = df.clean[, c("CustomerEmail")],
                            loyaltyMember = df.clean[, c("LoyaltyMember")],
                            stringsAsFactors = FALSE)
  # insert into the customer table
  insertIntoCustomerTable(dbCon, df.customer, batchSize, "Customer")
  
  df.clean <- updateForeignKeyMappings(dbCon, df.clean)
  
  # get the visit column from the df.clean dataframe
  df.visit <- data.frame(customerId = df.clean[, c("CustomerID")],
                         restaurantId = df.clean[, c("RestaurantID")],
                         visitDate = df.clean[, c("VisitDate")],
                         visitTime = df.clean[, c("VisitTime")],
                         mealType = df.clean[, c("MealType")],
                         partySize = df.clean[, c("PartySize")],
                         genders = df.clean[, c("Genders")],
                         waitTime = df.clean[, c("WaitTime")],
                         serverId = df.clean[, c("ServerID")],
                         stringsAsFactors = FALSE)
  # insert into the visit table
  insertIntoVisitTable(dbCon, df.visit, batchSize, "Visit")
  
  # get all rows from restaurant table
  restaurantTable <- dbGetQuery(dbCon, "SELECT * FROM Restaurant")
  print(restaurantTable)
  # get all rows from mealType table
  mealTypeTable <- dbGetQuery(dbCon, "SELECT * FROM MealType")
  print(mealTypeTable)
  # get all rows from paymentMethod table
  paymentMethodTable <- dbGetQuery(dbCon, "SELECT * FROM PaymentMethod")
  print(paymentMethodTable)
  # get all rows from serverEmployment table
  serverEmploymentTable <- dbGetQuery(dbCon, "SELECT * FROM ServerEmployment")
  print(serverEmploymentTable)
  # get all rows from server table
  serverTable <- dbGetQuery(dbCon, "SELECT * FROM Server")
  print(serverTable)
  # get all rows from customer table
  customerTable <- dbGetQuery(dbCon, "SELECT * FROM Customer")
  print(customerTable)
  # get all rows from visit table
  visitTable <- dbGetQuery(dbCon, "SELECT * FROM Visit")
  print(visitTable)
}


#*****************************************
#* Main Method
#*****************************************
main <- function() {
  # packages needed for R program to run
  packages <- c("RMySQL")
  # install the required packages
  installPackagesOnDemand(packages)
  # load required packages
  loadRequiredPackages(packages)
  
  # connect to the database
  dbCon <- connectToDatabase()
  
  # load data to a dataframe
  df.orig <- loadDataToDataframe()
  
  #print(df.orig)
  
  # clean the dataframe
  df.clean <- cleanTheDataframe(df.orig)
  
  #print(df.clean)
  
  # insert into the tables
  insertIntoTables(dbCon, df.clean)
  
  # disconnect from the database
  dbDisconnect(dbCon)
}

main()