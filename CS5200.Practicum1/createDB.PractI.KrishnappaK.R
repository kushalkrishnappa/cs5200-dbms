#*****************************************
#* title: Part C / Realize Database
#* author: Krishnappa, Kushal
#* date: Spring 2025
#*****************************************

#*****************************************
#* Install Required Packages
#* @param packages - list of packages
#*****************************************
installPackagesOnDemand <- function(packages) {
  # install the required packages if not present
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
#* Create Customer Table
#* @param dbCon - database connection
#*****************************************
createCustomerTable <- function(dbCon) {
  sqlQueryToCreateCustomerTable <- "
  CREATE TABLE Customer (
    customerId INTEGER PRIMARY KEY AUTO_INCREMENT,
    customerName VARCHAR(255),
    customerPhoneNumber VARCHAR(20),
    customerEmail VARCHAR(255),
    loyaltyMember BOOLEAN DEFAULT FALSE
  );
  "
  dbExecute(dbCon, sqlQueryToCreateCustomerTable)
}

#*****************************************
#* Create Restaurant Table
#* @param dbCon - database connection
#*****************************************
createRestaurantTable <- function(dbCon) {
  sqlQueryToCreateRestaurantTable <- "
  CREATE TABLE Restaurant (
    restaurantId INTEGER PRIMARY KEY AUTO_INCREMENT,
    restaurantName VARCHAR(255) NOT NULL
  );
  "
  dbExecute(dbCon, sqlQueryToCreateRestaurantTable)
}

#*****************************************
#* Create MealType Table
#* @param dbCon - database connection
#*****************************************
createMealTypeTable <- function(dbCon) {
  sqlQueryToCreateMealTypeTable <- "
  CREATE TABLE MealType (
    mtId INTEGER PRIMARY KEY AUTO_INCREMENT,
    mealType VARCHAR(50) NOT NULL
  );
  "
  dbExecute(dbCon, sqlQueryToCreateMealTypeTable)
}

#*****************************************
#* Create Server Table
#* @param dbCon - database connection
#*****************************************
createServerTable <- function(dbCon) {
  sqlQueryToCreateServerTable <- "
  CREATE TABLE Server (
    serverId INTEGER PRIMARY KEY AUTO_INCREMENT,
    serverEmpID VARCHAR(255) NOT NULL,
    serverName VARCHAR(255),
    serverBirthDate DATE,
    serverTIN VARCHAR(20),
    startDateHired DATE,
    endDateHired DATE,
    hourlyRate NUMERIC(19,4)
  );
  "
  dbExecute(dbCon, sqlQueryToCreateServerTable)
}

#*****************************************
#* Create Visit Table
#* @param dbCon - database connection
#*****************************************
createVisitTable <- function(dbCon) {
  sqlQueryToCreateVisitTable <- "
  CREATE TABLE Visit (
    visitId INTEGER PRIMARY KEY AUTO_INCREMENT,
    customerId INTEGER NOT NULL,
    restaurantId INTEGER NOT NULL,
    visitDate DATE NOT NULL,
    visitTime TIME,
    mealType INTEGER NOT NULL,
    partySize INTEGER NOT NULL,
    genders VARCHAR(99),
    waitTime INTEGER NOT NULL CHECK (waitTime >= 0),
    serverId INTEGER NOT NULL,
    FOREIGN KEY (customerId) REFERENCES Customer(customerId),
    FOREIGN KEY (restaurantId) REFERENCES Restaurant(restaurantId),
    FOREIGN KEY (mealType) REFERENCES MealType(mtId),
    FOREIGN KEY (serverId) REFERENCES Server(serverId)
  );
  "
  dbExecute(dbCon, sqlQueryToCreateVisitTable)
}

#*****************************************
#* Create PaymentMethod Table
#* @param dbCon - database connection
#*****************************************
createPaymentMethodTable <- function(dbCon) {
  sqlQueryToCreatePaymentMethodTable <- "
  CREATE TABLE PaymentMethod (
    pmId INT PRIMARY KEY AUTO_INCREMENT,
    paymentMethod VARCHAR(50) NOT NULL
  );
  "
  dbExecute(dbCon, sqlQueryToCreatePaymentMethodTable)
}

#*****************************************
#* Create Billing Table
#* @param dbCon - database connection
#*****************************************
createBillingTable <- function(dbCon) {
  sqlQueryToCreateBillingTable <- "
  CREATE TABLE Billing (
    billingId INTEGER PRIMARY KEY AUTO_INCREMENT,
    visitID INTEGER NOT NULL,
    foodBill NUMERIC(19,4) NOT NULL,
    tipAmount NUMERIC(19,4) NOT NULL,
    discountApplied NUMERIC(19,4) NOT NULL,
    orderedAlcohol BOOLEAN NOT NULL,
    paymentMethod INTEGER NOT NULL,
    FOREIGN KEY (visitID) REFERENCES Visit(visitID),
    FOREIGN KEY (paymentMethod) REFERENCES PaymentMethod(pmId)
  );
  "
  dbExecute(dbCon, sqlQueryToCreateBillingTable)
}

#*****************************************
#* Create AlcoholBilling Table
#* @param dbCon - database connection
#*****************************************
createAlcoholBillingTable <- function(dbCon) {
  sqlQueryToCreateAlcoholBillingTable <- "
  CREATE TABLE AlcoholBilling (
    billingId INTEGER PRIMARY KEY AUTO_INCREMENT,
    alcoholBill NUMERIC(19,4) NOT NULL,
    FOREIGN KEY (billingId) REFERENCES Billing(billingId)
  )
  "
  dbExecute(dbCon, sqlQueryToCreateAlcoholBillingTable)
}

#*****************************************
#* Create All Tables in MySQL Database
#*****************************************
createTables <- function(dbCon) {
  createCustomerTable(dbCon)
  createRestaurantTable(dbCon)
  createMealTypeTable(dbCon)
  createServerTable(dbCon)
  createVisitTable(dbCon)
  createPaymentMethodTable(dbCon)
  createBillingTable(dbCon)
  createAlcoholBillingTable(dbCon)
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
  
  # create tables
  createTables(dbCon)
  
  # print created tables
  tablesCreated <- dbListTables(dbCon)
  cat("Tables created:", paste(tablesCreated, collapse = ", "), "\n")
  
  # disconnect from the database
  dbDisconnect(dbCon)
}

# execute the script
main()
