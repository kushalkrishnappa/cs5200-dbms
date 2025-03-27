#*****************************************
#* title: Part H / Add Business Logic
#* author: Krishnappa, Kushal
#* date: Spring 2025
#*****************************************

# turn off warn safely for a script
# Reference - https://stackoverflow.com/questions/16194212/how-to-suppress-warnings-globally-in-an-r-script
options(warn = -1) 

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
#* Print Line
#*****************************************
printLine <- function() {
  cat("\n----------------------------------------", "\n")
}

#*****************************************
#* Create Store Visit Stored Procedure
#* @param dbCon - database connection
#*****************************************
createStoreVisitProcedure <- function(dbCon) {
  # drop the storeVisit stored procedure if it exists
  dbSendQuery(dbCon, "DROP PROCEDURE IF EXISTS storeVisit;")
  
  # sql query for `storeVisit` stored procedure
  queryToCreateStoreVisitProcedure <- "
   CREATE PROCEDURE storeVisit(
      IN p_restaurantId INT,
      IN p_customerId INT,
      IN p_visitDate DATE,
      IN p_mealType INT,
      IN p_partySize INT,
      IN p_waitTime INT,
      IN p_foodBill DECIMAL(10,2),
      IN p_alcoholBill DECIMAL(10,2),
      IN p_tipAmount DECIMAL(10,2),
      IN p_discountApplied DECIMAL(10,2),
      IN p_orderedAlchol BOOLEAN,
      IN p_paymentMethod INT,
      IN p_serverId INT
   )
   
   BEGIN
      DECLARE v_visitId INT;
      
      INSERT INTO Visit (restaurantId, customerId, visitDate, mealType, partySize, waitTime, serverId)
      VALUES (p_restaurantId, p_customerId, p_visitDate, p_mealType, p_partySize, p_waitTime, p_serverId);
      
      SET v_visitId = LAST_INSERT_ID();
      
      INSERT INTO Billing (visitId, foodBill, tipAmount, discountApplied, orderedAlcohol, paymentMethod) 
      VALUES (v_visitId, p_foodBill, p_tipAmount, p_discountApplied, p_orderedAlchol, p_paymentMethod);
      
      IF p_alcoholBill > 0 THEN
          INSERT INTO AlcoholBilling (billingId, alcoholBill) VALUES (LAST_INSERT_ID(), p_alcoholBill);
      END IF;
   END
  "
  tryCatch({
    suppressWarnings(dbSendQuery(dbCon, queryToCreateStoreVisitProcedure))
    cat("storeVisit stored procedure created successfully.", "\n")
  }, error = function(err) {
    stop("Error creating storeVisit stored procedure:", err$message, "\n")
  })
}

#*****************************************
#* Create Store New Visit Stored Procedure
#* @param dbCon - database connection
#*****************************************
createStoreNewVisitProcedure <- function(dbCon) {
  # drop the storeNewVisit stored procedure if it exists
  dbSendQuery(dbCon, "DROP PROCEDURE IF EXISTS storeNewVisit;")
  
  # sql query for `storeNewVisit` stored procedure
  queryToCreateStoreNewVisitProcedure <- "
   CREATE PROCEDURE storeNewVisit(
      IN p_restaurantName VARCHAR(255),
      IN p_customerName VARCHAR(255),
      IN p_customerPhoneNumber VARCHAR(14),
      IN p_customerEmail VARCHAR(255),
      IN p_loyaltyMember BOOLEAN,
      IN p_serverName VARCHAR(255),
      IN p_serverEmpID INT,
      IN p_visitDate DATE,
      IN p_mealType INT,
      IN p_partySize INT,
      IN p_waitTime INT,
      IN p_foodBill DECIMAL(10,2),
      IN p_alcoholBill DECIMAL(10,2),
      IN p_tipAmount DECIMAL(10,2),
      IN p_discountApplied DECIMAL(10,2),
      IN p_orderedAlchol BOOLEAN,
      IN p_paymentMethod INT
   )
   BEGIN
      DECLARE v_restaurantId INT;
      DECLARE v_customerId INT;
      DECLARE v_serverId INT;
      DECLARE v_visitId INT;
      DECLARE v_billingId INT;
      
      SELECT restaurantId INTO v_restaurantId FROM Restaurant WHERE restaurantName = p_restaurantName;
      IF v_restaurantId IS NULL THEN
          INSERT INTO Restaurant (restaurantName) VALUES (p_restaurantName);
          SET v_restaurantId = LAST_INSERT_ID();
      END IF;
      
      SELECT customerId INTO v_customerId FROM Customer 
      WHERE customerName = p_customerName AND customerPhoneNumber = p_customerPhoneNumber AND customerEmail = p_customerEmail;
      IF v_customerId IS NULL THEN
          INSERT INTO Customer (customerName, customerPhoneNumber, customerEmail, loyaltyMember) 
          VALUES (p_customerName, p_customerPhoneNumber, p_customerEmail, p_loyaltyMember);
          SET v_customerId = LAST_INSERT_ID();
      END IF;
      
      SELECT serverId INTO v_serverId FROM Server WHERE serverName = p_serverName;
      IF v_serverId IS NULL THEN
          INSERT INTO Server (serverEmpID, serverName) VALUES (p_serverEmpID, p_serverName);
          SET v_serverId = LAST_INSERT_ID();
      END IF;
      
      INSERT INTO Visit (restaurantId, customerId, visitDate, mealType, partySize, waitTime, serverId)
      VALUES (v_restaurantId, v_customerId, p_visitDate, p_mealType, p_partySize, p_waitTime, v_serverId);
      
      SET v_visitId = LAST_INSERT_ID();

      INSERT INTO Billing (visitId, foodBill, tipAmount, discountApplied, orderedAlcohol, paymentMethod) 
      VALUES (v_visitId, p_foodBill, p_tipAmount, p_discountApplied, p_orderedAlchol, p_paymentMethod);
      
      IF p_alcoholBill > 0 THEN
          INSERT INTO AlcoholBilling (billingId, alcoholBill) VALUES (LAST_INSERT_ID(), p_alcoholBill);
      END IF;
   END
  "
  tryCatch({
    suppressWarnings(dbSendQuery(dbCon, queryToCreateStoreNewVisitProcedure))
    cat("storeNewVisit stored procedure created successfully.", "\n")
  }, error = function(err) {
    stop("Error creating storeNewVisit stored procedure:", err$message, "\n")
  })
}

#*****************************************
#* Trigger Store Visit Stored Procedure
#* Reference: YouTube - Stored Procedures in MySQL by Alex the Analyst
#* @param dbCon - database connection
#* @param restaurantId - restaurant id
#* @param customerId - customer id
#* @param visitDate - visit date
#* @param partySize - party size
#* @param foodBill - food bill
#* @param alcoholBill - alcohol bill
#* @param tipAmount - tip amount
#* @param discountApplied - discount applied
#* @param orderedAlchol - ordered alcohol
#* @param paymentMethod - payment method
#* @param serverId - server id
#*****************************************
callStoreVisit <- function(dbCon, restaurantId, customerId, visitDate, 
                           mealType, partySize, waitTime, foodBill, 
                           alcoholBill, tipAmount, discountApplied,
                           orderedAlchol, paymentMethod, serverId) {
  # trigger the storeVisit stored procedure
  queryStoreVisit <- paste0("CALL storeVisit(", restaurantId, ", ", 
                            customerId, ", '", visitDate, "', ", 
                            mealType, ", ", partySize, ", ", 
                            waitTime, ", ", foodBill, ", ",
                            alcoholBill, ", ", tipAmount, ", ", 
                            discountApplied, ", ", orderedAlchol, ", ", 
                            paymentMethod, ", ", serverId, ");")
  tryCatch({
    cat("Query to trigger storeVisit stored procedure:", queryStoreVisit, "\n")
    suppressWarnings(dbSendQuery(dbCon, queryStoreVisit))
    print("Visit successfully added to the database.")
    printLine()
  }, error = function(err) {
    stop("Error in storeVisit stored procedure - failed inserting visit: ", err$message, "\n")
  })
}

#*****************************************
#* Trigger Store New Visit Stored Procedure
#* @param dbCon - database connection
#* @param restaurantName - new restaurant name not in db
#* @param customerName - new customer name not in b
#* @param customerPhoneNumber - customer phone number
#* @param customerEmail - customer email
#* @param loyaltyMember - loyalty member
#* @param serverName - new server name not in db
#* @param serverEmpID - server employee id
#* @param visitDate - visit date
#* @param mealType - meal type
#* @param partySize - party size
#* @param waitTime - wait time
#* @param foodBill - food bill
#* @param alcoholBill - alcohol bill
#* @param tipAmount - tip amount
#* @param discountApplied - discount applied
#* @param orderedAlchol - ordered alcohol
#* @param paymentMethod - payment method
#*****************************************
callStoreNewVisit <- function(dbCon, restaurantName, customerName, customerPhoneNumber, customerEmail, 
                              loyaltyMember, serverName, serverEmpID, visitDate, mealType, partySize, 
                              waitTime, foodBill, alcoholBill, tipAmount, discountApplied, 
                              orderedAlchol, paymentMethod) {
  # trigger the storeNewVisit stored procedure
  queryStoreNewVisit <- paste0("CALL storeNewVisit('", restaurantName, "', '", 
                               customerName, "', '", customerPhoneNumber, "', '", 
                               customerEmail, "', ", loyaltyMember, ", '", 
                               serverName, "', ", serverEmpID, ", '", 
                               visitDate, "', ", mealType, ", ", 
                               partySize, ", ", waitTime, ", ", 
                               foodBill, ", ", alcoholBill, ", ", 
                               tipAmount, ", ", discountApplied, ", ", 
                               orderedAlchol, ", ", paymentMethod, ");")
  tryCatch({
    cat("Query to trigger storeNewVisit stored procedure:", queryStoreNewVisit, "\n")
    suppressWarnings(dbSendQuery(dbCon, queryStoreNewVisit))
    print("New visit successfully added to the database.")
    printLine()
  }, error = function(err) {
    stop("Error in storeNewVisit stored procedure - failed inserting new visit: ", err$message, "\n")
  })
}

#*****************************************
#* Verify Visit Insertion for Store Visit
#*****************************************
verifyVisitInsertionForStoreVisit <- function(dbCon, expectedRestaurantId, expectedCustomerId, expectedVisitDate, 
                                 expectedMealType, expectedPartySize, expectedWaitTime, expectedServerId) {
  queryToGetLastVisit <- "SELECT * FROM Visit ORDER BY visitId DESC LIMIT 1;"
  result <- dbGetQuery(dbCon, queryToGetLastVisit)
  cat("Last inserted row in Visit table:", "\n")
  print(result)
  printLine()
  
  # expected values from the db
  expectedRestaurantId <- as.numeric(expectedRestaurantId)
  expectedCustomerId <- as.numeric(expectedCustomerId)
  expectedMealType <- as.numeric(expectedMealType)
  expectedPartySize <- as.numeric(expectedPartySize)
  expectedWaitTime <- as.numeric(expectedWaitTime)
  expectedServerId <- as.numeric(expectedServerId)
  
  print("Testing inserted visit details")
  
  print("Restaurant ID matches")
  test_that("Restaurant ID matches", {
    outputMsg <- paste("Expected:", expectedRestaurantId, "| DB:", result$restaurantId)
    expect_equal(result$restaurantId, expectedRestaurantId, info = outputMsg)
    print(outputMsg)
  })
  
  print("Customer ID matches")
  test_that("Customer ID matches", {
    outputMsg <- paste("Expected:", expectedCustomerId, "| DB:", result$customerId)
    expect_equal(result$customerId, expectedCustomerId, info = outputMsg)
    print(outputMsg)
  })
  
  print("Visit Date matches")
  test_that("Visit Date matches", {
    outputMsg <- paste("Expected:", expectedVisitDate, "| DB:", result$visitDate)
    expect_equal(result$visitDate, expectedVisitDate, info = outputMsg)
    print(outputMsg)
  })
  
  print("Meal Type matches")
  test_that("Meal Type matches", {
    outputMsg <- paste("Expected:", expectedMealType, "| DB:", result$mealType)
    expect_equal(result$mealType, expectedMealType, info = outputMsg)
    print(outputMsg)
  })
  
  print("Party Size matches")
  test_that("Party Size matches", {
    outputMsg <- paste("Expected:", expectedPartySize, "| DB:", result$partySize)
    expect_equal(result$partySize, expectedPartySize, info = outputMsg)
    print(outputMsg)
  })
  
  print("Wait Time matches")
  test_that("Wait Time matches", {
    outputMsg <- paste("Expected:", expectedWaitTime, "| DB:", result$waitTime)
    expect_equal(result$waitTime, expectedWaitTime, info = outputMsg)
    print(outputMsg)
  })
  
  print("Server ID matches")
  test_that("Server ID matches", {
    outputMsg <- paste("Expected:", expectedServerId, "| DB:", result$serverId)
    expect_equal(result$serverId, expectedServerId, info = outputMsg)
    print(outputMsg)
  })
}

#*****************************************
#* Verify Visit Insertion for Store New Visit
#* @param dbCon - database connection
#* @param expectedRestaurantName - expected restaurant name
#* @param expectedCustomerName - expected customer name
#* @param expectedCustomerPhoneNumber - expected customer phone number
#* @param expectedCustomerEmail - expected customer email
#* @param expectedLoyaltyMember - expected loyalty member
#* @param expectedServerName - expected server name
#* @param expectedServerEmpID - expected server employee id
#* @param expectedVisitDate - expected visit date
#* @param expectedMealType - expected meal type
#* @param expectedPartySize - expected party size
#* @param expectedWaitTime - expected wait time
#* @param expectedFoodBill - expected food bill
#* @param expectedAlcoholBill - expected alcohol bill
#* @param expectedTipAmount - expected tip amount
#* @param expectedDiscountApplied - expected discount applied
#* @param expectedOrderedAlchol - expected ordered alcohol
#* @param expectedPaymentMethod - expected payment method
#*****************************************
verifyStoreNewVisitInsertion <- function(dbCon, expectedRestaurantName, expectedCustomerName, expectedCustomerPhoneNumber, expectedCustomerEmail, 
                                        expectedLoyaltyMember, expectedServerName, expectedServerEmpID, expectedVisitDate, expectedMealType, expectedPartySize, 
                                        expectedWaitTime, expectedFoodBill, expectedAlcoholBill, expectedTipAmount, expectedDiscountApplied, 
                                        expectedOrderedAlchol, expectedPaymentMethod) {
  queryToGetLastVisit <- "
  SELECT *
  FROM Visit v
  JOIN Restaurant r ON v.restaurantId = r.restaurantId
  JOIN Customer c ON v.customerId = c.customerId
  JOIN Server s ON v.serverId = s.serverId
  JOIN Billing b ON v.visitId = b.visitId
  LEFT JOIN AlcoholBilling ab ON b.billingId = ab.billingId
  JOIN PaymentMethod pm ON b.paymentMethod = pm.pmId
  ORDER BY v.visitId DESC
  LIMIT 1;
  "
  visitResult <- dbGetQuery(dbCon, queryToGetLastVisit)
  cat("Last inserted row in Visit table:", "\n")
  print(visitResult)
  printLine()
  
  # expected values from the db
  expectedLoyaltyMember <- as.numeric(expectedLoyaltyMember)
  expectedMealType <- as.numeric(expectedMealType)
  expectedPartySize <- as.numeric(expectedPartySize)
  expectedWaitTime <- as.numeric(expectedWaitTime)
  expectedFoodBill <- as.numeric(expectedFoodBill)
  expectedAlcoholBill <- as.numeric(expectedAlcoholBill)
  expectedTipAmount <- as.numeric(expectedTipAmount)
  expectedDiscountApplied <- as.numeric(expectedDiscountApplied)
  expectedOrderedAlchol <- as.numeric(expectedOrderedAlchol)
  expectedPaymentMethod <- as.numeric(expectedPaymentMethod)
  
  print("Testing inserted visit details")
  
  print("Restaurant Name matches")
  test_that("Restaurant Name matches", {
    outputMsg <- paste("Expected:", expectedRestaurantName, "| DB:", visitResult$restaurantName)
    expect_equal(visitResult$restaurantName, expectedRestaurantName, info = outputMsg)
    print(outputMsg)
  })
  
  print("Customer Name matches")
  test_that("Customer Name matches", {
    outputMsg <- paste("Expected:", expectedCustomerName, "| DB:", visitResult$customerName)
    expect_equal(visitResult$customerName, expectedCustomerName, info = outputMsg)
    print(outputMsg)
  })
  
  print("Customer Phone Number matches")
  test_that("Customer Phone Number matches", {
    outputMsg <- paste("Expected:", expectedCustomerPhoneNumber, "| DB:", visitResult$customerPhoneNumber)
    expect_equal(visitResult$customerPhoneNumber, expectedCustomerPhoneNumber, info = outputMsg)
    print(outputMsg)
  })
  
  print("Customer Email matches")
  test_that("Customer Email matches", {
    outputMsg <- paste("Expected:", expectedCustomerEmail, "| DB:", visitResult$customerEmail)
    expect_equal(visitResult$customerEmail, expectedCustomerEmail, info = outputMsg)
    print(outputMsg)
  })
  
  print("Loyalty Member matches")
  test_that("Loyalty Member matches", {
    outputMsg <- paste("Expected:", expectedLoyaltyMember, "| DB:", visitResult$loyaltyMember)
    expect_equal(visitResult$loyaltyMember, expectedLoyaltyMember, info = outputMsg)
    print(outputMsg)
  })
  
  print("Server Name matches")
  test_that("Server Name matches", {
    outputMsg <- paste("Expected:", expectedServerName, "| DB:", visitResult$serverName)
    expect_equal(visitResult$serverName, expectedServerName, info = outputMsg)
    print(outputMsg)
  })
  
  print("Server Emp ID matches")
  test_that("Server Emp ID matches", {
    outputMsg <- paste("Expected:", expectedServerEmpID, "| DB:", visitResult$serverEmpID)
    expect_equal(visitResult$serverEmpID, expectedServerEmpID, info = outputMsg)
    print(outputMsg)
  })
  
  print("Visit Date matches")
  test_that("Visit Date matches", {
    outputMsg <- paste("Expected:", expectedVisitDate, "| DB:", visitResult$visitDate)
    expect_equal(visitResult$visitDate, expectedVisitDate, info = outputMsg)
    print(outputMsg)
  })
  
  print("Meal Type matches")
  test_that("Meal Type matches", {
    outputMsg <- paste("Expected:", expectedMealType, "| DB:", visitResult$mealType)
    expect_equal(visitResult$mealType, expectedMealType, info = outputMsg)
    print(outputMsg)
  })
  
  print("Party Size matches")
  test_that("Party Size matches", {
    outputMsg <- paste("Expected:", expectedPartySize, "| DB:", visitResult$partySize)
    expect_equal(visitResult$partySize, expectedPartySize, info = outputMsg)
    print(outputMsg)
  })
  
  print("Wait Time matches")
  test_that("Wait Time matches", {
    outputMsg <- paste("Expected:", expectedWaitTime, "| DB:", visitResult$waitTime)
    expect_equal(visitResult$waitTime, expectedWaitTime, info = outputMsg)
    print(outputMsg)
  })
  
  print("Food Bill matches")
  test_that("Food Bill matches", {
    outputMsg <- paste("Expected:", expectedFoodBill, "| DB:", visitResult$foodBill)
    expect_equal(visitResult$foodBill, expectedFoodBill, info = outputMsg)
    print(outputMsg)
  })
  
  print("Alcohol Bill matches")
  test_that("Alcohol Bill matches", {
    outputMsg <- paste("Expected:", expectedAlcoholBill, "| DB:", visitResult$alcoholBill)
    expect_equal(visitResult$alcoholBill, expectedAlcoholBill, info = outputMsg)
    print(outputMsg)
  })
  
  print("Tip Amount matches")
  test_that("Tip Amount matches", {
    outputMsg <- paste("Expected:", expectedTipAmount, "| DB:", visitResult$tipAmount)
    expect_equal(visitResult$tipAmount, expectedTipAmount, info = outputMsg)
    print(outputMsg)
  })
  
  print("Discount Applied matches")
  test_that("Discount Applied matches", {
    outputMsg <- paste("Expected:", expectedDiscountApplied, "| DB:", visitResult$discountApplied)
    expect_equal(visitResult$discountApplied, expectedDiscountApplied, info = outputMsg)
    print(outputMsg)
  })
  
  print("Ordered Alcohol matches")
  test_that("Ordered Alcohol matches", {
    outputMsg <- paste("Expected:", expectedOrderedAlchol, "| DB:", visitResult$orderedAlcohol)
    expect_equal(visitResult$orderedAlcohol, expectedOrderedAlchol, info = outputMsg)
    print(outputMsg)
  })
  
  print("Payment Method matches")
  test_that("Payment Method matches", {
    outputMsg <- paste("Expected:", expectedPaymentMethod, "| DB:", visitResult$paymentMethod)
    expect_equal(visitResult$paymentMethod, expectedPaymentMethod, info = outputMsg)
    print(outputMsg)
  })
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
  
  # create `storeVisit` and `storeNewVisit` stored procedures
  createStoreVisitProcedure(dbCon)
  createStoreNewVisitProcedure(dbCon)
  
  printLine()
  
  # call storeVisit stored procedure
  callStoreVisit(dbCon = dbCon, 
                 restaurantId = 7, 
                 customerId = 15, 
                 visitDate = "2025-04-11", 
                 mealType = 3,
                 partySize = 2,
                 waitTime = 10,
                 foodBill = 50.00,
                 alcoholBill = 33.00,
                 tipAmount = 10.00,
                 discountApplied = 5.00,
                 orderedAlchol = TRUE,
                 paymentMethod = 1,
                 serverId = 1)
  
  # verify visit insertion
  verifyVisitInsertionForStoreVisit(dbCon, 
                       expectedRestaurantId = 7,
                       expectedCustomerId = 15,
                       expectedVisitDate = "2025-04-11",
                       expectedMealType = 3,
                       expectedPartySize = 2,
                       expectedWaitTime = 10,
                       expectedServerId = 1)
  
  printLine()
  
  # call storeNewVisit stored procedure
  callStoreNewVisit(dbCon = dbCon, 
                    restaurantName = "Smoor", 
                    customerName = "Samruddhi Basutkar", 
                    customerPhoneNumber = "123-789-7890", 
                    customerEmail = "sam@email.com",
                    loyaltyMember = TRUE,
                    serverName = "Kushal Krishnappa",
                    serverEmpID = "333",
                    visitDate = "2025-05-14",
                    mealType = 2,
                    partySize = 2,
                    waitTime = 15,
                    foodBill = 100.00,
                    alcoholBill = 100.00,
                    tipAmount = 10.00,
                    discountApplied = 5.00,
                    orderedAlchol = TRUE,
                    paymentMethod = 2)
  
  # verify visit insertion
  verifyStoreNewVisitInsertion(dbCon = dbCon, 
                               expectedRestaurantName = "Smoor", 
                               expectedCustomerName = "Samruddhi Basutkar", 
                               expectedCustomerPhoneNumber = "123-789-7890", 
                               expectedCustomerEmail = "sam@email.com",
                               expectedLoyaltyMember = TRUE,
                               expectedServerName = "Kushal Krishnappa",
                               expectedServerEmpID = "333",
                               expectedVisitDate = "2025-05-14",
                               expectedMealType = 2,
                               expectedPartySize = 2,
                               expectedWaitTime = 15,
                               expectedFoodBill = 100.00,
                               expectedAlcoholBill = 100.00,
                               expectedTipAmount = 10.00,
                               expectedDiscountApplied = 5.00,
                               expectedOrderedAlchol = TRUE,
                               expectedPaymentMethod = 2)
  
  # disconnect from the database
  dbDisconnect(dbCon)
}

# execute the script
main()