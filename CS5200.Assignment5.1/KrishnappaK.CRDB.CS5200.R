#*****************************************
#* title: Implement a Relational Database
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
#* Drop Table If Exists
#* @param dbCon - database connection
#* @param tableName - name of the table to drop
#*****************************************
dropTableIfExists <- function(dbCon, tableName) {
  # drop the table if it exists
  sqlQueryToDropTable <- paste0("DROP TABLE IF EXISTS ", tableName)
  dbExecute(dbCon, sqlQueryToDropTable)
}

#*****************************************
#* Drop All Existing Tables
#* @param dbCon - database connection
#*****************************************
dropAllExistingTables <- function(dbCon) {
  # get all the tables in the database
  tables <- dbListTables(dbCon)
  # drop all the tables
  for (table in tables) {
    dropTableIfExists(dbCon, table)
  }
}

############################################################################
# SQL DEFINITIONS FOR CREATING TABLES AND INSERTING DATA INTO TABLES
############################################################################

#*****************************************
#* Create "experience_level" Table If Not Exists
#* @param dbCon - database connection
#* Creates the following columns for experience_level table:
#* exp_id, integer as PK
#* level, varchar
#*****************************************
createExperienceLevelTable <- function(dbCon) {
  sqlForCreatingExperienceLevelTable = (
  'CREATE TABLE IF NOT EXISTS experience_level(
    exp_id INTEGER PRIMARY KEY,
    level VARCHAR NOT NULL
  );')
  dbExecute(dbCon, sqlForCreatingExperienceLevelTable)
}

#*****************************************
#* Inserting Data Into "experience_level" Table. This is a look up table and 
#* inserts the different experience levels.
#* @param dbCon - database connection
#*****************************************
insertDataIntoExperienceLevelTable <- function(dbCon) {
  sqlForInsertingDataIntoExperienceLevelTable = (
  'INSERT INTO experience_level (exp_id, level) VALUES
    (1, "beginner"),
    (2, "intermediate"),
    (3, "advanced");')
  dbExecute(dbCon, sqlForInsertingDataIntoExperienceLevelTable)
}

#*****************************************
#* Create "author" Table If Not Exists.
#* @param dbCon - database connection
#* Creates the following columns for author table:
#* aid, varchar as PK
#* name, varchar
#* perDiemRate, integer
#* exp_id, integer as FK
#*****************************************
createAuthorTable <- function(dbCon) {
  sqlForCreatingAuthorTable = (
  'CREATE TABLE IF NOT EXISTS author(
    aid VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    perDiemRate INTEGER NOT NULL,
    exp_id INTEGER NOT NULL,
    FOREIGN KEY (exp_id) REFERENCES experience_level(exp_id)
  );')
  dbExecute(dbCon, sqlForCreatingAuthorTable)
}

#*****************************************
#* Insert Data Into "author" Table.
#* @param dbCon - database connection
#*****************************************
insertDataIntoAuthorTable <- function(dbCon) {
  sqlForInsertingDataIntoAuthorTable = (
  'INSERT INTO author (aid, name, perDiemRate, exp_id) VALUES
    ("A1", "Kushal Krishnappa", 300, 3),
    ("A2", "Abhishek Rao", 200, 2),
    ("A3", "Pulasthya Reddy Siddareddy", 100, 1),
    ("A4", "Raghs", 250, 3);')
  dbExecute(dbCon, sqlForInsertingDataIntoAuthorTable)
}

#*****************************************
#* Create "module" Table If Not Exists.
#* @param dbCon - database connection
#*****************************************
createModuleTable <- function(dbCon) {
  sqlForCreatingModuleTable = (
  'CREATE TABLE IF NOT EXISTS module(
    subject VARCHAR NOT NULL,          
    number INTEGER NOT NULL,
    title VARCHAR NOT NULL,
    PRIMARY KEY (subject, number)
  );')
  dbExecute(dbCon, sqlForCreatingModuleTable)
}

#*****************************************
#* Insert Data Into "module" Table.
#* @param dbCon - database connection
#*****************************************
insertDataIntoModuleTable <- function(dbCon) {
  sqlForInsertingDataIntoModuleTable = (
  'INSERT INTO module (subject, number, title) VALUES
    ("CS5200", 1, "Database Management Systems"),
    ("CS5300", 2, "R Programming Language"),
    ("CS5400", 3, "Cloud Computing"),
    ("CS5500", 4, "Building Scalable Systems"),
    ("CS5600", 5, "Computer Networks"),
    ("CS5700", 6, "Computer Networks Security");')
  dbExecute(dbCon, sqlForInsertingDataIntoModuleTable)
}

#*****************************************
#* Create "module_prerequisite" Table If Not Exists.
#* @param dbCon - database connection
#*****************************************
createModulePrerequisiteTable <- function(dbCon) {
  sqlForCreatingModulePrerequisiteTable = (
  'CREATE TABLE IF NOT EXISTS module_prerequisite(
    subject VARCHAR NOT NULL,
    number INTEGER NOT NULL,
    prereq_subject VARCHAR NOT NULL,
    prereq_number INTEGER NOT NULL,
    PRIMARY KEY (subject, number, prereq_subject, prereq_number),
    FOREIGN KEY (subject, number) REFERENCES module(subject, number),
    FOREIGN KEY (prereq_subject, prereq_number) REFERENCES module(subject, number)
  );')
  dbExecute(dbCon, sqlForCreatingModulePrerequisiteTable)
}

#*****************************************
#* Insert Data Into "module_prerequisite" Table.
#* @param dbCon - database connection
#*****************************************
insertDataIntoModulePrerequisiteTable <- function(dbCon) {
  sqlForInsertingDataIntoModulePrerequisiteTable = (
  'INSERT INTO module_prerequisite (subject, number, prereq_subject, prereq_number) VALUES
    ("CS5300", 2, "CS5200", 1), -- R Programming Language requires Database Management Systems
    ("CS5500", 4, "CS5400", 3), -- Building Scalable Systems requires Cloud Computing
    ("CS5700", 6, "CS5200", 1), -- Computer Networks Security requires Database Management Systems
    ("CS5700", 6, "CS5600", 5) -- Computer Networks Security requires Computer Networks;')
  dbExecute(dbCon, sqlForInsertingDataIntoModulePrerequisiteTable)
}

#*****************************************
#* Create "author_module_mapping" Table If Not Exists.
#* @param dbCon - database connection
#*****************************************
createAuthorModuleTable <- function(dbCon) {
  sqlForCreatingAuthorModuleMappingTable = (
  'CREATE TABLE IF NOT EXISTS author_module_mapping(
    aid VARCHAR NOT NULL,
    subject VARCHAR NOT NULL,
    number INTEGER NOT NULL,
    PRIMARY KEY (aid, subject, number),
    FOREIGN KEY (aid) REFERENCES author(aid),
    FOREIGN KEY (subject, number) REFERENCES module(subject, number)
  );')
  dbExecute(dbCon, sqlForCreatingAuthorModuleMappingTable)
}

#*****************************************
#* Insert Data Into "author_module_mapping" Table.
#* @param dbCon - database connection
#*****************************************
insertDataIntoAuthorModuleMappingTable <- function(dbCon) {
  sqlForInsertingDataIntoAuthorModuleMappingTable = (
  'INSERT INTO author_module_mapping (aid, subject, number) VALUES
    ("A1", "CS5600", 5), -- Kushal Krishnappa teaches Computer Networks
    ("A1", "CS5700", 6), -- Kushal Krishnappa teaches Computer Networks Security
    ("A2", "CS5300", 2), -- Abhishek Rao teaches R Programming Language
    ("A3", "CS5400", 3), -- Pulasthya Reddy Siddareddy teaches Cloud Computing
    ("A4", "CS5500", 4), -- Raghs teaches Building Scalable Systems
    ("A4", "CS5200", 1) -- Raghs teaches Database Management Systems;')
  dbExecute(dbCon, sqlForInsertingDataIntoAuthorModuleMappingTable)
}

############################################################################
# SQL QUERIES TO RETRIEVE DATA FROM TABLES
############################################################################

#*****************************************
#* SQL Statement to Get All Experience Levels
#*****************************************
sqlForGetAllExperienceLevels <- function() {
  return ('SELECT * FROM experience_level;')
}

#*****************************************
#* SQL Statement to Get All Authors
#*****************************************
sqlForGetAllAuthors <- function() {
  return ('SELECT * FROM author;')
}

#*****************************************
#* SQL Statement to Get All Modules
#*****************************************
sqlForGetAllModules <- function() {
  return ('SELECT * FROM module;')
}

#*****************************************
#* SQL Statement to Get All Module_Prerequisites
#*****************************************
sqlForGetAllModulesPrerequisite <- function() {
  return ('SELECT * FROM module_prerequisite;')
}

#*****************************************
#* SQL Statement to Get All Author_Module_Mapping
#*****************************************
sqlForGetAllAuthorModuleMapping <- function() {
  return ('SELECT * FROM author_module_mapping;')
}

#*****************************************
#* SQL Statement to Get Authors With Given Experience Level
#*****************************************
sqlToGetAllAuthorsWithGivenExperienceLevel <- function(experienceLevel) {
  return (sprintf('
  SELECT a.* FROM author a
  JOIN experience_level e ON a.exp_id = e.exp_id
  WHERE e.level = "%s";', experienceLevel))
}

#*****************************************
#* SQL Statement to Get the Prerequisites for a Module
#*****************************************
sqlToGetPrerequisitesForModule <- function(module) {
  return (sprintf('
  SELECT prereq_subject, prereq_number
  FROM module_prerequisite
  WHERE subject = "%s";', module))
}

#*****************************************
#* SQL Statement to Get the Most Expensive Author
#*****************************************
sqlToGetMostExpensiveAuthor <- function() {
  return ('
  SELECT * FROM author
  ORDER BY perDiemRate DESC
  LIMIT 1;')
}

############################################################################
# CREATE TABLES, INSERT DATA INTO TABLES AND VALIDATE THE DATA, RUN QUERIES
############################################################################

#*****************************************
#* Create All Tables
#* @param dbCon - database connection
#*****************************************
createAllTables <- function(dbCon) {
  # create experience_level table
  createExperienceLevelTable(dbCon)
  # create author table
  createAuthorTable(dbCon)
  # create module table
  createModuleTable(dbCon)
  # create module_prerequisite table
  createModulePrerequisiteTable(dbCon)
  # create author_module_mapping table
  createAuthorModuleTable(dbCon)
}

#*****************************************
#* Insert Data Into All Tables
#* @param dbCon - database connection
#*****************************************
insertDataIntoAllTables <- function(dbCon) {
  # insert data into experience_level table
  insertDataIntoExperienceLevelTable(dbCon)
  # insert data into author table
  insertDataIntoAuthorTable(dbCon)
  # insert data into module table
  insertDataIntoModuleTable(dbCon)
  # insert data into module_prerequisite table
  insertDataIntoModulePrerequisiteTable(dbCon)
  # insert data into author_module_mapping table
  insertDataIntoAuthorModuleMappingTable(dbCon)
}

#*****************************************
#* Validate Data Inserted Into Tables
#* @param dbCon - database connection
#*****************************************
validateDataInsertedIntoTables <- function(dbCon) {
  
  test_that("Test ExperienceLevel Table: experience_level table should contain 3 rows", {
    experience_level <- dbReadTable(dbCon, "experience_level")
    expect_equal(nrow(experience_level), 3)
  })
  
  test_that("Test Author Table: author table should contain 4 rows", {
    author <- dbReadTable(dbCon, "author")
    expect_equal(nrow(author), 4)
  })
  
  test_that("Test Module Table: module table should contain 6 rows", {
    module <- dbReadTable(dbCon, "module")
    expect_equal(nrow(module), 6)
  })
  
  test_that("Test ModulePrerequisite Table: module_prerequisite table should contain 3 rows", {
    module_prerequisite <- dbReadTable(dbCon, "module_prerequisite")
    expect_equal(nrow(module_prerequisite), 4)
  })
  
  test_that("Test AuthorModule Table: author_module_mapping table should contain 6 rows", {
    author_module_mapping <- dbReadTable(dbCon, "author_module_mapping")
    expect_equal(nrow(author_module_mapping), 6)
  })
}

#*****************************************
#* Run All Queries
#* @param dbCon - database connection
#*****************************************

# get all rows from all the tables
getAllRowsFromAllTables <- function(dbCon) {
  expLevels <- dbGetQuery(dbCon, sqlForGetAllExperienceLevels())
  print("ExperienceLevels:")
  print(expLevels)
  
  authors <- dbGetQuery(dbCon, sqlForGetAllAuthors())
  print("Authors:")
  print(authors)
  
  modules <- dbGetQuery(dbCon, sqlForGetAllModules())
  print("Modules:")
  print(modules)
  
  modulePrerequisites <- dbGetQuery(dbCon, sqlForGetAllModulesPrerequisite())
  print("ModulePrerequisites:")
  print(modulePrerequisites)
  
  authorModulesMapping <- dbGetQuery(dbCon, sqlForGetAllAuthorModuleMapping())
  print("AuthorModuleMapping:")
  print(authorModulesMapping)
}

# names of the authors whose experience level is advanced.
getAllAuthorNamesWhoseExperienceLevelIsAdvanced <- function(dbCon) {
  authorsAdvanced <- dbGetQuery(dbCon, sqlToGetAllAuthorsWithGivenExperienceLevel("advanced"))
  authorNames <- authorsAdvanced$name
  cat("Authors whose experience level is advanced: ", paste(authorNames, sep="" ,collapse = "; "), "\n")
}

# prerequisites for a module CS5700 (Computer Networks Security)
getPrerequisitesForModuleCS5700 <- function(dbCon) {
  prerequisites <- dbGetQuery(dbCon, sqlToGetPrerequisitesForModule("CS5700"))
  prereqList <- paste(prerequisites$prereq_subject, prerequisites$prereq_number, prerequisites$title, sep = " ", collapse = "; ")
  cat("Prerequisites for module CS5700: ", prereqList, "\n")
}

# get the most expensive author
getMostExpensiveAuthor <- function(dbCon) {
  mostExpensiveAuthor <- dbGetQuery(dbCon, sqlToGetMostExpensiveAuthor())
  cat("Most Expensive Author: ", mostExpensiveAuthor$name, "\n")
}

# run all queries
runAllQueries <- function(dbCon) {
  # get all rows from all the tables
  getAllRowsFromAllTables(dbCon)
  
  # get all author names whose experience level is advanced
  getAllAuthorNamesWhoseExperienceLevelIsAdvanced(dbCon)
  
  # get prerequisites for a module
  getPrerequisitesForModuleCS5700(dbCon)
  
  # get the most expensive author
  getMostExpensiveAuthor(dbCon)
}


#*****************************************
#* Main Method
#*****************************************
main <- function() {
  # packages needed for R program to run
  packages <- c("RSQLite", "testthat")
  installPackagesOnDemand(packages)
  
  # create a database connection
  dbCon <- createSQLiteDBConnection("lessonDB-KrishnappaK.sqlitedb")
  
  # drop all existing tables
  dropAllExistingTables(dbCon)
  
  # enable foreign key constraints
  # Reference: http://artificium.us/lessons/06.r/l-6-300-create-sqlitedb-in-r/l-6-300.html
  dbExecute(dbCon, "PRAGMA foreign_keys = ON")
  
  # create tables
  createAllTables(dbCon)
  insertDataIntoAllTables(dbCon)
  
  # tests to validate the data inserted into tables
  # the tests are valid only when the data is inserted for the first time.
  # any changes to data will make the tests invalid and needs to be updated.
  validateDataInsertedIntoTables(dbCon) # comment this line if you don't want to run the test
  
  runAllQueries(dbCon)
  
  # close the database connection
  dbDisconnect(dbCon)
}

main()
