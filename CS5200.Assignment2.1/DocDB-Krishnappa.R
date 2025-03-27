#*****************************************
#* title: ASSIGNMENT 02.1: Build File Data Store
#* author: Krishnappa, Kushal
#*****************************************

#*****************************************
#* Global Variables
#*****************************************
#* Header Style Reference: https://bookdown.org/yih_huynh/Guide-to-R-Book/r-conventions.html
#*****************************************
rootDir <- "docDB" 
intakeDir <- "docTemp"

#*****************************************
#* Create Directory if Not Exists
#* @param dirPath - path to the directory
#*****************************************
# Reference: 
# 1.http://artificium.us/lessons/06.r/l-6-402-filesystem-from-r/l-6-402.html#Interactive_Folder_and_File_Selection
# 2.https://stackoverflow.com/questions/67481634/what-does-the-recursive-argument-mean-in-dir-create
#*****************************************
createDirectoryIfNotExists <- function(dirPath) {
  if (!dir.exists(dirPath)) {
    dir.create(dirPath, recursive = TRUE)
    cat(sprintf("Success: %s: directory created.\n", dirPath))
  }
}

#*****************************************
#* Setup Database
#* @param root - path to the root directory (default = rootDir)
#* @param intake - path to intake directory (default = intakeDir)
#*****************************************
setupDB <- function(root = rootDir, intake = intakeDir) {
  cat("Setting up database...\n")
  createDirectoryIfNotExists(root)
  createDirectoryIfNotExists(intake)
}

#*****************************************
#* Parse Date From String
#* @param strDate - date in string format
#*****************************************
#* Reference: 
#* 1.http://artificium.us/lessons/06.r/l-6-112-text-proc/l-6-112.html
#*****************************************
parseDateFromString <- function(strDate) {
  date <- as.Date(strDate, format = "%d%m%y")
  return(date)
}

#*****************************************
#* Split File Name
#* @param fileName - Filename in string
#* @param sep - on what separater the filename should be split (default = "\\.")
#*****************************************
# Reference: 
# 1.http://artificium.us/lessons/06.r/l-6-112-text-proc/l-6-112.html
# 2.https://stackoverflow.com/questions/46518228/return-value-of-strsplit
#*****************************************
splitFileName <- function(fileName, sep = "\\.") {
  return(unlist(strsplit(fileName, sep))) 
}

#*****************************************
#* Check File for Validity
#* @param fileName - Filename in string
#*****************************************
# Reference: 
# 1.http://artificium.us/lessons/06.r/l-6-112-text-proc/l-6-112.html
# 2.https://stackoverflow.com/questions/1169248/test-if-a-vector-contains-a-given-element
#*****************************************
checkFile <- function(fileName) {
  splitFileNameList <- splitFileName(fileName)
  
  # Check for valid number of dots
  if (length(splitFileNameList) != 4) {
    cat(sprintf("Error: %s: invalid file: wrong number of dots.\n", fileName))
    return(FALSE)
  }
  
  # Check for valid extensions
  validExtensions <- c("xml", "csv", "json")
  fileExtension <- splitFileNameList[4]
  if (!(fileExtension %in% validExtensions)){
    cat(sprintf("Error: %s: invalid file: unsupported file extension.\n", fileName))
    return(FALSE)
  }
  
  # Check for valid dates
  firstday <- parseDateFromString(splitFileNameList[2])
  lastday <- parseDateFromString(splitFileNameList[3])
  if (is.na(firstday) || is.na(lastday)) {
    cat(sprintf("Error: %s: invalid file: issue with dates.\n", fileName))
    return(FALSE)
  }
  
  # Check lastday is after the firstday
  if(firstday > lastday) {
    cat(sprintf("Error: %s: invalid file: firstday is greater than lastday.\n", fileName))
    return(FALSE)
  }
  
  return(TRUE)
}

#*****************************************
#* Get Customer Name
#* @param fileName - Filename in string
#*****************************************
getCustomerName <- function(fileName) {
  if(checkFile(fileName)) {
    customerName = splitFileName(fileName)[1]
    return(customerName)
  } else {
    cat(sprintf("Error: %s: cannot get customer name.\n", fileName))
  }
}

#*****************************************
#* Get First Day
#* @param fileName - Filename in string
#*****************************************
getFirstDay <- function(fileName) {
  if(checkFile(fileName)) {
    firstDay = splitFileName(fileName)[2]
    return(firstDay)
  } else {
    cat(sprintf("Error: %s: cannot get first day.\n", fileName))
  }
}

#*****************************************
#* Get Extension
#* @param fileName - Filename in string
#*****************************************
getExtension <- function(fileName) {
  if(checkFile(fileName)) {
    ext = splitFileName(fileName)[4]
    return(ext)
  } else {
    cat(sprintf("Error: %s: cannot get extention.\n", fileName))
  }
}

#*****************************************
#* Generate Document Path
#* @param root - path to the root directory (default = rootDir)
#* @param firstday - date in string (format = mmddyy)
#* @param ext - extension in string
#*****************************************
genDocPath <- function(root = rootDir, firstday, ext) {
  return(paste(root, firstday, ext, sep = "/"))
}

#*****************************************
#* Generate File Path
#* @param dirPath - directory path in string
#* @param fileName - Filename in string
#*****************************************
genFilePath <- function(dirPath, fileName) {
  return(paste(dirPath, fileName, sep = "/"))
}

#*****************************************
#* Check Folder Exists
#* @param folderPath - folder path in string
#*****************************************
checkFolderExists <- function(folderPath) {
  if(dir.exists(folderPath)) {
    return(TRUE)
  } else {
    cat(sprintf("Error: %s: folder doesn't exist.\n", folderPath))
    return(FALSE)
  }
}

#*****************************************
#* Check File Exists
#* @param filePath - file path in string
#*****************************************
checkFileExists <- function(filePath) {
  if(file.exists(filePath)) {
    return(TRUE)
  } else {
    cat(sprintf("Error: %s: file doesn't exist.\n", filePath))
    return(FALSE)
  }
}

#*****************************************
#* Remove File From File Path
#* @param filePath - file path in string
#*****************************************
removeFile <- function(filePath) {
  if ((file.remove(filePath))) {
    return(TRUE)
  } else {
    cat(sprintf("Error: %s: file not removed.\n", filePath))
    return(FALSE)
  }
}

#*****************************************
#* Store Document
#* @param intake - path to intake directory (default = intakeDir)
#* @param fileName - Filename in string
#* @param docFolder - path to the root directory (default = rootDir)
#*****************************************
storeDoc <- function(intakeFolder = intakeDir, fileName, docFolder = rootDir) {
  if(!checkFile(fileName)) {
    cat(sprintf("Error: %s: cannot store document.\n", fileName))
    return(FALSE)
  }
  
  # check file exists in the intakeDir
  intakeFilePath <- genFilePath(dirPath = intakeFolder, fileName = fileName)
  if (!checkFileExists(filePath = intakeFilePath)) {
    cat(sprintf("Error: %s: cannot store document.\n", fileName))
    return(FALSE)
  }
  
  # generate docPath
  docPath <- genDocPath(root = docFolder, firstday = getFirstDay(fileName), ext = getExtension(fileName))
  # generate docFilePath to store the file
  docFilePath <- genFilePath(dirPath = docPath, fileName = splitFileName(fileName)[1])
  
  # create docPath directories if not exists
  createDirectoryIfNotExists(docPath)
  
  # copy the file 
  file.copy(intakeFilePath, docFilePath)
  
  # check the size of the files
  sizeOfFileInIntakeDir <- file.info(intakeFilePath)$size
  sizeOfFileInRootDir <- file.info(docFilePath)$size
  if(sizeOfFileInIntakeDir == sizeOfFileInRootDir) {
    removeFile(intakeFilePath)
    cat(sprintf("Success: %s: document stored.\n", fileName))
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#*****************************************
#* Check File Copied Successfully
#* @param root - path to the root directory (default = rootDir)
#* @param fileName - Filename in string
#*****************************************
checkFileCopiedSuccessfully <- function(root = rootDir, fileName) {
  docPath <- genDocPath(root = root, firstday = getFirstDay(fileName), ext = getExtension(fileName))
  copyFileName <- splitFileName(fileName)[1]
  docFilePath <- genFilePath(docPath, copyFileName)
  if(!checkFileExists(filePath = docFilePath)) {
    cat(sprintf("Error: %s: file wasn't copied successfully.\n", fileName))
    return(FALSE)
  }
  return(TRUE)
}

#*****************************************
#* Store All Documents
#* @param intakeFolder - path to intake directory (default = intakeDir)
#* @param rootFolder - path to the root directory (default = rootDir)
#*****************************************
# Reference:
# 1.https://www.geeksforgeeks.org/read-all-files-in-directory-using-r/
# 2.http://artificium.us/lessons/06.r/l-6-104-r4progs/l-6-104.html#Install_Packages_on_Demand
# 3.https://www.geeksforgeeks.org/how-to-append-values-to-list-in-r/
#*****************************************
storeAllDocs <- function(intakeFolder = intakeDir, rootFolder = rootDir) {
  # Throw error if intakeDir and docDB folders doesn't exist
  if(!(checkFolderExists(intakeFolder) || checkFolderExists(rootFolder))) {
    stop(sprintf("Error: intake folder: %s or root folder: %s, does not exist.\n", intakeFolder, rootFolder))
  }
  
  # get the list of files
  filesInIntakeDir <- list.files(intakeFolder)
  
  numOfFilesProcessed <- 0
  filesNotProcessed <- c()
  
  # Process each file in intakeDir
  for(file in filesInIntakeDir) {
    # copy the file to rootDir
    if(!storeDoc(intakeFolder = intakeFolder, fileName = file, docFolder = rootFolder)) {
      cat(sprintf("Error: %s: file was not processed.\n", file))
      filesNotProcessed <- append(filesNotProcessed, file)
    }
    # check if the file is successfully copied
    else if(!checkFileCopiedSuccessfully(fileName = file)) {
      cat(sprintf("Error: %s: file was not processed: file copy failed.\n", file))
      append(filesNotProcessed, file)
    } else {
      numOfFilesProcessed <- numOfFilesProcessed + 1
    }
  }
  
  # Message after processing is completed
  cat(sprintf("Successfully processed %d files.\n", numOfFilesProcessed))
  
  # List the files that were failed to process
  if(length(filesNotProcessed) > 0) {
    cat("These files were not processed:", sep = "\n")
    cat(filesNotProcessed, sep = "\n")
  }
}

#*****************************************
#* Reset DB
#* @param root - path to the root directory (default = rootDir)
#*****************************************
resetDB <- function(root = rootDir) {
  resetDbPath <- genFilePath(dirPath = root, fileName = "*")
  cat("Resetting the database...\n")
  unlink(resetDbPath, recursive = TRUE)
}

#*****************************************
#* Install Packages
#* @param packages - list of packages required for script to run
#*****************************************
# Reference: 
# 1.https://www.geeksforgeeks.org/read-all-files-in-directory-using-r/
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

#****************************************************************************
#* Tests
#* Reference:
#* 1. http://artificium.us/lessons/06.r/l-6-194-testing/l-6-194.html
#* 2. ChatGPT with promt - "I want to write the testcases for R program. 
#* Help me understand how to write tests for R using `testthat` framework."
#****************************************************************************

tests1 <- function(testDir = "tests") {
  
  test_that("Test createDirectoryIfNotExists: create valid directory if doesn't exist", {
    createTestDirPath <- paste(testDir, "createTestDir", sep = "/") 
    # create directory
    createDirectoryIfNotExists(createTestDirPath)
    expect_true(dir.exists(createTestDirPath)) # check if directory created
    # remove directory
    unlink(createTestDirPath, recursive = TRUE)
    expect_false(dir.exists(createTestDirPath)) # check if directory removed
  })
  
  test_that("Test setupDB: check if the setup db works", {
    testDocDir <- paste(testDir, "testDocDir", sep = "/")
    testIntakeDir <- paste(testDir, "testIntakeDir", sep = "/")
    setupDB(root = testDocDir, intake = testIntakeDir)
    expect_true(dir.exists(testDocDir))
    expect_true(dir.exists(testIntakeDir))
    unlink(testDocDir, recursive = TRUE)
    unlink(testIntakeDir, recursive = TRUE)
  })
  
  test_that("Test parseDateFromString: parse valid dates", {
    # edge case: year is 2000
    expect_equal(parseDateFromString("110400"), as.Date("2000-04-11"))
    # edge case: everything 0
    expect_true(is.na(parseDateFromString("000000")))
  })
  
  test_that("Test splitFileName: file should be split into vector with passed sep", {
    # check default sep = "\\."
    expect_equal(splitFileName("ValidFile.010123.020123.xml"), c("ValidFile", "010123", "020123", "xml"))
    # check sep = "/"
    expect_equal(splitFileName(fileName = "ValidFile/010123/020123/xml", sep = "/"), c("ValidFile", "010123", "020123", "xml"))
  })
  
  test_that("Test checkFile: check given file is has valid name", {
    # valid filenames
    expect_true(checkFile("ValidFile.010123.020123.xml"))
    expect_true(checkFile("KlainerIndustries.011224.021224.xml"))
    expect_true(checkFile("TechCorp.150124.150124.csv"))
    expect_true(checkFile("GlobalLogistics.310124.050224.json"))
    # invalid filenames
    expect_false(checkFile("KlainerIndustries.LLC.011224.021224.xml")) # More than three dots
    expect_false(checkFile("TechCorp.150124.140124.csv")) # LastDay < FirstDay
    expect_false(checkFile("GlobalLogistics.310124.050224.doc")) # Invalid extension
    expect_false(checkFile("InnovateLLC.010324150324.xml")) # Missing dot separator
    expect_false(checkFile("TechCorp_150124_150124.csv"))  # Underscores instead of dots
    expect_false(checkFile("ExampleCorp.1501.150224.json")) # Invalid date format
  })
}

tests2 <- function(testDir = "tests") {
  
  test_that("Test getCustomerName: return customer name for valid file", {
    # check getCustomerName for valid filename
    expect_equal(getCustomerName("Customer.010123.020123.xml"), "Customer")
    # check getCustomerName for invalid filename
    expect_null(getCustomerName("Customer.Moredots.010123.020123.xml"))
  })
  
  test_that("Test getFirstDay: return first day for valid file", {
    # check getCustomerName for valid filename
    expect_equal(getFirstDay("Customer.010123.020123.xml"), "010123")
    # check getCustomerName for invalid filename
    expect_null(getFirstDay("Customer.Moredots.010123.020123.xml"))
  })
  
  test_that("Test getExtension: return extension name for valid file", {
    # check getCustomerName for valid filename
    expect_equal(getExtension("Customer.010123.020123.xml"), "xml")
    # check getCustomerName for invalid filename
    expect_null(getExtension("Customer.Moredots.010123.020123.xml"))
  })
  
  test_that("Test genDocPath: generate valid document path", {
    expect_equal(genDocPath("testDoc", "firstday", "ext"), "testDoc/firstday/ext")
  })
  
  test_that("Test genFilePath: generate valid file path", {
    expect_equal(genFilePath("testDir", "filename"), "testDir/filename")
  })
}

tests3 <- function(testDir = "tests") {
  
  test_that("Test checkFileExists & removeFile: check if the file exists in the filepath", {
    # create temp_test_file.xml and check file exists
    tempFile <- file.path(testDir, "temp_test_file.xml")
    writeLines("This is a test file.", tempFile)
    expect_true(checkFileExists(tempFile))
    
    # delete file and check if it is deleted
    removeFile(tempFile)
    expect_false(checkFileExists(tempFile))
  })
  
  test_that("Test storeDoc: check if doc is stored in the docDB", {
    # check if storeDoc returns False if testDir is not present
    expect_false(storeDoc("randomFolder1","file1","randomFolder2"))
    
    # check if storeDoc returns Flase if testFile is not present
    mockDocDBPath <- paste(testDir,"mockDocDB",sep = "/")
    mockIntakeDirPath = paste(testDir,"mockDocTemp",sep = "/")
    setupDB(mockDocDBPath,mockIntakeDirPath) 
    expect_false(storeDoc(mockIntakeDirPath,"file1",mockDocDBPath))
    unlink(mockDocDBPath, recursive = TRUE)
    unlink(mockIntakeDirPath, recursive = TRUE)
  })
  
  test_that("Test checkFileCopiedSuccessfully: check if the file is copied successfully", {
    # create a mock docDB and intakeDir
    mockDocDBPath <- paste(testDir,"mockDocDB",sep = "/")
    mockIntakeDirPath = paste(testDir,"mockDocTemp",sep = "/")
    setupDB(mockDocDBPath,mockIntakeDirPath)
    
    # create a mock file in mock intakeDB
    testFileName = "KlainerIndustries.011224.021224.xml"
    tempDoc <- file.path(mockIntakeDirPath, testFileName)
    writeLines("This is a test file.", tempDoc)
    
    # Simulate a copy process to the root directory
    docPath <- genDocPath(root = mockDocDBPath, firstday = getFirstDay(testFileName), ext = getExtension(testFileName))
    createDirectoryIfNotExists(docPath)
    docFilePath <- genFilePath(docPath, splitFileName(testFileName)[1])
    file.copy(tempDoc, docFilePath)
    
    # check file copied successfully
    expect_true(checkFileCopiedSuccessfully(root = mockDocDBPath, fileName = testFileName))
    
    # delete the file and check
    unlink(docFilePath, recursive = TRUE)
    expect_false(checkFileCopiedSuccessfully(root = mockDocDBPath, fileName = testFileName))
    
    # clear the mocks
    unlink(mockDocDBPath, recursive = TRUE)
    unlink(mockIntakeDirPath, recursive = TRUE)
  })
  
  test_that("Test storeAllDocs: check if all the docs in the intake folder are processed", {
    # throw error if the rootDir and intakeDir directories are not present
    expect_error(storeAllDocs("randomFolder1","randomFolder2"), 
                 "Error: intake folder: randomFolder1 or root folder: randomFolder2, does not exist.")
    
    # check if files are stored successfully
    # create a mock docDB and intakeDir
    mockDocDBPath <- paste(testDir,"mockDocDB",sep = "/")
    mockIntakeDirPath = paste(testDir,"mockDocTemp",sep = "/")
    setupDB(mockDocDBPath,mockIntakeDirPath)
    
    # create the valid file mock
    testValidFileName = "KlainerIndustries.011224.021224.xml"
    tempValidDoc <- file.path(mockIntakeDirPath, testValidFileName)
    writeLines("This is a valid test file.", tempValidDoc)
    # create the invalid file mock
    testInvalidFileName = "KlainerIndustries.011224.021224.doc"
    tempInvalidDoc <- file.path(mockIntakeDirPath, testInvalidFileName)
    writeLines("This is a invalid test file.", tempInvalidDoc)
    
    storeAllDocs(intakeFolder = mockIntakeDirPath, rootFolder = mockDocDBPath)
    
    expect_false(file.exists(tempValidDoc))
    expect_true(file.exists("tests/mockDocDB/011224/xml/KlainerIndustries"))
    expect_true(file.exists(tempInvalidDoc))
    
    # clear the mocks
    unlink(mockDocDBPath, recursive = TRUE)
    unlink(mockIntakeDirPath, recursive = TRUE)
  })
  
  test_that("Test resetDB: check if the db is cleared", {
    # setup mock docDB
    mockDocDBPath <- paste(testDir, "mockDocDB", sep = "/")
    createDirectoryIfNotExists(mockDocDBPath)
    
    # create a temporary file within a folder in mockDocDB
    tempDirPath <- paste(mockDocDBPath, "tempDir", sep = "/")
    createDirectoryIfNotExists(tempDirPath)
    tempDoc <- file.path(tempDirPath, "temp_test_file.xml")
    writeLines("This is a test file.", tempDoc)
    
    # check docDB exists
    expect_true(dir.exists(mockDocDBPath))
    # check temp dir and doc exists
    expect_true(dir.exists(tempDirPath))
    expect_true(file.exists(tempDoc))
    
    # reset the DB
    resetDB(root = mockDocDBPath)
    
    # check docDB exists
    expect_true(dir.exists(mockDocDBPath))
    # check temp dir and doc exists
    expect_false(dir.exists(tempDirPath))
    expect_false(file.exists(tempDoc))
    
    # clear the mockDocDB
    unlink(mockDocDBPath, recursive = TRUE)
  })
  
}

clearTestsDirectory <- function(testDir = "tests") {
  unlink(testDir, recursive = TRUE)
}

invokeTests <- function() {
  # Create a test directory
  testDir = "tests"
  createDirectoryIfNotExists(testDir)
  # invoke all the test funtions in same order
  tests1()
  tests2()
  tests3()
  clearTestsDirectory()
}

#*****************************************
#* Create Files in docTemp
#* Reference: ChatGPT to create a mock files for development
#*****************************************
createFilesInDocTemp <- function() {
  # Ensure the intake folder exists
  createDirectoryIfNotExists(intakeDir)
  
  # File names to create
  valid_files <- c(
    "KlainerIndustries.011224.021224.xml",
    "TechCorp.150124.150124.csv",
    "GlobalLogistics.310124.050224.json",
    "InnovateLLC.010324.150324.xml"
  )
  
  invalid_files <- c(
    "KlainerIndustries.LLC.011224.021224.xml",   # More than three dots
    "TechCorp.150124.140124.csv",               # LastDay < FirstDay
    "GlobalLogistics.310124.050224.doc",        # Invalid extension
    "InnovateLLC.010324150324.xml",             # Missing dot separator
    "TechCorp_150124_150124.csv",               # Underscores instead of dots
    "ExampleCorp.1501.150224.json"              # Invalid date format
  )
  
  # Combine valid and invalid files
  all_files <- c(valid_files, invalid_files)
  
  # Create each file in the intake directory
  for (file_name in all_files) {
    file_path <- file.path(intakeDir, file_name)
    # Write a simple message in each file
    writeLines(c("This is a test file.", paste("Filename:", file_name)), file_path)
    cat("File created:", file_path, "\n")
  }
}

#*****************************************
#* Main Method
#*****************************************
main <- function() {
  # packages needed in R program
  packages <- c("testthat")
  installPackagesOnDemand(packages)
  
  setupDB()
  createFilesInDocTemp() #uncomment to create mock files in docTemp
  invokeTests() #uncomment to run tests
  
  storeAllDocs()
  resetDB() #uncomment to reset the DB database
}

################################################################################

main()
