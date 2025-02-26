"""
title: ASSIGNMENT 07.2
subtitle: Query a Database with SQL in Python
author: Krishnappa, Kushal
date: Spring 2025
"""
import sqlite3

def create_dbConnection(dbPath: str) -> sqlite3.Connection | None:
    """
    Create a connection to the SQLite database specified by dbPath.

    @param dbPath: the path to the SQLite database
    @return: the SQLite connection object or None
    """
    try:
        dbConnection = sqlite3.connect(dbPath)
        return dbConnection
    except sqlite3.Error as e:
        print("Error connecting to database", e)
    return None

def close_dbConnection(dbConnection: sqlite3.Connection):
    """Close the connection to the SQLite database

    @param conn: the SQLite connection object
    """
    if dbConnection:
        dbConnection.close()

# What are the name, contact name, and country of all suppliers, sorted by supplier name? Print the result set.
def query_supplier_details(dbConnection: sqlite3.Connection):
    """
    Query the SQLite database for the name, contact name, and country of all suppliers, sorted by supplier name.

    @param dbConnection: the SQLite connection object
    """
    try:
        sqlQuery = """
        SELECT SupplierName, ContactName, Country
        FROM Suppliers
        ORDER BY SupplierName
        """
        cursor = dbConnection.cursor()
        cursor.execute(sqlQuery)
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        print("The name, contact name, and country of all suppliers, sorted by supplier name.")
        print(column_names)
        for row in rows:
            print(row)
        cursor.close()
    except sqlite3.Error as e:
        print("Error in querying supplier details", e)

# What is the total number of different products offered for sale by each supplier? Display the result.
def query_total_products_by_supplier(dbConnection: sqlite3.Connection):
    """
    Query the SQLite database for the total number of different products offered for sale by each supplier.

    @param dbConnection: the SQLite connection object
    """
    try:
        sqlQuery = """
        SELECT S.SupplierName, COUNT(DISTINCT P.ProductID) AS TotalProducts
        FROM Suppliers S
        JOIN Products P ON S.SupplierID = P.SupplierID
        GROUP BY S.SupplierName
        ORDER BY S.SupplierName
        """
        cursor = dbConnection.cursor()
        cursor.execute(sqlQuery)
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        print("The total number of different products offered for sale by each supplier.")
        print(column_names)
        for row in rows:
            print(row)
        cursor.close()
    except sqlite3.Error as e:
        print("Error in querying total products sold by supplier", e)

# Which countries have more than ten suppliers? List the country and the number of suppliers in each.
def query_countries_with_more_than_ten_suppliers(dbConnection: sqlite3.Connection):
    """
    Query the SQLite database for the countries with more than ten suppliers.

    @param dbConnection: the SQLite connection object
    """
    try:
        sqlQuery = """
        SELECT S.Country, COUNT(S.SupplierID) AS TotalSuppliers
        FROM Suppliers S
        GROUP BY S.Country
        HAVING TotalSuppliers > 10
        """
        cursor = dbConnection.cursor()
        cursor.execute(sqlQuery)
        rows = cursor.fetchall()
        if (len(rows) > 10):
            print("The countries with more than ten suppliers.")
            column_names = [description[0] for description in cursor.description]
            print(column_names)
            for row in rows:
                print(row)
        else:
            print("No countries have more than 10 suppliers.")

        sqlQuery = """
        SELECT S.Country, COUNT(S.SupplierID) AS TotalSuppliers
        FROM Suppliers S
        GROUP BY S.Country
        ORDER BY TotalSuppliers DESC
        """
        cursor.execute(sqlQuery)
        rows = cursor.fetchall()
        print("The countries with the number of suppliers.")
        column_names = [description[0] for description in cursor.description]
        print(column_names)
        for row in rows:
            print(row)
        cursor.close()
    except sqlite3.Error as e:
        print("Error in querying countries with more than ten suppliers", e)

def print_line():
    """
    Print a line.
    """
    print("-" * 80)

def main():
    """
    Main function.
    """
    dbPath = "OrdersDB.sqlitedb.db" # Path to the SQLite database

    dbConnection = create_dbConnection(dbPath) # Create a connection to the SQLite database
    if dbConnection is None:
        print(f"Failed to create a connection to {dbPath} SQLite database.")
        return
    
    dbConnection.execute("PRAGMA foreign_keys = ON") # Enable foreign key constraints

    print_line()
    query_supplier_details(dbConnection) # Query the SQLite database for supplier details
    print_line()
    query_total_products_by_supplier(dbConnection) # Query the SQLite database for total products sold by supplier
    print_line()
    query_countries_with_more_than_ten_suppliers(dbConnection) # Query the SQLite database for countries with more than ten suppliers
    print_line()

    close_dbConnection(dbConnection) # Close the connection to the SQLite database

if __name__ == '__main__':
    """Run this python file as standalone script."""
    main()
