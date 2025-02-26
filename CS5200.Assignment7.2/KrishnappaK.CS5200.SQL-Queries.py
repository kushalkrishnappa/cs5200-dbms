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
    @raise Exception: if the connection cannot be created
    @return: the SQLite connection object or None
    """
    try:
        dbConnection = sqlite3.connect(dbPath)
        return dbConnection
    except sqlite3.Error as e:
        print(e)
    return None

def close_dbConnection(dbConnection: sqlite3.Connection):
    """Close the connection to the SQLite database

    @param conn: the SQLite connection object
    """
    if dbConnection:
        print(f"Closing the connection to the SQLite database {dbConnection}")
        dbConnection.close()

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

    close_dbConnection(dbConnection) # Close the connection to the SQLite database

if __name__ == '__main__':
    """Run this python file as standalone script."""
    main()
