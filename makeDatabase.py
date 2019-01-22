import json
import sqlite3
import os
import sys
import traceback
from collections import OrderedDict
# retrieves a json file from the path provided


def retrieveAllSetsJSON(path):
    print("Opening the Vault!")
    with open(path, "r", encoding="utf8") as mtg_json:
        pythonMtgSets = json.load(mtg_json)
    return pythonMtgSets


# Ensure the database is in the proper formatting
def createDatabaseConnection(filename, forcedOverride=False):
    # Forcibly override database. Good for refresh.
    if os.path.exists(filename) and forcedOverride:
        os.remove(filename)
    try:
        conn = sqlite3.connect(filename)
        return conn
    except sqlite3.Error:
        print("ERROR: Creating Connection")

        return None
# General query function


def insertIntoTableBulk(databaseConnection, tableColumnsSet, tableRowsSetList, tableName):
    try:
        insertStatement = "Insert into "+tableName+"(\"" + "\",\"".join(
            tableColumnsSet) + "\") values(\"" + ",".join(["?"]*tableColumnsSet.length)+"\");"

        # Attempts insert statement
        databaseConnection.executemany(insertStatement, tableRowsSetList)
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Insert statements failed for table " +
              tableName+". Performing Rollback")
        databaseConnection.rollback()
        raise


def insertIntoTableSingle(databaseConnection, tableColumnsSet, tableRowSet, tableName):
    try:
        insertStatement = "Insert into "+tableName+"(\"" + "\",\"".join(
            tableColumnsSet) + "\") values(\"" + ",".join(["?"]*tableColumnsSet.length)+"\");"

        # Attempts insert statement
        databaseConnection.execute(insertStatement, tableRowSet)
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Insert statements failed for table " +
              tableName+". Performing Rollback")
        databaseConnection.rollback()
        raise
# Driver function to load data into tables


def loadJSONDataIntoTables(dbConnection, jsonFile):
    return None


def normalizeIrregularValues(rowDictionary, ColumnsSet):
    for i in ColumnsSet:
        if i in rowDictionary:
            if rowDictionary[i] is None:
                rowDictionary[i] = 'n/a'
        else:
            rowDictionary[i] = 'n/a'


def insertIntoSetsTable(databaseConnection, JSONDictionary):

    # Iterate through each set
            # Insert into sets table
    setsColumnsSet = ("baseSetSize", "block", "code", "isOnlineOnly",
                      "mtgoCode", "name", "releaseDate", "totalSetSize", "type")


def createDatabaseTables(databaseConnection):

    # Create table for set information.
    try:

        databaseConnection.execute("""CREATE TABLE tblSets(
        Size	INTEGER,
        Block	TEXT,
        BoosterScheme	TEXT,
        Code	TEXT,
        IsOnlineOnly	INTEGER,
        MTGOCode	TEXT,
        Name	TEXT,
        ReleaseDate	TEXT,
        TotalSetSize	INTEGER,
        Type	INTEGER,
        PRIMARY KEY(Code)
    ); """)

# Create table for card information

        databaseConnection.execute("""CREATE TABLE tblCards(
        Artist	TEXT,
        BorderColor	TEXT,
        ConvertedManaCost	REAL,
        DuelDeck	TEXT,
        ConvertedManaCostFace	REAL,
        FlavorText	TEXT,
        FrameEffect	TEXT,
        FrameVersion	TEXT,
        HasFoil	TEXT,
        HasNonFoil	TEXT,
        IsAlternative	TEXT,
        IsFoilOnly	TEXT,
        IsOnlineOnly	TEXT,
        IsOversized	TEXT,
        IsReserved	TEXT,
        IsTimeShifted	TEXT,
        Layout	TEXT,
        Loyalty	TEXT,
        ManaCost	TEXT,
        MultiverseID	INTEGER,
        Name	TEXT,
        NamesArray Text,
        Number	TEXT,
        OriginalText	TEXT,
        OriginalType	TEXT,
        Power	TEXT,
        Rarity	TEXT,
        ScryFallID	TEXT,
        Side	TEXT,
        Starter	TEXT,
        Text	TEXT,
        Toughness	TEXT,
        FullTypeText	TEXT,
        MTGJSONID	TEXT,
        Watermark Text,
        PRIMARY KEY(MTGJSONID)
    ); """)

    # Create table for token information

        databaseConnection.execute("""CREATE TABLE tblTokens(
            Artist	TEXT,
            BorderColor	TEXT,
            Loyalty	TEXT,
            Name	TEXT,
            Number	TEXT,
            OriginalText	TEXT,
            OriginalType	TEXT,
            Power	TEXT,
            ScryFallID	TEXT,
            Side	TEXT,
            Starter	TEXT,
            Text	TEXT,
            Toughness	TEXT,
            FullTypeText	TEXT,
            MTGJSONID	TEXT,
            PRIMARY KEY(MTGJSONID)
        ); """)

    # Create link tables

        databaseConnection.execute("""CREATE TABLE `tblSetsTokens` (`Code`	TEXT,
        `MTGJSONID`	TEXT,
        PRIMARY KEY(`Code`, `MTGJSONID`)
    ); """)

        databaseConnection.execute("""CREATE TABLE `tblSetsCards` (`Code`	TEXT,
        `MTGJSONID`	TEXT,
        PRIMARY KEY(`Code`, `MTGJSONID`)
    ); """)

    # Minor facet tables

        databaseConnection.execute(
            """CREATE TABLE tblCardColorIdentity(Color Text, MTGJSONID Text, Primary Key(Color, MTGJSONID)); """)

        databaseConnection.execute(
            """CREATE TABLE tblCardColors(Color Text, MTGJSONID Text, Primary Key(Color, MTGJSONID)); """)

        databaseConnection.execute(
            """CREATE TABLE tblLegalFormat(MTGJSONID Text, LegalFormat Text, Primary Key(MTGJSONID, LegalFormat)); """)

        databaseConnection.execute(
            """CREATE TABLE tblSubTypes(MTGJSONID Text, SubType Text, Primary Key(MTGJSONID, SubType)); """)

        databaseConnection.execute(
            """CREATE TABLE tblSuperTypes(MTGJSONID Text, SuperType Text, Primary Key(MTGJSONID, SuperType)); """)

        databaseConnection.execute(
            """CREATE TABLE tblCardType(MTGJSONID Text, CardType Text, Primary Key(MTGJSONID, CardType)); """)
        databaseConnection.execute(
            """CREATE TABLE tblCardVariations(MTGJSONID Text, MTGJSONIDVariation Text, Primary Key(MTGJSONID, MTGJSONIDVariation));""")
        print("Table Cursor: Commit Statement")
        databaseConnection.commit()
    except sqlite3.Error as e:
        print(e)
        print("ERROR: Something Broke. Probably the tables")

        databaseConnection.rollback()
        raise


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: %s <database path> <json path> [append current database]" % sys.argv[0])
        exit(1)

    jsonFile = retrieveAllSetsJSON(os.path.expanduser(sys.argv[2]))
    dbConnection = createDatabaseConnection(
        os.path.expanduser(sys.argv[1]), os.path.expanduser(sys.argv[3]))
    if dbConnection is not None:
        print("I Made a pretty database!!")
        print(type(dbConnection))

    try:
        print("Creating Database.")
        createDatabaseTables(dbConnection)
        loadJSONDataIntoTables(dbConnection, jsonFile)
    except:
        print("ERROR: Something Broke. This is the main function.")
        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print(exc_type, fname, exc_tb.tb_lineno)
        traceback.print_exc()
        exit("General Error")

    if dbConnection is not None:
        dbConnection.close()
        print("We closed the database!")


if __name__ == '__main__':
    main()
