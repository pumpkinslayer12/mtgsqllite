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


def insertIntoTableBulk(databaseConnection, columnRowsTuple, tableName):
    try:
        insertStatement = "Insert into "+tableName+"(\"" + "\",\"".join(
            columnRowsTuple[0]) + "\") values(\"" + ",".join(["?"]*len(columnRowsTuple[0]))+"\");"

        # Attempts insert statement
        databaseConnection.executemany(insertStatement, columnRowsTuple[1])
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Insert statements failed for table " +
              tableName+". Performing Rollback")
        databaseConnection.rollback()
        raise


def insertIntoTableSingle(databaseConnection, columnRowTuple, tableName):
    try:
        insertStatement = "Insert into "+tableName+"(\"" + "\",\"".join(
            columnRowTuple[0]) + "\") values(\"" + ",".join(["?"]*len(columnRowTuple[0]))+"\");"

        # Attempts insert statement
        databaseConnection.execute(insertStatement, columnRowTuple[1])
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Insert statements failed for table " +
              tableName+". Performing Rollback")
        databaseConnection.rollback()
        raise



def normalizeIrregularValues(rowDictionary, columnsList):
    for i in columnsList:
        if i in rowDictionary:
            if rowDictionary[i] is None:
                rowDictionary[i] = 'n/a'
        else:
            rowDictionary[i] = 'n/a'

def createTable(databaseConnection, tableName, tableStructureDictionary, primaryKeyList):
    try:
        columnStatements=list()
        for columnName, dataType in tableStructureDictionary.items():
            columnStatements.append(columnName+" "+dataType)
        
        if len(primaryKeyList)==0 or primaryKeyList is None:
            primaryKeyStatement=""
        else:
            primaryKeyStatement=",Primary Key("+",".join(primaryKeyList)+")"

        createStatement="Create Table "+ tableName +"("+",".join(columnStatements)+primaryKeyStatement+");"

        # Attempts insert statement
        databaseConnection.execute(createStatement)
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Create statement failed for table " +
              tableName+". Performing Rollback")
        databaseConnection.rollback()
        raise
# Driver function to load data into tables
def loadJSONDataIntoTables(dbConnection, jsonFile):
    return None

# returns tuple of table name, sqllite meta structure as dictionary and primary keys as lists


def getCardColorIdentityTableStructure():
    return ("tblCardColorIdentity", {"Color": "Text", "MTGJSONID": "Text"}, ["Color", "MTGJSONID"])


def getCardColorsTableStructure():
    return ("tblCardColors", {"Color": "Text", "MTGJSONID": "Text"}, ["Color", "MTGJSONID"])


def getCardTypeTableStructure():
    return ("tblCardType", {"MTGJSONID": "Text", "CardType": "Text"}, ["MTGJSONID", "CardType"])


def getCardVariationsTableStructure():
    return ("tblCardVariations", {"MTGJSONID": "Text", "MTGJSONIDVariation": "Text"}, ["MTGJSONID", "MTGJSONIDVariation"])


def getCardsTableStructure():
    return ("tblCards", {"Artist": "TEXT", "BorderColor": "TEXT", "ConvertedManaCost": "REAL", "DuelDeck": "TEXT", "ConvertedManaCostFace": "REAL", "FlavorText": "TEXT", "FrameEffect": "TEXT", "FrameVersion": "TEXT", "HasFoil": "TEXT", "HasNonFoil": "TEXT", "IsAlternative": "TEXT", "IsFoilOnly": "TEXT", "IsOnlineOnly": "TEXT", "IsOversized": "TEXT", "IsReserved": "TEXT", "IsTimeShifted": "TEXT", "Layout": "TEXT", "Loyalty": "TEXT", "ManaCost": "TEXT", "MultiverseID": "INTEGER", "Name": "TEXT", "NamesArray": "Text", "Number": "TEXT", "OriginalText": "TEXT", "OriginalType": "TEXT", "Power": "TEXT", "Rarity": "TEXT", "ScryFallID": "TEXT", "Side": "TEXT", "Starter": "TEXT", "Text": "TEXT", "Toughness": "TEXT", "FullTypeText": "TEXT", "MTGJSONID": "TEXT", "Watermark": "Text"}, ["MTGJSONID"])


def getLegalFormatTableStructure():
    return ("tblLegalFormat", {"MTGJSONID": "Text", "LegalFormat": "Text"}, ["MTGJSONID", "LegalFormat"])


def getSetsTableStructure():
    return ("tblSets", {"Size": "INTEGER", "Block": "TEXT", "BoosterScheme": "TEXT", "Code": "TEXT", "IsOnlineOnly": "INTEGER", "MTGOCode": "TEXT", "Name": "TEXT", "ReleaseDate": "TEXT", "TotalSetSize": "INTEGER", "Type": "INTEGER"}, ["Code"])


def getSetsCardsTableStructure():
    return ("tblSetsCards", {"Code": "TEXT", "MTGJSONID": "TEXT"}, ["Code", "MTGJSONID"])


def getSetsTokensTableStructure():
    return ("tblSetsTokens",{"Code" :"TEXT", "MTGJSONID" :"TEXT"}, ["Code", "MTGJSONID"])

def getSubTypesTableStructure():
    return ("tblSubTypes",{"MTGJSONID" :"TEXT", "SubType" :"TEXT"},["MTGJSONID", "SubType"])


def getSuperTypesTableStructure():
    return ("tblSuperTypes",{"MTGJSONID" :"TEXT", "SuperType" :"TEXT"}, ["MTGJSONID", "SuperType"])

def getTokensTableStructure():
    return ("tblTokens", {"Artist":"TEXT","BorderColor":"TEXT","Loyalty":"TEXT","Name":"TEXT","Number" : "TEXT","Original":"TEXT","OriginalType" : "TEXT","Power" : "TEXT","ScryFallID" : "TEXT","Side" : "TEXT","Starter" : "TEXT","Text":"TEXT","Toughness" : "TEXT","FullType":"TEXT","MTGJSONID" : "TEXT"},["MTGJSONID"])


def createDatabaseTables(databaseConnection):
    #returnTuple=getCardColorIdentityTableStructure()
    #createTable(databaseConnection, returnTuple[0], returnTuple[1], returnTuple[2])
    createTable(databaseConnection, *getCardColorIdentityTableStructure())
    createTable(databaseConnection, *getCardColorsTableStructure())
    createTable(databaseConnection, *getCardTypeTableStructure())
    createTable(databaseConnection, *getCardVariationsTableStructure())
    createTable(databaseConnection, *getCardsTableStructure())
    createTable(databaseConnection, *getLegalFormatTableStructure())
    createTable(databaseConnection, *getSetsTableStructure())
    createTable(databaseConnection, *getSetsCardsTableStructure())
    createTable(databaseConnection, *getSetsTokensTableStructure())
    createTable(databaseConnection, *getSubTypesTableStructure())
    createTable(databaseConnection, *getSuperTypesTableStructure())
    createTable(databaseConnection, *getTokensTableStructure())

def main():
    if len(sys.argv) < 3:
        print(
            "Usage: %s <database path> <json path> [append current database]" % sys.argv[0])
        exit(1)

    #jsonFile = retrieveAllSetsJSON(os.path.expanduser(sys.argv[2]))
    dbConnection = createDatabaseConnection(
        os.path.expanduser(sys.argv[1]), os.path.expanduser(sys.argv[3]))
    if dbConnection is not None:
        print("I Made a pretty database!!")
        print(type(dbConnection))

    try:
        print("Creating Database.")
        createDatabaseTables(dbConnection)
        #loadJSONDataIntoTables(dbConnection, jsonFile)
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
