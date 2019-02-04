import json
import sqlite3
import os
import sys
import traceback
from collections import OrderedDict
# retrieves a json file from the path provided

#opens json file in utf-8 encoding
def retrieveAllSetsJSON(path):
    print("Opening the vault!")
    with open(path, "r", encoding="utf8") as mtg_json:
        pythonMtgSets = json.load(mtg_json)
    print("Closing the vault!")
    return pythonMtgSets


# Ensure the database is sqlite compatible file and creates connection
# Option to remove file.
# NEEDS REVIEW FOR SAFER FILE HANDLING
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

#Flexible create table function. 
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

        databaseConnection.execute(createStatement)
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Create statement failed for table " +
              tableName+". Performing Rollback")
        databaseConnection.rollback()
        raise

#single insert into table statement
def insertIntoTableSingle(databaseConnection, rowDictionary, tableName):
    #break apart dictionary
    rowColumns=[str(column) for column in rowDictionary.keys()]
    rowValues=[str(rowDictionary[values]) for values in rowColumns]
    try:
        insertStatement = "Insert into "+tableName+"(\"" + "\",\"".join(
            rowColumns) + "\") values(\"" + "\",\"".join(rowValues)+"\");"

        databaseConnection.execute(insertStatement)
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Insert statements failed for table " +
              tableName+". Performing Rollback")
        databaseConnection.rollback()
        raise

#Bulk insert into table statement
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

# Assign default values to column row values that are empty
# If value is not present, it generally is not included in json file
def getCleanDictionary(rowDictionary, columnsList):
    #build references to row values needed
    #print(rowDictionary.keys())
    #print(columnsList)
    cleanDictionary=dict()
    for column in columnsList:
        if column in rowDictionary: 
            
            cleanDictionary[column]=rowDictionary[column]
            if cleanDictionary[column] is None:
                cleanDictionary[column]='n/a'
        else:
            cleanDictionary[column]='n/a'
    return cleanDictionary

# Represents individual components of each table.
# Get statement will return a tuple of table name, 
# sqllite columnname/dataype structure as dictionary and 
# primary keys as a list of column names
def getCardColorIdentityTableStructure():
    return ("tblCardColorIdentity", {"color": "Text", "mtgJsonID": "Text"}, ["Color", "mtgJsonID"])


def getCardColorsTableStructure():
    return ("tblCardColors", {"Color": "Text", "mtgJsonID": "Text"}, ["Color", "mtgJsonID"])


def getCardTypeTableStructure():
    return ("tblCardType", {"MTGJSONID": "Text", "CardType": "Text"}, ["mtgJsonID", "CardType"])


def getCardVariationsTableStructure():
    return ("tblCardVariations", {"mtgJsonID": "Text", "mtgJsonIDVariation": "Text"}, ["mtgJsonID", "mtgJsonIDVariation"])


def getCardsTableStructure():
    return ("tblCards", {"Artist": "TEXT", "BorderColor": "TEXT", "ConvertedManaCost": "REAL", "DuelDeck": "TEXT", "ConvertedManaCostFace": "REAL", "FlavorText": "TEXT", "FrameEffect": "TEXT", "FrameVersion": "TEXT", "HasFoil": "TEXT", "HasNonFoil": "TEXT", "IsAlternative": "TEXT", "IsFoilOnly": "TEXT", "IsOnlineOnly": "TEXT", "IsOversized": "TEXT", "IsReserved": "TEXT", "IsTimeShifted": "TEXT", "Layout": "TEXT", "Loyalty": "TEXT", "ManaCost": "TEXT", "MultiverseID": "INTEGER", "Name": "TEXT", "NamesArray": "Text", "Number": "TEXT", "OriginalText": "TEXT", "OriginalType": "TEXT", "Power": "TEXT", "Rarity": "TEXT", "ScryFallID": "TEXT", "Side": "TEXT", "Starter": "TEXT", "Text": "TEXT", "Toughness": "TEXT", "FullTypeText": "TEXT", "MTGJSONID": "TEXT", "Watermark": "Text"}, ["MTGJSONID"])


def getLegalFormatTableStructure():
    return ("tblLegalFormat", {"MTGJSONID": "Text", "LegalFormat": "Text"}, ["MTGJSONID", "LegalFormat"])


def getSetsTableStructure():
    return ("tblSets", {"baseSetSize": "INTEGER", "block": "TEXT", "code": "TEXT", "isOnlineOnly": "INTEGER", "mtgoCode": "TEXT", "name": "TEXT", "releaseDate": "TEXT", "totalSetSize": "INTEGER", "type": "INTEGER"}, ["code"])


def getSetsCardsTableStructure():
    return ("tblSetsCards", {"code": "TEXT", "MTGJSONID": "TEXT"}, ["code", "MTGJSONID"])


def getSetsTokensTableStructure():
    return ("tblSetsTokens",{"code" :"TEXT", "MTGJSONID" :"TEXT"}, ["code", "MTGJSONID"])

def getSubTypesTableStructure():
    return ("tblSubTypes",{"MTGJSONID" :"TEXT", "SubType" :"TEXT"},["MTGJSONID", "SubType"])


def getSuperTypesTableStructure():
    return ("tblSuperTypes",{"MTGJSONID" :"TEXT", "SuperType" :"TEXT"}, ["MTGJSONID", "SuperType"])

def getTokensTableStructure():
    return ("tblTokens", {"Artist":"TEXT","BorderColor":"TEXT","Loyalty":"TEXT","Name":"TEXT","Number" : "TEXT","Original":"TEXT","OriginalType" : "TEXT","Power" : "TEXT","ScryFallID" : "TEXT","Side" : "TEXT","Starter" : "TEXT","Text":"TEXT","Toughness" : "TEXT","FullType":"TEXT","MTGJSONID" : "TEXT"},["MTGJSONID"])

#Calls and passes all get statements to create table function 
# to freshly build all tables.
def createAllDatabaseTables(databaseConnection):
    print("Creating all tables")
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

# Driver function to load data into tables
#mtgJsonFile will be a dictionary of a set's abreviated name
#as the key, with the values being a dictionary representing
#the different aspects of a set.
def parseMtgJsonIntoTables(dbConnection, mtgJsonFile):
    #load table structure information
    cardColorIdentityTableStructure= getCardColorIdentityTableStructure()
    cardColorsTableStructure= getCardColorsTableStructure()
    cardTypeTableStructure=getCardTypeTableStructure()
    cardVariationsTableStructure=getCardVariationsTableStructure()
    cardsTableStructure=getCardsTableStructure()
    legalFormatTableStructure=getLegalFormatTableStructure()
    setsTableStructure=getSetsTableStructure()
    setsCardsTableStructure=getSetsCardsTableStructure()
    setsTokensTableStructure=getSetsTokensTableStructure()
    subTypesTableStructure=getSubTypesTableStructure()
    superTypesTableStructure=getSuperTypesTableStructure()
    tokensTableStructure=getTokensTableStructure()

    print("Loading the tables")
    for setInformation in mtgJsonFile.values():
        #print("prenormalize: "+str(setInformation.keys()))
        #insert set information 
        #getCleanDictionary(setInformation, setsTableStructure[1].keys())
        #requiredFields= {x:y for (x,y) in setInformation.items() if x in setsTableStructure[1]} 
        insertIntoTableSingle(dbConnection,
        getCleanDictionary(setInformation, setsTableStructure[1].keys()),
        setsTableStructure[0])
        #print("post-normalize: "+str(setInformation.keys()))
        #print("Required fields: "+str(requiredFields.keys()))

def main():
    if len(sys.argv) < 3:
        print(
            "Usage: %s <database path> <json path> [append current database]" % sys.argv[0])
        exit(1)

    dbConnection = createDatabaseConnection(os.path.expanduser(sys.argv[1]), os.path.expanduser(sys.argv[3]))

    try:
        print("Creating Database.")
        createAllDatabaseTables(dbConnection)
        parseMtgJsonIntoTables(dbConnection, retrieveAllSetsJSON(os.path.expanduser(sys.argv[2])))
    except:
        print("ERROR: Something Broke. This is the main function.")
        traceback.print_exc()
        exit("General Error")

    if dbConnection is not None:
        dbConnection.close()
        print("The database is done building.")


if __name__ == '__main__':
    main()
