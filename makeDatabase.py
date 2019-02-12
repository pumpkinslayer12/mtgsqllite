import json
import sqlite3
import os
import sys
import traceback
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
    createStatement=""
    try:
        columnStatements=list()
        for columnName, dataType in tableStructureDictionary.items():
            columnStatements.append(columnName+" "+dataType)
        
        if len(primaryKeyList)==0 or primaryKeyList is None:
            primaryKeyStatement=""
        else:
            primaryKeyStatement=",Primary Key("+",".join(primaryKeyList)+")"

        createStatement="Create Table "+ tableName +"("+",".join(columnStatements)+primaryKeyStatement+");"
        #createStatement="Create Table "+ tableName +"("+",".join(columnStatements)+");"
        databaseConnection.execute(createStatement)
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Create statement failed for table " +
              tableName+". Performing Rollback")
        print(createStatement)
        databaseConnection.rollback()
        raise

#single insert into table statement
def insertIntoTableSingle(databaseConnection, rowDictionary, tableName):
    insertStatement=""
    #break apart dictionary
    rowColumns=[str(column) for column in rowDictionary.keys()]
    rowValues=[str(rowDictionary[values]) for values in rowColumns]
    try:
        insertStatement = "Insert into "+tableName+"(\"" + "\" , \"".join(
            rowColumns) + "\") values(\"" + "\" , \"".join(rowValues)+"\");"

        databaseConnection.execute(insertStatement)
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Insert statements failed for table " +
              tableName+". Performing Rollback")
        print(insertStatement)
        databaseConnection.rollback()
        raise

#single insert into table statement
def insertIntoTableMany(databaseConnection, columnHeadings, rowValues, tableName):
    insertStatement=""
    #break apart dictionary
    try:
        insertStatement = "Insert into "+tableName+"(\"" + "\" , \"".join(
            columnHeadings) + "\") values("+" , ".join(["?"]*len(columnHeadings))+");"
        databaseConnection.executemany(insertStatement,set(rowValues))
        databaseConnection.commit()
    # Rolls back database changes if errors are encountered
    except sqlite3.Error:
        print("ERROR: Insert statements failed for table " +
              tableName+". Performing Rollback")
        print(insertStatement)
        #print(len(rowValues))
        #print(len(set(rowValues)))
        databaseConnection.rollback()
        raise

# Assign default values to column row values that are empty
# If value is not present, it generally is not included in json file
def getCleanDictionary(rowDictionary, columnsList):
    #build references to row values needed
    cleanDictionary=dict()
    for column in columnsList:
        if column in rowDictionary: 
            
            cleanDictionary[column]=str(rowDictionary[column]).replace('\n',' ').replace('\r',' ').replace("\"","")
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
    return ("tblCardColorIdentity", {"color": "Text", "uuid": "Text"}, ["color", "uuid"])


def getCardColorsTableStructure():
    return ("tblCardColors", {"color": "Text", "uuid": "Text"}, ["color", "uuid"])


def getCardTypeTableStructure():
    return ("tblCardType", {"uuid": "Text", "cardType": "Text"}, ["uuid", "cardType"])


def getCardVariationsTableStructure():
    return ("tblCardVariations", {"uuid1": "Text", "uuid2": "Text"}, ["uuid1", "uuid2"])


def getCardsTableStructure():
    return ("tblCards", {"artist": "TEXT", "borderColor": "TEXT", "convertedManaCost": "REAL", "DuelDeck": "TEXT", "convertedManaCostFace": "REAL", "flavorText": "TEXT", "frameEffect": "TEXT", "frameVersion": "TEXT", "hasFoil": "TEXT", "hasNonFoil": "TEXT", "isAlternative": "TEXT", "isFoilOnly": "TEXT", "isOnlineOnly": "TEXT", "isOversized": "TEXT", "isReserved": "TEXT", "isTimeShifted": "TEXT", "layout": "TEXT", "loyalty": "TEXT", "manaCost": "TEXT", "multiverseID": "INTEGER", "name": "TEXT", "namesArray": "Text", "number": "TEXT", "originalText": "TEXT", "originalType": "TEXT", "power": "TEXT", "rarity": "TEXT", "scryFallID": "TEXT", "side": "TEXT", "starter": "TEXT", "text": "TEXT", "toughness": "TEXT", "fullTypeText": "TEXT", "uuid": "TEXT", "watermark": "Text"}, ["uuid"])


def getLegalFormatTableStructure():
    return ("tblLegalFormat", {"uuid": "Text", "LegalFormat": "Text"}, ["uuid", "LegalFormat"])


def getSetsTableStructure():
    return ("tblSets", {"baseSetSize": "INTEGER", "block": "TEXT", "code": "TEXT", "isOnlineOnly": "INTEGER", "mtgoCode": "TEXT", "name": "TEXT", "releaseDate": "TEXT", "totalSetSize": "INTEGER", "type": "INTEGER"}, ["code"])


def getSetsCardsTableStructure():
    return ("tblSetsCards", {"code": "TEXT", "uuid": "TEXT"}, ["code", "uuid"])


def getSetsTokensTableStructure():
    return ("tblSetsTokens",{"code" :"TEXT", "uuid" :"TEXT"}, ["code", "uuid"])

def getSubTypesTableStructure():
    return ("tblSubTypes",{"uuid" :"TEXT", "SubType" :"TEXT"},["uuid", "subType"])


def getSuperTypesTableStructure():
    return ("tblSuperTypes",{"uuid" :"TEXT", "superType" :"TEXT"}, ["uuid", "superType"])

def getTokensTableStructure():
    return ("tblTokens", {"artist":"TEXT","borderColor":"TEXT","loyalty":"TEXT","name":"TEXT","number" : "TEXT","original":"TEXT","originalType" : "TEXT","power" : "TEXT","scryFallID" : "TEXT","side" : "TEXT","starter" : "TEXT","text":"TEXT","toughness" : "TEXT","fullType":"TEXT","uuid" : "TEXT"},["uuid"])

def getAllTableColumns():
    rawTables=[
    getCardColorIdentityTableStructure(),
    getCardColorsTableStructure(),
    getCardTypeTableStructure(),
    getCardVariationsTableStructure(),
    getCardsTableStructure(),
    getLegalFormatTableStructure(),
    getSetsTableStructure(),
    getSetsCardsTableStructure(),
    getSetsTokensTableStructure(),
    getSubTypesTableStructure(),
    getSuperTypesTableStructure(),
    getTokensTableStructure()]
    return {i[0]:[str(j) for j in i[1].keys()] for i in rawTables}
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
    allTableColumns=getAllTableColumns()
    batchDictionary={i:[] for i in allTableColumns.keys()}

    #Here because list objects are accessible in template, but not as raw numbers.
    #Not sure why, but fixes scope issue.
    emergency=[0]
    #General template function used by all tables to append their values for batch insert
    def appendTemplate(tblName, valuesDictionary):
        cleanDictionary=getCleanDictionary(valuesDictionary, allTableColumns[tblName])
        
        if 'uuid' in cleanDictionary:
            if cleanDictionary['uuid']=='n/a':
                cleanDictionary['uuid']="FIX"+str(emergency[0])
                emergency[0]+=1
                
        setValues=tuple(cleanDictionary[i] for i in allTableColumns[tblName])
        batchDictionary[tblName].append(setValues)
    
    print("Loading the tables")
    for abrv, setInformation in mtgJsonFile.items():
        
        
        appendTemplate('tblSets',setInformation)

        for card in setInformation['cards']:
            appendTemplate('tblCards',card)
            appendTemplate('tblSetsCards',{'uuid':card['uuid'],'code':setInformation['code']})
            
            for color in card['colorIdentity']:
                appendTemplate('tblCardColorIdentity',{'uuid':card['uuid'],'color':color})

            for color in card['colors']:
                appendTemplate('tblCardColors',{'uuid':card['uuid'],'color':color})

            
                appendTemplate('tblCardColorIdentity',{'uuid':card['uuid'],'color':color})
            
            for cardType in card['types']:
                if 'uuid' in card: 
                    appendTemplate('tblCardType',{'uuid':card['uuid'],'cardType':cardType})
                else:
                    appendTemplate('tblCardType',{'uuid':"n/a",'cardType':cardType})
            
            for variationID in card['variations']:
                if 'uuid' in card: 
                    appendTemplate('tblCardType',{'uuid':card['uuid'],'cardType':cardType})
                else:
                    appendTemplate('tblCardType',{'uuid':"n/a",'cardType':cardType})
        
        for token in setInformation['tokens']:
           
            
            appendTemplate('tblTokens',token)
            if 'uuid' in token:
                appendTemplate('tblSetsTokens',{'uuid':token['uuid'],'code':setInformation['code']})
            else:
                appendTemplate('tblSetsTokens',{'uuid':"n/a",'code':setInformation['code']})
            
    for tableName, values in batchDictionary.items():
        if len(values)>0:
            insertIntoTableMany(dbConnection, allTableColumns[tableName], values, tableName)
        
        
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
        traceback.print_exc()
        exit("General Error")

    if dbConnection is not None:
        dbConnection.close()
        print("The database is done building.")


if __name__ == '__main__':
    main()
