import json
import sqlite3
import os
import sys
import traceback

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
    except sqlite3.Error as e:
        print(e)
        print("ERROR: Creating Connection")
        traceback.print_exc()
        return None


# loads Json Data into sql lite tables
def loadJSONDataIntoTables(databaseConnection, allSetJSONDictionary):
    print(type(allSetJSONDictionary))
    try:
        test = 0
        # Begin inserting table.

        for setKey, setInfo in allSetJSONDictionary.items():
            # insert into set table

            setInsert = "Insert into tblSets('Size','Block','BoosterScheme','Code','IsOnlineOnly','MTGOCode','Name','ReleaseDate','TotalSetSize','Type') values(?,?,?,?,?,?,?,?,?,?);"

            setList = []

            if (test == 0):
                print(setKey)
                test += 1
                print(allSetJSONDictionary[setKey]["baseSetSize"])
                setList.append(allSetJSONDictionary[setKey]["baseSetSize"])
                print(allSetJSONDictionary[setKey].keys())

                # setList.append(allSetJSONDictionary[setKey]["block"])
                print(setList)
                """
                setList.append(str(allSetJSONDictionary[setKey]["boosterV3"]))
                setList.append(allSetJSONDictionary[setKey]["code"])
                setList.append(allSetJSONDictionary[setKey]["isOnlineOnly"])
                setList.append(allSetJSONDictionary[setKey]["mtgoCode"])
                setList.append(allSetJSONDictionary[setKey]["name"])
                setList.append(allSetJSONDictionary[setKey]["releaseDate"])
                setList.append(allSetJSONDictionary[setKey]["totalSetSize"])
                setList.append(allSetJSONDictionary[setKey]["type"])

                (allSetJSONDictionary[setKey]["baseSetSize"],
                allSetJSONDictionary[setKey]["block"],
                str(allSetJSONDictionary[setKey]["boosterV3"]),
                allSetJSONDictionary[setKey]["code"],
                allSetJSONDictionary[setKey]["isOnlineOnly"],
                allSetJSONDictionary[setKey]["mtgoCode"],
                allSetJSONDictionary[setKey]["name"],
                allSetJSONDictionary[setKey]["releaseDate"],
                allSetJSONDictionary[setKey]["totalSetSize"],
                allSetJSONDictionary[setKey]["type"]
                )"""

            #cursor.execute(setInsert, setList)

    except sqlite3.Error as e:
        print(e)
        print("ERROR: Insert statements failed")
        traceback.print_exc()
        raise

# Create database tables for sqllite database.


def createDatabaseTables(databaseConnection):
    cursor = databaseConnection.cursor()

# Create table for set information.
    try:
        print("Table Cursor: 1")
        cursor.execute("""CREATE TABLE tblSets (
        Size	INTEGER,
        Block	TEXT,
        BoosterScheme	TEXT,
        Code	TEXT UNIQUE,
        IsOnlineOnly	INTEGER,
        MTGOCode	TEXT,
        Name	TEXT,
        ReleaseDate	TEXT,
        TotalSetSize	INTEGER,
        Type	INTEGER,
        PRIMARY KEY(Code)
    );""")

# Create table for card information
        print("Table Cursor: 2")
        cursor.execute("""CREATE TABLE tblCards (
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
        MTGJSONID	TEXT UNIQUE,
        Watermark Text,
        PRIMARY KEY(MTGJSONID)
    );""")

    # Create table for token information
        print("Table Cursor: 3")
        cursor.execute("""CREATE TABLE tblTokens (
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
            MTGJSONID	TEXT UNIQUE,
            PRIMARY KEY(MTGJSONID)
        );""")

    # Create link tables
        print("Table Cursor: 4")
        cursor.execute("""CREATE TABLE `tblSetsTokens` (
        `Code`	TEXT,
        `MTGJSONID`	TEXT,
        PRIMARY KEY(`Code`,`MTGJSONID`)
    );""")

        print("Table Cursor: 5")
        cursor.execute("""CREATE TABLE `tblSetsCards` (
        `Code`	TEXT,
        `MTGJSONID`	TEXT,
        PRIMARY KEY(`Code`,`MTGJSONID`)
    );""")

    # Minor facet tables
        print("Table Cursor: 6")
        cursor.execute(
            """CREATE TABLE tblCardColorIdentity(Color Text, MTGJSONID Text, Primary Key (Color,MTGJSONID));""")
        print("Table Cursor: 7")
        cursor.execute(
            """CREATE TABLE tblCardColors(Color Text, MTGJSONID Text, Primary Key (Color,MTGJSONID));""")
        print("Table Cursor: 8")
        cursor.execute(
            """CREATE TABLE tblLegalFormat(MTGJSONID Text, LegalFormat Text, Primary Key (MTGJSONID,LegalFormat));""")
        print("Table Cursor: 9")
        cursor.execute(
            """CREATE TABLE tblSubTypes(MTGJSONID Text, SubType Text, Primary Key (MTGJSONID,SubType));""")
        print("Table Cursor: 10")
        cursor.execute(
            """CREATE TABLE tblSuperTypes(MTGJSONID Text, SuperType Text, Primary Key (MTGJSONID,SuperType));""")
        print("Table Cursor: 11")
        cursor.execute(
            """CREATE TABLE tblCardType(MTGJSONID Text, CardType Text, Primary Key (MTGJSONID,CardType));""")
        print("Table Cursor: 12")
        cursor.execute(
            """CREATE TABLE tblCardVariations(MTGJSONID Text, MTGJSONIDVariation Text, Primary Key (MTGJSONID,MTGJSONIDVariation));""")
        print("Table Cursor: Commit Statement")
        databaseConnection.commit()
    except sqlite3.Error as e:
        print(e)
        print("ERROR: Something Broke. Probably the tables")
        traceback.print_exc()

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
    except Exception as e:
        print("ERROR: Something Broke. This is the main function.")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        traceback.print_exc()

        print("Rollback occuring")
        dbConnection.rollback()
    if dbConnection is not None:
        dbConnection.close()
        print("We closed the database!")


if __name__ == '__main__':
    main()
