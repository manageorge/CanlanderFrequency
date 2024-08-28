import requests
import json
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import shutil
from tqdm import tqdm
import re
import sqlite3

def saveData(output, cardDb):
    try:
        conn = sqlite3.connect(cardDb)
        cur = conn.cursor()
        date = datetime.now().strftime('%Y_%m_%d')
        #get name of columns in the 'frequency' table
        tempCols = cur.execute("PRAGMA table_info('frequency')").fetchall()
        cols = []
        i = 0
        while i < len(tempCols):
            cols.append(tempCols[i][1])
            i += 1
        #add freqdate cols to frequency table if needed
        outputCard = next(iter(output['cards']))
        for freqcount in output['cards'][outputCard]:
            withdate = freqcount + date    
            if not withdate in cols:
                sql = ("ALTER TABLE frequency ADD " + withdate + " INTEGER")
                cur.execute(sql)
        tempCards = cur.execute("SELECT name FROM frequency").fetchall()
        cards = []
        i = 0
        while i < len(tempCards):
            cards.append(tempCards[i][0])
            i += 1
        #add card freq data to freqdate cols in frequency table
        for card in output['cards']:
            if not card in cards:
                #make new entry if no entry for current card
                cur.execute("INSERT INTO frequency (name) VALUES (?)", (card,))
            for freqcount in output['cards'][card]:
                countInt = output['cards'][card][freqcount]
                if countInt != 0:
                    withdate = freqcount + date
                    sql = ("UPDATE frequency SET " + withdate + " = ? WHERE name = ?")
                    cur.execute(sql, (countInt, card))
        #decks count table
        #get dates in table
        tempDates = cur.execute("SELECT date FROM deckscount").fetchall()
        dates = []
        i = 0
        while i < len(tempDates):
            dates.append(tempDates[i][0])
            i += 1
        if not date in dates:
            #make new entry if no entry for current date
            cur.execute("INSERT INTO deckscount VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (date, output['meta']['counts']['deckCount'], output['meta']['counts']['waCount'], output['meta']['counts']['quarterDeckCount'], output['meta']['counts']['quarterWaCount'], output['meta']['counts']['monthDeckCount'], output['meta']['counts']['monthWaCount'], output['meta']['counts']['sincePointsDeckCount'], output['meta']['counts']['sincePointsWaCount']))
        else:
            #update existing entry if there is one for current date
            cur.execute("UPDATE deckscount SET deckcount = ?, wacount = ?, quarterdeckcount = ?, quarterwacount = ?, monthdeckcount = ?, monthwacount = ?, sincepointsdeckcount = ?, sincepointswacount = ? WHERE date = ?", (output['meta']['counts']['deckCount'], output['meta']['counts']['waCount'], output['meta']['counts']['quarterDeckCount'], output['meta']['counts']['quarterWaCount'], output['meta']['counts']['monthDeckCount'], output['meta']['counts']['monthWaCount'], output['meta']['counts']['sincePointsDeckCount'], output['meta']['counts']['sincePointsWaCount'], date))
        #points spreads table
        #get name of columns in the 'pointsspreads' table
        tempCols = cur.execute("PRAGMA table_info('pointsspreads')").fetchall()
        cols = []
        i = 0
        while i < len(tempCols):
            cols.append(tempCols[i][1])
            i += 1
        #add freqdate cols to cards table if needed
        outputSpread = next(iter(output['points']))
        for freqcount in output['points'][outputSpread]:
            withdate = freqcount + date    
            if not withdate in cols:
                sql = ("ALTER TABLE pointsspreads ADD " + withdate + " INTEGER DEFAULT NULL")
                cur.execute(sql)
        #get list of existing spreads in db
        tempSpreads = cur.execute("SELECT spread FROM pointsspreads").fetchall()
        spreads = []
        i = 0
        while i < len(tempSpreads):
            spreads.append(tempSpreads[i][0])
            i+= 1
        #add spreads and frequency to db
        for spread in output['points']:
            if not spread in spreads:
                cur.execute("INSERT INTO pointsspreads (spread) VALUES (?)", (spread,))
            for freqcount in output['points'][spread]:
                countInt = output['points'][spread][freqcount]
                if countInt != 0:
                    withdate = freqcount + date
                    sql = ("UPDATE pointsspreads SET " + withdate + " = ? WHERE spread = ?")
                    cur.execute(sql, (countInt, spread))
        #make wadecks/events table
        #get decks in table
        tempWaDecks = cur.execute("SELECT deckid FROM waevents").fetchall()
        waDecks = []
        i = 0
        while i < len(tempWaDecks):
            waDecks.append(tempWaDecks[i][0])
            i += 1
        for deckid in output['waDecks']:
            deckdate = output['waDecks'][deckid][0]
            deckname = output['waDecks'][deckid][1]
            eventstring = output['waDecks'][deckid][2]
            deckurl = output['waDecks'][deckid][3]
            if not deckid in waDecks:
                #make new entry if no entry for deck
                cur.execute("INSERT INTO waevents VALUES (?, ?, ?, ?, ?)", (deckid, deckdate, deckname, eventstring, deckurl))
            else:
                #update existing entry if there is one for deck
                cur.execute("UPDATE waevents SET date = ?, name = ?, event = ?, publicurl = ? WHERE deckid = ?", (deckdate, deckname, eventstring, deckurl, deckid))
        conn.commit()
    finally:
        conn.close()
    

def processDecks(externalDir, cardDb):
    with open(Path(externalDir + 'meldDict.json')) as file:
        meldDict = json.loads(file.read())
    try:
        conn = sqlite3.connect(cardDb)
        cur = conn.cursor()
        tempDecksToProcess = cur.execute("SELECT deckid FROM decks").fetchall()
    finally:
        conn.close()
    decksToProcess = []
    i = 0
    while i < len(tempDecksToProcess):
        decksToProcess.append(tempDecksToProcess[i][0])
        i += 1
    decksToArchive = []
    waCount = 0
    waDecks = {}
    deckCount = 0
    quarterDeckCount = 0
    monthDeckCount = 0
    quarterWaCount = 0
    monthWaCount = 0
    sincePointsDeckCount = 0
    sincePointsWaCount = 0
    ##to-do: add lastPointsDate to pointsList.json (would also need to change prior access to that file)
    ##or just change the whole thing to the db file
    lastPointsDate = datetime.strptime('01-01-2024', '%m-%d-%Y')
    pointSets = {}
    frequencyDict = {}
    for deck in tqdm(decksToProcess, desc='Processing decks', unit=' decks'):  
        try:
            conn = sqlite3.connect(cardDb)
            cur = conn.cursor()
            deckDataPull = cur.execute("SELECT deckid, lastupdate, createdby, publicurl, cards, points, name FROM decks WHERE deckid = ?", (deck,)).fetchone()
        finally:
            conn.close()
        deckId = deckDataPull[0]
        deckLastUpdate = deckDataPull[1]
        deckLastUpdateDt = datetime.strptime(deckLastUpdate[:10], '%Y-%m-%d')
        deckCreatedBy = deckDataPull[2]
        deckUrl = deckDataPull[3]
        deckCardsStr = deckDataPull[4]
        deckCardsList = deckCardsStr.split(',,')
        deckPointsStr = deckDataPull[5]
        deckName = deckDataPull[6]
        oneYearAgo = datetime.now() - relativedelta(years=1)
        oneQuarterAgo = datetime.now() - relativedelta(months=3)
        oneMonthAgo = datetime.now() - relativedelta(months=1)
        #add to waevents list even if deck is older than 1 year
        if deckCreatedBy == "CanlanderWinnersArchive":
            wa = True
            r = re.compile('\d{4}/\d{2}/\d{2}')
            if r.match(deckName[:10]) is not None: #if the first 10 characters are like 2023/12/31
                date = deckName[:10]    
                deckName = deckName[13:]
            else:
                date = deckLastUpdate
            eventIndex = deckName.rfind('(')
            if eventIndex >= 0: #if ( is found in name
                eventString = deckName[eventIndex:]
                endIndex = eventString.find(',')
                if endIndex >= 0: # if , is found in eventString
                    eventString = eventString[1:endIndex]
                elif eventString.find('.') >= 0: # to fix one , that is a . instead
                    endIndex = eventString.find('.')
                    eventString = eventString[1:endIndex]
                else:
                    endIndex = eventString.find(')')
                    eventString = eventString[1:endIndex]
            if 'Async' in eventString:
                if 'MTGO' not in eventString:
                    eventString = 'Async'
                else:
                    eventString = 'MTGO Async'
            data = [date,deckName,eventString,deckUrl]
            waDecks[deck] = data
        else:
            wa = False
        #skip decks that haven't been updated in the last year
        if deckLastUpdateDt < oneYearAgo:
            decksToArchive.append(deckId)
            continue
        else:
            deckCount += 1
            if wa == True:
                waCount += 1
        #add to appropriate deck counters
        if deckLastUpdateDt >= oneQuarterAgo:
            quarterDeckCount += 1
            inQuarter = True
            if wa == True:
                quarterWaCount +=1
        else:
            inQuarter = False
        if deckLastUpdateDt >= oneMonthAgo:
            monthDeckCount += 1
            inMonth = True
            if wa == True:
                monthWaCount +=1
        else:
            inMonth = False
        if deckLastUpdateDt >= lastPointsDate:
            sincePointsDeckCount += 1
            sincePoints = True
            if wa == True:
                sincePointsWaCount +=1
        else:
            sincePoints = False
        for card in deckCardsList:
            name = card
            if name.startswith("A-"): #turns Alchemy-changed cards into their paper name (the Alchemy versions shouldn't be in canlander decks anyways)
                name = name.replace("A-", "")
            if name in meldDict:
                name = meldDict[name]
            if name not in frequencyDict:
                cardAttributes = {} #add card to allCards Data
                cardAttributes['frequency'] = 1
                if wa == True:
                    cardAttributes['waFrequency'] = 1
                else:
                    cardAttributes['waFrequency'] = 0
                if inQuarter == True:
                    cardAttributes['quarterFrequency'] = 1
                    if wa == True:
                        cardAttributes['waQuarterFrequency'] = 1
                    else:
                        cardAttributes['waQuarterFrequency'] = 0
                else:
                    cardAttributes['quarterFrequency'] = 0
                    cardAttributes['waQuarterFrequency'] = 0
                if inMonth == True:
                    cardAttributes['monthFrequency'] = 1
                    if wa == True:
                        cardAttributes['waMonthFrequency'] = 1
                    else:
                        cardAttributes['waMonthFrequency'] = 0
                else:
                    cardAttributes['monthFrequency'] = 0
                    cardAttributes['waMonthFrequency'] = 0
            else:
                cardAttributes = frequencyDict[name] #set cardAttributes to what's already been pulled
                cardAttributes['frequency'] += 1 #add one to cardAttributes['frequency']
                if wa == True:
                    cardAttributes['waFrequency'] += 1
                if inQuarter == True:
                    cardAttributes['quarterFrequency'] += 1
                    if wa == True:
                        cardAttributes['waQuarterFrequency'] += 1
                if inMonth == True:
                    cardAttributes['monthFrequency'] += 1
                    if wa == True:
                        cardAttributes['waMonthFrequency'] += 1
            frequencyDict[name] = cardAttributes #set allCardsData[name] to changed cardAttributes
        pointsString = deckPointsStr.replace(',,',', ')
        if pointsString not in pointSets:
            spreadAttributes = {}
            spreadAttributes['frequency'] = 1
            if wa == True:
                spreadAttributes['waFrequency'] = 1
            else:
                spreadAttributes['waFrequency'] = 0
            if inQuarter:
                spreadAttributes['quarterFrequency'] = 1
                if wa == True:
                    spreadAttributes['waQuarterFrequency'] = 1
                else:
                    spreadAttributes['waQuarterFrequency'] = 0
            else:
                spreadAttributes['quarterFrequency'] = 0
                spreadAttributes['waQuarterFrequency'] = 0
            if inMonth:
                spreadAttributes['monthFrequency'] = 1
                if wa == True:
                    spreadAttributes['waMonthFrequency'] = 1
                else:
                    spreadAttributes['waMonthFrequency'] = 0
            else:
                spreadAttributes['monthFrequency'] = 0
                spreadAttributes['waMonthFrequency'] = 0
            if sincePoints == True:
                spreadAttributes['sincePoints'] = 1
                if wa == True:
                    spreadAttributes['waSincePoints'] = 1
                else:
                    spreadAttributes['waSincePoints'] = 0
            else:
                spreadAttributes['sincePoints'] = 0
                spreadAttributes['waSincePoints'] = 0
        else:
            spreadAttributes = pointSets[pointsString]
            spreadAttributes['frequency'] += 1
            if wa == True:
                spreadAttributes['waFrequency'] += 1
            if inQuarter == True:
                spreadAttributes['quarterFrequency'] += 1
                if wa == True:
                    spreadAttributes['waQuarterFrequency'] += 1
            if inMonth == True:
                spreadAttributes['monthFrequency'] += 1
                if wa == True:
                    spreadAttributes['waMonthFrequency'] += 1
            if sincePoints == True:
                spreadAttributes['sincePoints'] += 1
                if wa == True:
                    spreadAttributes['waSincePoints'] += 1
        pointSets[pointsString] = spreadAttributes
    output = {'meta': {'created': datetime.now().strftime('%Y-%m-%d'), 
              'counts': {'deckCount':deckCount, 'waCount':waCount, 'quarterDeckCount':quarterDeckCount,
              'quarterWaCount':quarterWaCount, 'monthDeckCount':monthDeckCount, 'monthWaCount':monthWaCount,
              'sincePointsDeckCount':sincePointsDeckCount, 'sincePointsWaCount':sincePointsWaCount}}, 
              'cards': frequencyDict, 'points': pointSets, 'waDecks': waDecks}
    try:
        conn = sqlite3.connect(cardDb)
        cur = conn.cursor()
        for deckId in tqdm(decksToArchive, desc='Archiving stale decks', unit=' decks'):
            deckData = cur.execute("SELECT * FROM decks WHERE deckid = ?", (deckId,)).fetchone()
            cur.execute("DELETE FROM decks WHERE deckid = ?", (deckId,))
            cur.execute("INSERT INTO staledecks VALUES (?, ?, ?, ?, ?, ?, ?)", (deckData))
        conn.commit()
    finally:
        conn.close()
    return output    

def sortColorsWUBRG(strColor):
    colorUpper = strColor.upper()
    if colorUpper == 'W':
        return 0
    elif colorUpper == 'U':
        return 1
    elif colorUpper == 'B':
        return 2
    elif colorUpper == 'R':
        return 3
    elif colorUpper == 'G':
        return 4
    elif colorUpper == 'C':
        return 5

def makeCardsDb(mtgJsonFile, externalDir, cardDb):
    cardOut = {}
    getData = ['firstPrinting', 'colors', 'points', 'types', 'manaValue']
    phprList = ['Arena', 'Sewers of Estark', 'Windseeker Centaur', 'Giant Badger', 'Mana Crypt']
    errors = {}
    with open(Path(externalDir + 'pointsList.json')) as file:
        pointsListDict = json.loads(file.read())
    with open(mtgJsonFile) as file:
        loadedFile = json.loads(file.read())
        for card in tqdm(loadedFile['data'], desc='Loading cards', unit=' cards'):
            cardAttributes = {}
            for attribute in getData:
                try:
                    if attribute == 'firstPrinting' and (card in phprList):
                        cardAttributes[attribute] = 'PHPR'
                    elif attribute == 'colors':
                        cardAttributes[attribute] = sorted(loadedFile['data'][card][0][attribute], key=sortColorsWUBRG)
                    elif attribute == 'points':
                        if card in pointsListDict:
                            cardAttributes[attribute] = pointsListDict[card]
                        else:
                            cardAttributes[attribute] = '0 points'
                    else:
                        cardAttributes[attribute] = loadedFile['data'][card][0][attribute]
                except KeyError:
                    #1.9.24
                    #Errors are limited to art cards, should fix this as they may be used as proxies?
                    cardAttributes[attribute] = ''
                    if card not in errors:
                        errorAttributes = {}
                        errorAttributes['frequency'] = 1
                        errorAttributes['cardAttribute'] = attribute
                        errors[card] = errorAttributes
                    else:
                        errorAttributes = errors[card]
                        errorAttributes['frequency'] += 1
                        errors[card] = errorAttributes
            cardOut[card] = cardAttributes
    try:
        conn = sqlite3.connect(cardDb)
        cur = conn.cursor()
        cur.execute("DELETE FROM cards")
        #add all cards in cardOut to cardDb
        for card in tqdm(cardOut, desc='Saving cards', unit=' cards'):
            fp = cardOut[card]['firstPrinting']
            color = str(cardOut[card]['colors']).replace('[', '').replace("'", '').replace(']', '').replace(', ','')
            points = int(cardOut[card]['points'][0])
            types = str(cardOut[card]['types']).replace('[', '').replace("'", '').replace(']', '').replace('Tribal', 'Kindred')
            mv = cardOut[card]['manaValue']
            cur.execute("INSERT INTO cards (name, firstprinting, colors, points, types, manavalue) VALUES (?, ?, ?, ?, ?, ?)", (card, fp, color, points, types, mv))
        conn.commit()
    finally:
        conn.close()
            
def downloadMtgJson(path, updatePath, cardDb):
    ##to-do: add a progress bar to this download, current options leave me with file I can't open correctly in the code
    print("Downloading AtomicCards.json.")
    mtgjsonUrl = "https://mtgjson.com/api/v5/AtomicCards.json"
    r = requests.get(mtgjsonUrl)
    j = json.loads(r.text)
    path.parent.mkdir(exist_ok=True, parents=True)
    with open(path, 'w') as file:
        json.dump(j, file)
    updatePath.parent.mkdir(exist_ok=True, parents=True)
    date = datetime.today().strftime('%Y-%m-%d')
    with open(updatePath, 'w') as file:
        json.dump(date, file) #used to use j['meta']['date']
    mtgjsonSetListUrl = 'https://mtgjson.com/api/v5/SetList.json'
    r = requests.get(mtgjsonSetListUrl)
    j = json.loads(r.text)
    try:
        conn = sqlite3.connect(cardDb)
        cur = conn.cursor()
        tempSets = cur.execute("SELECT setcode FROM sets").fetchall()
        sets = []
        i = 0
        while i < len(tempSets):
            sets.append(tempSets[i][0])
            i += 1
        for item in j['data']:
            if item['code'] in sets:
                cur.execute("UPDATE sets SET setname = ?, releasedate = ? WHERE setcode = ?", (item['name'], item['releaseDate'], item['code']))
            else:
                cur.execute("INSERT INTO sets VALUES (?, ?, ?)", (item['code'], item['name'], item['releaseDate']))
        conn.commit()
    finally:
        conn.close()

def mtgJsonFetch(externalDir, cardDb):
    print("Grabbing mtgjson data.")
    mtgJsonFile = Path(externalDir + 'AtomicCards.json')
    mtgJsonFileLastUpdate = Path(externalDir + 'AtomicCardsLastUpdate.json')
    needNewMtgJsonFile = False
    if mtgJsonFile.is_file():
        if mtgJsonFileLastUpdate.is_file():
            with open(mtgJsonFileLastUpdate) as f:
                fileDate = json.loads(f.read())
                if datetime.today().strftime('%Y-%m-%d') != fileDate:
                    needNewMtgJsonFile = True
                else:
                    print("mtgjson file is already up-to-date.")
        else:
            needNewMtgJsonFile = True
    else:
        needNewMtgJsonFile = True
    if needNewMtgJsonFile:
        downloadMtgJson(mtgJsonFile, mtgJsonFileLastUpdate, cardDb)
        return mtgJsonFile
    else:
        return mtgJsonFile

def fetchDecks(fetchDecksList, cardDb):
    if fetchDecksList != []:
        #start with the oldest new deck to prevent issues if script is interrupted while fetching decks
        fetchDecksList.reverse()
        try:
            conn = sqlite3.connect(cardDb)
            cur = conn.cursor()
            #load decks list
            tempDecksList = cur.execute("SELECT deckid FROM decks").fetchall()
            decksList = []
            i = 0
            while i < len(tempDecksList):
                decksList.append(tempDecksList[i][0])
                i += 1
            #load staledecks list
            tempStaleDecksList = cur.execute("SELECT deckid FROM staledecks").fetchall()
            staleDecksList = []
            i = 0
            while i < len(tempStaleDecksList):
                staleDecksList.append(tempStaleDecksList[i][0])
                i += 1
            ##change pointsList to database table
            with open(Path('./data/external/pointsList.json')) as file:
                pointsList = json.loads(file.read())
            for deck in tqdm(fetchDecksList, desc='Grabbing decks', unit=' decks'):
                deckLastUpdate = deck['lastUpdatedAtUtc']
                if deck['id'] in decksList:
                    checkUpdateTemp = cur.execute("SELECT lastupdate FROM decks WHERE deckid = ?", (deck['id'],)).fetchone()
                    checkUpdate = checkUpdateTemp[0]
                    if deckLastUpdate == checkUpdate:
                        update = False
                        continue
                    else:
                        update = True
                else:
                    update = False
                if deck['id'] in staleDecksList:
                    cur.execute("DELETE FROM staledecks WHERE deckid = ?", (deck['id'],))
                deckUrl = ("https://api2.moxfield.com/v3/decks/all/" + deck['publicId'])
                rGetCards = requests.get(deckUrl, headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
                jGetCards = json.loads(rGetCards.text)
                deckId = jGetCards['id']
                lastUpdate = jGetCards['lastUpdatedAtUtc']
                deckName = jGetCards['name']
                createdby = jGetCards['createdByUser']['userName']
                publicurl = jGetCards['publicUrl']
                cardsList = []
                deckPoints = []
                for cardId in jGetCards['boards']['mainboard']['cards']:
                    name = jGetCards['boards']['mainboard']['cards'][cardId]['card']['name']
                    cardsList.append(name)
                    if name in pointsList:
                        deckPoints.append(name.replace("'", '"'))
                cards = ',,'.join(cardsList) #used ',,' to handle cards with ', ' in the name
                points = ',,'.join(deckPoints)
                if not update == True:
                    cur.execute("INSERT INTO decks (deckid, lastupdate, name, createdby, publicurl, cards, points) VALUES (?, ?, ?, ?, ?, ?, ?)", (deckId, lastUpdate, deckName, createdby, publicurl, cards, points))
                    decksList.append(deck['id'])
                else:
                    cur.execute("UPDATE decks SET lastupdate = ?, name = ?, createdby = ?, publicurl = ?, cards = ?, points = ? WHERE deckid = ?", (lastUpdate, deckName, createdby, publicurl, cards, points, deckId))
            conn.commit()
        finally:
            conn.close()

def fetchDecksList(cardDb):
    decksList = [] # list of decks
    try:
        conn = sqlite3.connect(cardDb)
        cur = conn.cursor()
        #load decks list
        tempDecksList = cur.execute("SELECT deckid, lastupdate FROM decks").fetchall()
        existingDecksDict = {}
        i = 0
        while i < len(tempDecksList):
            existingDecksDict[tempDecksList[i][0]] = tempDecksList[i][1]
            i += 1
    finally:
        conn.close()
    for i in tqdm(range(0,100), desc='Grabbing pages of decks', unit=' pages'):
        #grab deck info from each page
        getDecksUrl = ("https://api2.moxfield.com/v2/decks/search?pageNumber="
                       + str(i)
                       + "&pageSize=100&sortType=updated&sortDirection=Descending&fmt=highlanderCanadian")
        rGetDecks = requests.get(getDecksUrl, headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        jGetDecks = json.loads(rGetDecks.text)
        #put the deck data into decksList
        deckKeys = list(existingDecksDict.keys())
        for deck in jGetDecks['data']:
            if deck['id'] in deckKeys:
                if deck['lastUpdatedAtUtc'] == existingDecksDict[deck['id']]:
                    #continue #could use continue here to grab decks not previously public
                    return decksList
            decksList.append(deck)
    return decksList

def createTables(cardDb):
    try:
        conn = sqlite3.connect(cardDb)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS cards (name TEXT PRIMARY KEY NOT NULL, firstprinting TEXT, colors TEXT, points INTEGER, types TEXT, manavalue INTEGER)")
        cur.execute("CREATE TABLE IF NOT EXISTS frequency (name TEXT PRIMARY KEY NOT NULL)")
        cur.execute("CREATE TABLE IF NOT EXISTS deckscount (date TEXT PRIMARY KEY NOT NULL, deckcount INTEGER, wacount INTEGER, quarterdeckcount INTEGER, quarterwacount INTEGER, monthdeckcount INTEGER, monthwacount INTEGER, sincepointsdeckcount INTEGER, sincepointswacount INTEGER)")
        cur.execute("CREATE TABLE IF NOT EXISTS pointsspreads (spread TEXT PRIMARY KEY NOT NULL)")
        cur.execute("CREATE TABLE IF NOT EXISTS decks (deckid TEXT PRIMARY KEY NOT NULL, lastupdate TEXT, name TEXT, createdby TEXT, publicurl TEXT, cards TEXT, points TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS staledecks (deckid TEXT PRIMARY KEY NOT NULL, lastupdate TEXT, name TEXT, createdby TEXT, publicurl TEXT, cards TEXT, points TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS waevents (deckid TEXT PRIMARY KEY NOT NULL, date TEXT, name TEXT, event TEXT, publicurl TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS sets (setcode TEXT PRIMARY KEY NOT NULL, setname TEXT, releasedate TEXT)")
        conn.commit()
    finally:
        conn.close()

def run():
    ##to-do: turn all external files (other than mtgjson) into tables in the db
    ##to-do: add number of new/edited decks (number of decks fetched)
    ##to-do: add better mtgjson check using mtgjson's meta file
    externalDir = './data/external/'
    cardDb = './data/cards.db'
    createTables(cardDb)
    runDecksList = fetchDecksList(cardDb)
    fetchDecks(runDecksList, cardDb)
    mtgJsonFile = mtgJsonFetch(externalDir, cardDb)
    makeCardsDb(mtgJsonFile, externalDir, cardDb)
    output = processDecks(externalDir, cardDb)
    saveData(output, cardDb)

if __name__ == '__main__':
    run()
