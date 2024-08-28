import json
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import shutil
from tqdm import tqdm
import csv
import re

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

def amendWaDeckToCsv(path, data):
    '''
    var path must be a Path object
    var data should be a list containing items meant to be in a row
    '''
    if path.is_file():
        with open(path, 'a', newline='') as fileWrite:
            writer = csv.writer(fileWrite)
            writer.writerow(data)
    else:
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, 'w', newline='') as fileWrite:
            writer = csv.writer(fileWrite)
            writer.writerow(['Created Date', 'Deck Name', 'Event', 'Deck URL'])
            writer.writerow(data)

def waDeckToCsv(deck):
    savePath = Path('./data/wa/waDecks.csv')
    savePath.parent.mkdir(exist_ok=True, parents=True)
    name = deck['name']
    #eventlist= ['Discord Cockatrice','Discord Webcam','Discord LCQ','Geek Fortress','Nottingham','Cosmic Games','PortLander','Async','MTGO Async','Newmarket','Toronto']
    r = re.compile('\d{4}/\d{2}/\d{2}')
    if r.match(name[:10]) is not None: #if the first 10 characters are like 2023/12/31
        name = name[13:]
    eventIndex = name.rfind('(')
    if eventIndex >= 0: #if ( is found in name
        eventString = name[eventIndex:]
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
    data = [deck['createdAtUtc'][:10],name,eventString,deck['publicUrl']]
    amendWaDeckToCsv(savePath, data)
    eventPath = Path('./data/wa/events/' + eventString + '.csv')
    amendWaDeckToCsv(eventPath, data)
    
def processData(dataDir, loadedAtomicCards):
    with open(Path('./data/external/pointsList.json')) as file:
        pointsList = json.loads(file.read())
    with open(Path('./data/external/meldDict.json')) as file:
        meldDict = json.loads(file.read())
    phprList = ['Arena', 'Sewers of Estark', 'Windseeker Centaur', 'Giant Badger', 'Mana Crypt']
    dataDecksDir = dataDir + "/decks"
    decksToProcess = list(Path(dataDecksDir).glob('*.json'))
    allCardsData = {}
    getData = ['firstPrinting', 'colors', 'types', 'manaValue']
    errors = {}
    decksToArchive = []
    pointsSpreadFrequency = {}
    wa = False
    if dataDir == './data/wa':
        wa = True
    if wa:
        processingDesc = 'Processing wa decks'
    else:
        processingDesc = 'Processing decks'
    for deck in tqdm(decksToProcess, desc=processingDesc, unit=' decks'):   
        with open(deck) as loadedDeck:
            g = json.loads(loadedDeck.read()) #puts file contents into dict g
            try:
                deckLastUpdate = g['lastUpdatedAtUtc'][:10]
            except KeyError:
                print(g)
            oneYearAgo = datetime.now() - relativedelta(years=1)
            oneQuarterAgo = datetime.now() - relativedelta(months=3)
            oneMonthAgo = datetime.now() - relativedelta(months=1)
            if datetime.strptime(deckLastUpdate, '%Y-%m-%d') < oneYearAgo:
                decksToArchive.append(g['id'])
                continue
            deckPoints = []
            if wa:
                waDeckToCsv(g)
            for cardId in g['boards']['mainboard']['cards']:
                try:
                    name = g['boards']['mainboard']['cards'][cardId]['card']['name']
                    if name.startswith("A-"): #turns Alchemy-changed cards into their paper name (the Alchemy versions shouldn't be in canlander decks anyways)
                        name = name.replace("A-", "")
                    if name in meldDict:
                        name = meldDict[name]
                    if name in pointsList:
                        deckPoints.append(name.replace("'", '"')) #record points spread for each deck
                    if name not in allCardsData:
                        cardAttributes = {} #add card to allCards Data
                        cardAttributes['frequency'] = 1
                        if datetime.strptime(deckLastUpdate, '%Y-%m-%d') >= oneQuarterAgo:
                            cardAttributes['quarterFrequency'] = 1
                        else:
                            cardAttributes['quarterFrequency'] = 0
                        if datetime.strptime(deckLastUpdate, '%Y-%m-%d') >= oneMonthAgo:
                            cardAttributes['monthFrequency'] = 1
                        else:
                            cardAttributes['monthFrequency'] = 0
                        if name in pointsList:
                            cardAttributes['points'] = pointsList[name]
                        else:
                            cardAttributes['points'] = 0
                        for attribute in getData:
                            if attribute == 'firstPrinting' and (name in phprList):
                                cardAttributes[attribute] = 'PHPR'
                            elif attribute == 'colors':
                                cardAttributes[attribute] = sorted(loadedAtomicCards['data'][name][0][attribute], key=sortColorsWUBRG)
                            else:
                                cardAttributes[attribute] = loadedAtomicCards['data'][name][0][attribute]
                        allCardsData[name] = cardAttributes
                    else:
                        cardAttributes = allCardsData[name] #set cardAttributes to what's already been pulled
                        cardAttributes['frequency'] += 1 #add one to cardAttributes['frequency']
                        if datetime.strptime(deckLastUpdate, '%Y-%m-%d') >= oneQuarterAgo:
                            cardAttributes['quarterFrequency'] += 1
                        if datetime.strptime(deckLastUpdate, '%Y-%m-%d') >= oneMonthAgo:
                            cardAttributes['monthFrequency'] += 1
                        allCardsData[name] = cardAttributes #set allCardsData[name] to changed cardAttributes
                except KeyError: 
                #errors at 12/24/23 are:
                #The Initiative // Undercity (token, do not fix)
                #Needleverge Pathway // Needleverge Pathway (art card, do not fix)
                #Reidane, God of the Worthy // Reidane, God of the Worthy (art card, do not fix)
                    if name not in errors:
                        errorAttributes = {}
                        errorAttributes['frequency'] = 1
                        errorAttributes['firstDeckId'] = g['id']
                        errorAttributes['firstDeckUrl'] = g['publicUrl']
                        errorAttributes['cardAttribute'] = attribute
                        errors[name] = errorAttributes
                    else:
                        errorAttributes = errors[name]
                        errorAttributes['frequency'] += 1
                        errors[name] = errorAttributes    
        saveDeckPoints = str(deckPoints)
        if saveDeckPoints not in pointsSpreadFrequency: #if points spread not recorded before
            pointsSpreadFrequency[saveDeckPoints] = 1
        else:
            pointsSpreadFrequency[saveDeckPoints] += 1
    print("Saving frequency data")
    savePath = Path(dataDir + "/allCardsData.json")
    savePath.parent.mkdir(exist_ok=True, parents=True)
    with open(savePath, "w") as outfile:
        json.dump(allCardsData, outfile)
    print("Saving points spread data")
    savePath = Path(dataDir + '/pointsSpreads.json')
    savePath.parent.mkdir(exist_ok=True, parents=True)
    with open(savePath, "w") as outfile:
        json.dump(pointsSpreadFrequency, outfile)
    print("Saving error data")
    savePath = Path(dataDir + "/errors/processingErrors.json")
    savePath.parent.mkdir(exist_ok=True, parents=True)
    with open(savePath, "w") as outfile:
        json.dump(errors, outfile)
    savePath = Path(dataDir + "/archive/decks/")
    savePath.mkdir(exist_ok=True, parents=True)
    if dataDir == './data/wa':
        archiveDesc = 'Archiving old wa decks'
    else:
        archiveDesc = 'Archiving old decks'
    for stale in tqdm(decksToArchive, desc=archiveDesc, unit=' decks'):
        fileSource = (dataDir + "/decks/" + stale + ".json")
        fileDest = (dataDir + "/archive/decks/" + stale + ".json")
        shutil.move(fileSource, fileDest)

def runDataProcessing():
    savedDataDir = './data'
    waDataDir = './data/wa'
    waDecks = Path('./data/wa/waDecks.csv')
    existingEvents = Path('./data/wa/events')
    #delete old waDecks.csv file, shouldn't be necessary but here just in case
    if waDecks.is_file():
        waDecks.unlink()
    #delete old events lists
    if existingEvents.exists():
        shutil.rmtree(existingEvents, ignore_errors=True)
    print("Loading atomicCards.json")
    with open('./data/external/AtomicCards.json') as atomicCardsFile:
        loadedAtomicCards = json.loads(atomicCardsFile.read())
        processData(savedDataDir, loadedAtomicCards)
        processData(waDataDir, loadedAtomicCards)
    #add archived wa decks to waDecks and events csvs (this is untested as there are no current archived WA decks)
    archivedWaDecks = list(Path('./data/wa/archive/decks').glob('*.json'))
    for deck in archivedWaDecks:
        with open(deck) as f:
            g = json.loads(f.read())
            waDeckToCsv(g)
    print("Sorting waDecks.csv")
    with open(waDecks) as file:
        reader = csv.reader(file)
        sortedlist = sorted(reader, key=lambda row: row[0], reverse=True)
    with open('./data/wa/waDecksSorted.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        for row in sortedlist:
            writer.writerow(row)
    #delete new waDecks.csv file
    if waDecks.is_file():
        waDecks.unlink()
    #sort csvs in events folder
    eventFiles = list(Path('./data/wa/events').glob('*.csv'))
    for event in eventFiles:
        with open(event) as file:
            reader = csv.reader(file)
            sortedlist = sorted(reader, key=lambda row: row[0], reverse=True)
        eventSorted = './data/wa/events/' + str(Path(event).stem) + 'Sorted.csv'
        with open(eventSorted, 'w', newline='') as file:
            writer = csv.writer(file)
            for row in sortedlist:
                writer.writerow(row)
        deleteOld = Path(event)
        if deleteOld.is_file():
            deleteOld.unlink()
    
if __name__ == '__main__':
    runDataProcessing()
