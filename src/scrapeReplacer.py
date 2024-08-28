import requests
import json
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import shutil
from tqdm import tqdm

def downloadMtgJson(path, updatePath):
    print("Downloading AtomicCards.json")
    mtgjsonUrl = "https://mtgjson.com/api/v5/AtomicCards.json"
    r = requests.get(mtgjsonUrl)
    j = json.loads(r.text)
    path.parent.mkdir(exist_ok=True, parents=True)
    with open(path, 'w') as file:
        json.dump(j, file)
    updatePath.parent.mkdir(exist_ok=True, parents=True)
    with open(updatePath, 'w') as file:
        json.dump(j['meta']['date'], file)

def getDecks():
    getDecksList = [] # list of decks
    for i in tqdm(range(0,100), desc='Grabbing pages of decks', unit=' pages'):
        #grab deck info from each page
        getDecksUrl = ("https://api2.moxfield.com/v2/decks/search?pageNumber="
                       + str(i)
                       + "&pageSize=100&sortType=updated&sortDirection=Descending&fmt=highlanderCanadian")
        rGetDecks = requests.get(getDecksUrl, headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        jGetDecks = json.loads(rGetDecks.text)
        #put the deck data into decksList
        for deck in jGetDecks['data']:
            deckFile = Path('./data/decks/' + deck['id'] + ".json")
            if deckFile.is_file():
                with open(deckFile) as checkDeck:
                    check = json.loads(checkDeck.read())
                    if deck['lastUpdatedAtUtc'] == check['lastUpdatedAtUtc']:
                        #return at first deck already in data set
                        deckNumber = str(len(getDecksList))
                        if deckNumber == 0 or deckNumber == 1: #this if/else is entirely for output consistency, need new line if tqdm ends too early
                            print("\nFound " + deckNumber + " decks before finding existing, unupdated deck.", flush=True) #testing flush=True for issue with this printing after grabbing decsk tqdm
                        else:
                            print("Found " + deckNumber + " decks before finding existing, unupdated deck.", flush=True)
                        return getDecksList
            getDecksList.append(deck)
    return list(reversed(getDecksList))

def runScrapeReplacer():
    decksList = getDecks()
    ''' pretty sure this section is never used, commenting out 1/9/24 to ensure it isn't necessary before totally removing
    #record decks data    
    print("Saving list of decks to file.")
    saveLists = Path("./data/decksList.json")
    saveLists.parent.mkdir(exist_ok=True, parents=True)
    with open(saveLists, "w") as outfile:
        json.dump(decksList, outfile)
    '''
    #grab cards for each deck list
    if decksList != []:
        for deck2 in tqdm(decksList, desc='Grabbing decks', unit=' decks'):
            #check if deck is stale (>1yr old)
            deckLastUpdate = deck2['lastUpdatedAtUtc'][:10]
            oneYearAgo = datetime.now() - relativedelta(years=1)
            if datetime.strptime(deckLastUpdate, '%Y-%m-%d') < oneYearAgo:
                continue
            #check if deck in data set and not updated
            deckFile = Path('./data/decks/' + deck2['id'] + ".json")
            if deckFile.is_file():
                with open(deckFile) as checkDeck:
                    check = json.loads(checkDeck.read())
                    if deck2['lastUpdatedAtUtc'] == check['lastUpdatedAtUtc']:
                        continue
            #grab deck
            deckUrl = ("https://api2.moxfield.com/v3/decks/all/" + deck2['publicId'])
            rGetCards = requests.get(deckUrl, headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
            jGetCards = json.loads(rGetCards.text)
            #saving each deck to a file
            deckFile.parent.mkdir(exist_ok=True, parents=True)
            with open(deckFile, "w") as outfile:
                json.dump(jGetCards, outfile)
            #copy WA decks to WA directory
            if deck2['createdByUser']['userName'] == "CanlanderWinnersArchive":
                copyDeck = Path("./data/wa/decks/" + deck2['id'] + '.json')
                copyDeck.parent.mkdir(exist_ok=True, parents=True)
                shutil.copyfile(deckFile, copyDeck)
    #grab mtgJson data
    print("Grabbing mtgjson data")
    mtgjsonFile = Path('./data/external/AtomicCards.json')
    mtgjsonFileLastUpdate = Path('./data/external/AtomicCardsLastUpdate.json')
    needNewMtgJsonFile = False
    if mtgjsonFile.is_file():
        if mtgjsonFileLastUpdate.is_file():
            with open(mtgjsonFileLastUpdate) as f:
                fileDate = json.loads(f.read())
                if datetime.today().strftime('%Y-%m-%d') != fileDate:
                    needNewMtgJsonFile = True
                else:
                    print("mtgjson file is already up-to-date")
        else:
            needNewMtgJsonFile = True
    else:
        needNewMtgJsonFile = True
    if needNewMtgJsonFile:
        downloadMtgJson(mtgjsonFile, mtgjsonFileLastUpdate)
    
if __name__ == '__main__':
    runScrapeReplacer()
