import json
from pathlib import Path
import csv
from datetime import datetime
import shutil
import pandas as pd
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from canlanderDBTools import create_connection, make_sql_table
import urllib

def addDateToCsvs(directory):
    fileList = list(Path(directory).rglob('*.csv'))
    for item in fileList:
        with open(item, 'a', newline='') as fileWrite:
            writer = csv.writer(fileWrite)
            writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M")])

def cardToCsv(attribute, file, dataDir, finalFrequencyDict, card):
    saveData = []
    savePath = Path(dataDir + "/files/" + attribute + "/" + file + ".csv")
    saveData.append(card)
    saveData.append(finalFrequencyDict[card]['frequency'])
    ##testing adding month and quarter frequency
    saveData.append(finalFrequencyDict[card]['quarterFrequency'])
    saveData.append(finalFrequencyDict[card]['monthFrequency'])
    savePath.parent.mkdir(exist_ok=True, parents=True)
    if savePath.is_file():
        with open(savePath, 'a', newline='') as fileWrite:
            writer = csv.writer(fileWrite)
            writer.writerow(saveData)
    else:
        with open(savePath, 'w', newline='') as fileWrite:
            writer = csv.writer(fileWrite)
            writer.writerow(['cardName', 'frequency', 'quarterFrequency','monthFrequency'])
            writer.writerow(saveData)

def compareRanks(df, fileArchive, diff, card, dataDir):
    logKeyErrors = {}
    strippedDf = df.iloc[:,[1,2]]
    strippedDict = strippedDf.set_index(strippedDf.columns[1]).T.to_dict() #using column index 1, since name of what we're ranking isn't always 'cardName'
    with open(fileArchive, encoding='utf-8') as oldFile: #I don't know why this open statement is different, but adding the encoding fixed the error with accented letters so...
        oldDf = pd.read_csv(oldFile)
        oldStrippedDf  = oldDf.iloc[:,[1,2]]
        oldStrippedDict = oldStrippedDf.set_index(oldStrippedDf.columns[1]).T.to_dict()
        for card in strippedDict:
            try:
                #negative = falling (higher rank number), postive = rising (lower rank number)
                strippedDict[card] = oldStrippedDict[card]['rankDense'] - strippedDict[card]['rankDense']
            except KeyError:
                strippedDict[card] = 'new '
                logKeyErrors[card] = [fileArchive, diff]
        dfTransfer = pd.DataFrame([strippedDict]).T.reset_index()
        df[diff] = dfTransfer[0]
    #make errors log if any errors occur
    if not logKeyErrors == []:
        errorSave = Path(dataDir + '/errors/compareRanksKeyErrors.csv')
        errorSave.parent.mkdir(exist_ok=True, parents=True)
        if errorSave.is_file():
            with open(errorSave, 'a', newline='') as outfile:
                writer = csv.writer(outfile)
                for card in logKeyErrors:
                    writer.writerow([card, logKeyErrors[card]])
        else:
            with open(errorSave, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                for card in logKeyErrors:
                    writer.writerow([card, logKeyErrors[card]])
    return df

def cardToUrlName(text):
    #this breaks if any columns in the .csv file doesn't have a name (for the df)
    text = urllib.parse.quote(text)
    return(text)

def spreadToUrlName(text):
    text = urllib.parse.quote(text.replace('[', '').replace("'", '').replace(',', '').replace(']', ''))
    if text == '':
        text == 'No%20Points'
    return(text)

def outputFiles(dataDir):
    #deleting old data
    existingData = Path(dataDir + "/files")
    if existingData.exists():
        print("Deleting existing output data")
        shutil.rmtree(existingData, ignore_errors=True) #ignoring errors probably not great but this should get around the folder issue I'm having with the auto-back up
    existingErrors = Path(dataDir + '/errors/compareRanksKeyErrors.csv')
    if existingErrors.is_file():
        print("Deleting existing error data")
        existingErrors.unlink()
    #load allCardsData for sorting
    allCardsData = Path(dataDir + '/allCardsData.json')
    with open(allCardsData) as f:
        g = json.loads(f.read()) #puts file contents into dict g
        frequencyDict = {}
        for card in g:
            frequencyDict[card] = g[card]['frequency']
        #sorting by frequency (highest) and card name
        sortedAlphaKey = dict(sorted(frequencyDict.items()))
        sortedFrequencyDict = dict(sorted(sortedAlphaKey.items(), key=lambda x:x[1], reverse=True))
        finalFrequencyDict = {}
        for card in sortedFrequencyDict:
            finalFrequencyDict[card] = g[card]
        print("Saving sorted data", flush=True)
        saveData = Path(dataDir + "/files/allCardsSorted.csv")
        saveData.parent.mkdir(exist_ok=True, parents=True)
        with open(saveData, "w", newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['cardName', 'frequency', 'quarterFrequency', 'monthFrequency'])
            for card in finalFrequencyDict:
                writer.writerow([card, finalFrequencyDict[card]['frequency'], finalFrequencyDict[card]['quarterFrequency'], finalFrequencyDict[card]['monthFrequency']])
    #iterate through all cards, placing card and frequency in appropriate output files
    pointedSort = {}  
    if dataDir == './data/wa':
        sortDesc = 'Sorting wa output'
    else:
        sortDesc = 'Sorting output'
    for card in tqdm(finalFrequencyDict, desc=sortDesc, unit=' cards'):
        for attribute in finalFrequencyDict[card]:
            if attribute == "frequency" or attribute == 'quarterFrequency' or attribute == 'monthFrequency':
                continue
            elif attribute == 'points':
                if finalFrequencyDict[card][attribute] == 0:
                    continue
                else:
                    file = str(finalFrequencyDict[card][attribute])
                    pointedSort[card] = [finalFrequencyDict[card]['frequency'], finalFrequencyDict[card]['quarterFrequency'], finalFrequencyDict[card]['monthFrequency']]
            elif attribute == 'firstPrinting':
                file = finalFrequencyDict[card][attribute]
            elif attribute == 'colors':
                if finalFrequencyDict[card][attribute] == []:
                    file = "C"
                else:
                    file = str(finalFrequencyDict[card][attribute]).replace('[', '').replace("'", '').replace(', ', '').replace(']', '')
            elif attribute == 'types':
                for cardType in finalFrequencyDict[card][attribute]:
                    file = cardType
                    cardToCsv(attribute, file, dataDir, finalFrequencyDict, card)
                continue
            elif attribute == 'manaValue':
                file = str(finalFrequencyDict[card][attribute])[0]
            cardToCsv(attribute, file, dataDir, finalFrequencyDict, card)       
    print("Saving sorted points list data")
    saveData = Path(dataDir + "/files/pointedCardsSorted.csv")
    saveData.parent.mkdir(exist_ok=True, parents=True)
    with open(saveData, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['cardName', 'frequency', 'quarterFrequency', 'monthFrequency'])
        for card in pointedSort:
            writer.writerow([card, pointedSort[card][0], pointedSort[card][1],  pointedSort[card][2]])
    #sort points spreads
    with open(dataDir + "/pointsSpreads.json") as f:
        g = json.loads(f.read())
        sortedAlphaKey = dict(sorted(g.items()))
        sortedFrequencyDict = dict(sorted(sortedAlphaKey.items(), key=lambda x:x[1], reverse=True))
        print("Saving sorted spreads")
        saveData = Path(dataDir + "/files/spreadsSorted.csv")
        saveData.parent.mkdir(exist_ok=True, parents=True)
        with open(saveData, "w", newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(["spread", 'frequency'])
            for spread in sortedFrequencyDict:
                writer.writerow([spread, sortedFrequencyDict[spread]])
    #adding % of here and % of decks, output files to csv and sql
    savedFilesDir = dataDir + '/files'
    dataR = list(Path(savedFilesDir).rglob('*.csv'))
    savedDecksDir = dataDir + '/decks'
    numDecks = len(list(Path(savedDecksDir).glob('*.json')))
    if dataDir == './data/wa':
        fileDesc = 'Exporting to wa files'
    else:
        fileDesc = 'Exporting to files'
    for file in tqdm(dataR, desc=fileDesc, unit=' files'):
        with open(file) as importCsv:
            df = pd.read_csv(importCsv)
            df['percent_of_here'] = (df['frequency']/df['frequency'].sum()).mul(100).round(2).astype(str) + '%'
            df['percent_of_decks'] = (df['frequency']/numDecks).mul(100).round(2).astype(str) + '%'
            df.insert(0, 'rank', df['frequency'].rank(method='min', ascending=False))
            df.insert(1, 'rankDense', df['frequency'].rank(method='dense', ascending=False))
            #adding urlName, spreads have to be handled differently
            if str(Path(file).name) == 'spreadsSorted.csv':
                ##this is a short-term solution, figure out ids using shortuuid and a .json file to save ids
                df['urlName'] = df['spread'].apply(spreadToUrlName)
                '''
                ##add id for spreads? - this is a project for later
                spreadIds = Path('./data/spreadIds.json')
                if spreadIds.is_file():
                    with open spreadIds as f:
                        g = json.loads(f.read())
                '''            
            else:
                df['urlName'] = df['cardName'].apply(cardToUrlName)
            #add change in frequency over given periods of time
            timeDiff = {}
            yesterday = datetime.now() - relativedelta(days=1)
            timeDiff['dayChange'] = yesterday.strftime("%Y-%m-%d")
            lastWeek = datetime.now() - relativedelta(days=7)
            timeDiff['weekChange'] = lastWeek.strftime("%Y-%m-%d")
            lastMonth = datetime.now() - relativedelta(months=1)
            timeDiff['monthChange'] = lastMonth.strftime("%Y-%m-%d")
            lastQuarter = datetime.now() - relativedelta(months=3)
            timeDiff['quarterChange'] = lastQuarter.strftime("%Y-%m-%d")
            lastYear = datetime.now() - relativedelta(years=1)
            timeDiff['yearChange'] = lastYear.strftime("%Y-%m-%d")
            for diff in timeDiff:
                archiveDate = Path(dataDir + '/archive/' + timeDiff[diff])
                if archiveDate.exists():
                    fileArchive = Path(dataDir + '/archive/' + timeDiff[diff] + "/" + Path(file).name)
                    if fileArchive.exists():
                        df = compareRanks(df, fileArchive, diff, card, dataDir)
                    else:
                        dataPath = Path(dataDir + '/archive/' + timeDiff[diff])
                        dirList = [f for f in dataPath.iterdir() if f.is_dir()]
                        for directory in dirList:
                            fileArchive = Path(str(directory) + '/' + Path(file).name)
                            if fileArchive.exists():
                                df = compareRanks(df, fileArchive, diff, card, dataDir)
                                break
        df.to_csv(file, index=False)
        tableName = Path(file).stem
        #output to sql database
        '''
        dbFile = './data/frequency.db'
        conn = create_connection(dbFile)
        if str(Path(file).name) == 'spreadsSorted.csv':
            try:
                if dataDir == './data/wa':
                    tableName = 'WinArc' + tableName
                df.to_sql(tableName, conn, if_exists='replace', index=False)
            finally:
                if conn:
                    conn.close()
        else:
            with open('./data/external/replaceTableNames.json') as f:
                replaceTableNames = json.loads(f.read())
            foreignKey = 'cardName'
            foreignKeyRef = 'cards'
            foreignKeyRefCol = 'cardName'
            if tableName in replaceTableNames: #to deal with csv names that cause errors as table names
                tableName = replaceTableNames[tableName]
            if dataDir == './data/wa':
                tableName = 'WinArc' + tableName
            try:
                make_sql_table(conn, df, pk=None, fk=foreignKey, fk_ref=foreignKeyRef, fk_ref_col=foreignKeyRefCol, tbl_name=tableName)
            finally:
                if conn:
                    conn.close()
            '''
    archiveData = Path(dataDir + "/archive/" + datetime.now().strftime("%Y-%m-%d"))
    print("Saving data to archive: " + str(archiveData))
    if archiveData.exists():
        shutil.rmtree(archiveData, ignore_errors=True)
    shutil.copytree(savedFilesDir, archiveData, dirs_exist_ok=True)

def runFileOutput():
    savedDataDir = './data'
    waDataDir = './data/wa'
    #delete old database file
    existingDB = Path(savedDataDir + '/frequency.db')
    if existingDB.is_file() and savedDataDir and waDataDir:
        existingDB.unlink()
    #add allCardsData to sql database as cards table
    try:
        ##make a literallyAllCardsData.json in data processing and use that here so cards outside the database can still be viewed with relevant attributes
        allCardsData = Path('./data/allCardsData.json')
        with open(allCardsData) as file:
            f = json.loads(file.read())
            df = pd.DataFrame.from_dict(f, orient='index')
        df = df.reset_index()
        df.rename(columns={'index':'cardName'}, inplace=True)
        for column in df.columns:
            if df[column].dtypes == 'object':
                df[column] = df[column].astype('str')
        tableName = 'cards'
        dbFile = savedDataDir + '/frequency.db'
        conn = create_connection(dbFile)
        make_sql_table(conn, df, pk='cardName', tbl_name=tableName)
    finally:
        if conn:
            conn.close()
    outputFiles(savedDataDir)
    outputFiles(waDataDir)
    #save database to archive
    shutil.copy(savedDataDir + '/frequency.db', savedDataDir + '/archive/' + datetime.now().strftime("%Y-%m-%d"))
    addDateToCsvs(savedDataDir + '/files')
    addDateToCsvs(waDataDir + '/files')

if __name__ == '__main__':    
    runFileOutput()
