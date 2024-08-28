import scrapeReplacer
import dataProcessing
import fileOutput
import oneFileWIP

if __name__ == '__main__':
    scrapeReplacer.runScrapeReplacer()
    dataProcessing.runDataProcessing()
    fileOutput.runFileOutput()
    #oneFileWIP is an attempt to use an SQLite DB instead of JSON as the JSON files totaled about 5 GB (15,000 decks and a few months of archives)
    #it's largely a copy of the previous code with different output (previous files were kept as the .csv files fed automatically to the Google Sheets)
    oneFileWIP.run()
