#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# 
#---------------------------------------------------------------------------------------------------
import sys, os, json, sqlite3, datetime
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import IntelROCCS.Api.phedex.phedexData as phedexData

class phedexDb():
    def __init__(self, oldestAllowedHours):
        dbFile = "%s/Cache/PhedexCache/blockReplicas.db" % (os.environ['INTELROCCS_BASE'])
        update = 0
        timeNow = datetime.datetime.now()
        deltaNhours = datetime.timedelta(seconds = 60*60*(oldestAllowedHours))
        if os.path.isfile(dbFile):
            modTime = datetime.datetime.fromtimestamp(os.path.getmtime(dbFile))
            if os.path.getsize(dbFile) == 0:
                os.remove(dbFile)
                update = 1
            elif (timeNow-deltaNhours) > modTime:
                os.remove(dbFile)
                update = 1
        else:
            update = 1
        self.dbCon = sqlite3.connect(dbFile)
        cur = self.dbCon.cursor()
        cur.execute('PRAGMA foreign_keys = ON')
        cur.close()
        if update == 1:
            with self.dbCon:
                cur = self.dbCon.cursor()
                cur.execute('CREATE TABLE Datasets (DatasetId INTEGER PRIMARY KEY AUTOINCREMENT, DatasetName TEXT UNIQUE, SizeGb INTEGER)')
                cur.execute('CREATE TABLE Replicas (SiteName TEXT, DatasetId INTEGER, FOREIGN KEY(DatasetId) REFERENCES Datasets(DatasetId))')
                phedex = phedexData.phedexData(oldestAllowedHours)
                phedexJsonData = phedex.getPhedexData('blockReplicas')
                self.buildPhedexDb(phedexJsonData)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def buildPhedexDb(self, phedexJsonData):
        datasets = phedexJsonData.get('phedex').get('dataset')
        for dataset in datasets:
            datasetName = dataset.get('name')
            sizeGb = 0
            size = dataset.get('bytes')
            if not size:
                for block in dataset.get('block'):
                    sizeGb += int(block.get('bytes')/10**9)
            else:
                sizeGb = int(size/10**9)
            with self.dbCon:
                cur = self.dbCon.cursor()
                cur.execute('INSERT INTO Datasets(DatasetName, SizeGb) VALUES(?, ?)', (datasetName, sizeGb))
                datasetId = cur.lastrowid
                for replica in dataset.get('block')[0].get('replica'):
                    siteName = replica.get('node')
                    cur.execute('INSERT INTO Replicas(SiteName, DatasetId) VALUES(?, ?)', (siteName, datasetId))

    def getDatasetSize(self, datasetName):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT SizeGb FROM Datasets WHERE DatasetName=?', (datasetName))
            sizeGb = cur.fetchone()[0] # TODO : Check that something is returned
            return sizeGb

    def getNumberReplicas(self, datasetName):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT SiteName FROM Replicas NATUREAL JOIN Datasets WHERE Datasets.DatasetName=?', (datasetName,))
            replicas = 0
            for row in cur:
                print row
                replicas += 1
            return replicas

if __name__ == '__main__':
    phedexDb = phedexDb(12)
    replicas = phedexDb.getNumberReplicas("/RelValPREMIXUP15_PU25/CMSSW_7_0_5_patch1-PU25ns_POSTLS170_V7-v1/GEN-SIM-DIGI-RAW")
    print replicas
    sys.exit(0)