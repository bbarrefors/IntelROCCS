#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# getPhedexData.py
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
import phedexApi

class getPhedexData:
    def __init__(self, cachePath, cacheFileName, oldestAllowedHours):
        self.phdxApi = phedexApi.phedexApi()
        self.cachePath = cachePath
        self.cacheFileName = cacheFileName
        self.oldestAllowedHours = oldestAllowedHours

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def shouldAccessPhedex(self):
        cache = self.cachePath+'/'+self.cacheFileName
        timeNow = datetime.datetime.now()
        deltaNhours = datetime.timedelta(seconds = 60*60*(self.oldestAllowedHours))
        modTime = datetime.datetime.fromtimestamp(0)
        if os.path.isfile(cache):
            modTime = datetime.datetime.fromtimestamp(os.path.getmtime(cache))
            if os.path.getsize(cache) == 0:
                # cache is empty
                return True
            if (timeNow-deltaNhours) > modTime:
                # cache is not up to date
                return True
            # fetch data from cache
            return False
        # there is no cache file
        return True

    def updateCache(self):
        jsonData = self.phdxApi.blockReplicas(node='T2_US_MIT', subscribed='y', complete='n', show_dataset='y', create_since='0')
        phdxCache = open(phedexCache, 'w')
        phdxCache.write(jsonData)
        # datasets = jsonData.get('phedex').get('dataset')
        # for dataset in datasets:

        #     datasetName = dataset.get('name')
        #     sizeGb = dataset.get('bytes')
        #     files = dataset.get('files')
        #     custodial = dataset.get
        #     groupName = ""
        #     created = ""
        #     blocks = dataset.get('block')            
        #     for block in blocks:
        #         size += block.get('bytes')
        #         files += block.get('files')
        phdxCache.close()

#===================================================================================================
#  M A I N
#===================================================================================================
    def getPhedexData(self):
        test = self.shouldAccessPhedex()
        self.updateCache()
        # get data

if __name__ == '__main__':
    cachePath = os.environ['INTELROCCS_BASE'] + "/Cache"
    cacheFileName = "phedexCache.dat"
    phedexData = getPhedexData(cachePath, cacheFileName, 12)
    phedexData.getPhedexData()
    sys.exit(0)
