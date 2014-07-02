#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# getPhedexData.py
#---------------------------------------------------------------------------------------------------
import sys, os, datetime, json
import phedexApi

class getPopData:
    def __init__(self, cachePath, oldestAllowedHours):
        self.popDbApi = popDbApi.popDbApi()
        self.cachePath = cachePath
        self.cacheFileName = cacheFileName
        self.oldestAllowedHours = oldestAllowedHours

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def shouldAccessPopDb(self, date):
        cache = self.cachePath+"/"+date
        if os.path.isfile(cache):
            if os.path.getsize(cache) == 0:
                # cache is empty
                return True
            # fetch data from cache
            return False
        # there is no cache file
        return True

    def updateCache(self, date):
        tstart = date
        tstop = tstart
        jsonData = self.popDbApi.DSStatInTimeWindow(tstart=tstop, tstop=tstop)
        with open(self.cachePath+"/"+tstart, 'w') as cacheFile:
            json.dump(jsonData, cacheFile)
        return jsonData

#===================================================================================================
#  M A I N
#===================================================================================================
    def getPopDbData(self, date):
        if self.shouldAccessPopDb(date):
            jsonData = self.updateCache(date)
            return jsonData
        cacheFile = open(self.cachePath+"/"+date, 'r')
        cache = cacheFile.read()
        cacheFile.close()
        jsonData = json.loads(cache)
        return jsonData

if __name__ == '__main__':
    cachePath = os.environ['INTELROCCS_BASE'] + "/Cache/popDbCache"
    popDbData = getPopData(cachePath, 12)
    jsonData = popDbData.getPopData()
    print jsonData
    sys.exit(0)
