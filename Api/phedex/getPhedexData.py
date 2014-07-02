#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Middleware to access phedex data. To reduce access time we cache the data, this class checks if 
# the cache exists and is up to date. If it does then fetch it and return. If cache does not exist
# then access access phedex directly and get the data. First update the cache and then return the
# data. 
#---------------------------------------------------------------------------------------------------
import sys, os, json, datetime, subprocess
import phedexApi

class getPhedexData:
    def __init__(self, cachePath, oldestAllowedHours):
        self.phedexApi = phedexApi.phedexApi()
        self.cachePath = cachePath
        self.oldestAllowedHours = oldestAllowedHours

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def shouldAccessPhedex(self, apiCall):
        cache = "%s/%s" % (self.cachePath, apiCall)
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

    def updateCache(self, apiCall):
        jsonData = ""
        # can easily extend this to support more api calls
        if apiCall == "blockReplicas":
            jsonData = self.phedexApi.blockReplicas(node='T2*', subscribed='y', show_dataset='y', create_since='0')
        if not os.path.exists(self.cachePath):
            os.makedirs(self.cachePath)
        with open("%s/%s" % (self.cachePath, apiCall), 'w') as cacheFile:
            json.dump(jsonData, cacheFile)
        return jsonData

#===================================================================================================
#  M A I N
#===================================================================================================
    def getPhedexData(self, apiCall):
        if self.shouldAccessPhedex(apiCall):
            subprocess.call(["grid-proxy-init", "-valid", "24:00"])
            jsonData = self.updateCache(apiCall)
            return jsonData
        # access cache
        cacheFile = open("%s/%s" % (self.cachePath, apiCall), 'r')
        cache = cacheFile.read()
        cacheFile.close()
        # TODO : what if file is incorrect or corrupt? email someone
        jsonData = json.loads(cache)
        return jsonData

if __name__ == '__main__':
    cachePath = "%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE'])
    phedexData = getPhedexData(cachePath, 12)
    phedexData.getPhedexData("blockReplicas")
    sys.exit(0)
