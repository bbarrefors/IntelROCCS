#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# getPhedexData.py
#---------------------------------------------------------------------------------------------------
import sys, os, json, datetime, subprocess
import phedexApi

class getPhedexData:
    def __init__(self, cachePath, cacheFileName, oldestAllowedHours):
        self.phedexApi = phedexApi.phedexApi()
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
        jsonData = self.phedexApi.blockReplicas(node='T2*', subscribed='y', show_dataset='y', create_since='0')
        with open("%s/%s" % (self.cachePath, self.cacheFileName), 'w') as cacheFile:
            json.dump(jsonData, cacheFile)
        return jsonData

#===================================================================================================
#  M A I N
#===================================================================================================
    def getPhedexData(self):
        if self.shouldAccessPhedex():
            subprocess.call(["grid-proxy-init", "-valid", "24:00"])
            jsonData = self.updateCache()
            return jsonData
        # TODO : what if file is incorrect or corrupt? email someone
        cacheFile = open("%s/%s" % (self.cachePath, self.cacheFileName), 'r')
        cache = cacheFile.read()
        cacheFile.close()
        jsonData = json.loads(cache)
        print jsonData

if __name__ == '__main__':
    cachePath = "%s/Cache" % (os.environ['INTELROCCS_BASE'])
    cacheFileName = "phedexCache.dat"
    phedexData = getPhedexData(cachePath, cacheFileName, 12)
    phedexData.getPhedexData()
    sys.exit(0)
