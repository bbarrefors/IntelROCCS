#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Collects all the necessary data to generate rankings for all available sites
#
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
for root, dirs, files in os.walk(BASEDIR):
    sys.path.append(root)
import phedex, dbAccess, popDB, getSites

class siteRanking():
    def __init__(self):
        self.dbaccess = dbAccess.dbAccess()
        self.phdx = phedex.phedex()
        self.popdb = popDB.popDB()
        self.getsites = getSites.getSites()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def getQuota(self, site):
        query = "SELECT SizeTb FROM Quotas WHERE SiteName=%s"
        values = [site]
        data = self.dbaccess.dbQuery(query, values=tuple(values))
        return data[0][0]

    def getSpace(self, site):
        # space = 0.95*total_space - used_space
        try:
            json_data = self.phdx.blockReplicas(node=site, group='AnalysisOps', create_since='0')
        except Exception, e:
            return None
        data = json_data.get('phedex').get('block')
        if not data:
            return None
        used_space = float(0)
        for block in data:
            replica = block.get('replica')[0]
            if replica.get('subscribed') == 'y':
                used_space += block.get('bytes')
            else:
                used_space += replica.get('bytes')
        used_space = int(used_space/10**9)
        quota = int(self.getQuota(site)*10**3)
        space = quota - used_space
        return space

    def getCPU(self, site):
        # Get cpu as below for last 5 days
        # cpu = top3:max(cpu_usage)/3 - cpu_usage
        # Example: {'2014-06-18':cpu, '2014-06-17':cpu, '2014-06-16':cpu, '2014-06-15':cpu, '2014-06-14':cpu}
        cpus = dict()
        for i in range(1,6):
            tstart = (datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            tstop = tstart
            try:
                json_data = self.popdb.DSStatInTimeWindow(sitename=site, tstart=tstop, tstop=tstop)
            except Exception, e:
                return None
            cpu = 0
            data = json_data.get('DATA')
            if not data:
                cpu += 0
            for dataset in data:
                cpu += dataset.get('TOTCPU')
            cpus[tstart] = cpu
        return cpus

    def getNaiveRank(self, space, cpus):
        # rank = space * cpu
        tstart = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        cpu = cpus[tstart]
        rank = space*cpu
        return rank

    def getSiteRankings(self):
        # Example : {site:{'rank':rank, 'space':space, 'cpu':{'2014-06-18':cpu, '2014-06-17':cpu, '2014-06-16':cpu, '2014-06-15':cpu, '2014-06-14':cpu}}}
        rankings = dict()
        availableSites = self.getsites.getAvailableSites()
        for site in availableSites:
            space = self.getSpace(site)
            if not space:
                continue
            cpu = self.getCPU(site)
            if not cpu:
                continue
            rank = self.getNaiveRank(space, cpu)
            rankings[site] = {'rank':rank, 'space':space, 'cpu':cpu}
        return rankings

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./datasetRanking.py
if __name__ == '__main__':
    if not (len(sys.argv) == 1):
        print "Usage: python ./siteRanking.py"
        sys.exit(2)
    siteRanking = siteRanking()
    data = siteRanking.getSiteRankings()
    print data
    sys.exit(0)
