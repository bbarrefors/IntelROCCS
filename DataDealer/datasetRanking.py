#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Collects all the necessary data to generate rankings for all datasets in the Analysisops space.
#
# TODO: Currently don't handle errors in popDB or Phedex, or if no data is returned an IndexError
# is thrown which is currently not caught.
#
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
for root, dirs, files in os.walk(BASEDIR):
    sys.path.append(root)
import phedex, dbAccess, popDB

class datasetRanking():
    def __init__(self):
        self.dbaccess = dbAccess.dbAccess()
        self.phdx = phedex.phedex()
        self.popdb = popDB.popDB()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def getReplicas(self, dataset):
        replicas = 0
        try:
            json_data = self.phdx.blockReplicas(dataset=dataset, group='AnalysisOps', show_dataset='y', create_since='0')
            data = json_data.get('phedex').get('dataset')[0]
            for replica in data.get('block')[0].get('replica'):
                replicas += 1
        except Exception, e:
            # TODO : Print to log
            return None
        return replicas

    def getSize(self, dataset):
        try:
            json_data = self.phdx.data(dataset=dataset, level='block', create_since='0')
            data = json_data.get('phedex').get('dbs')[0].get('dataset')[0].get('block')
        except Exception, e:
            return None
        size = float(0)
        for block in data:
            size += block.get('bytes')
        # Make it into GB
        size = size / 10**9
        return int(size)

    def getAccesses(self, rankings):
        # Get number of accesses for the last 5 days
        # Example: {'2014-06-18':accesses, '2014-06-17':accesses, '2014-06-16':accesses, '2014-06-15':accesses, '2014-06-14':accesses}
        accesses = dict()
        for d in range(1,6):
            tstart = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            tstop = tstart
            try:
                json_data = self.popdb.DSStatInTimeWindow(tstart=tstart, tstop=tstop)
            except Exception, e:
                return None
            data = json_data.get('data')[0]
            for d in data:
                dataset = d.get('COLLNAME')
                if dataset in rankings:
                    rankings[dataset]['accesses'][tstart] = d.get('NACC')
        return rankings

    def getNaiveRank(self, replicas, size, accesses):
        # rank = (log(n_accesses)*d_accesses)/(size*relpicas^2)
        tstart = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        tstop = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        n_accesses = accesses[tstart]
        d_accesses = max(accesses[tstop] - accesses[tstart], 1)
        rank = (math.log10(n_accesses)*d_accesses)(size*replicas**2)
        return rank

    def getRankings(self):
        # Example: {'rank':rank, 'replicas':replicas, 'size':size, 'accesses':{'2014-06-18':accesses, '2014-06-17':accesses, '2014-06-16':accesses, '2014-06-15':accesses, '2014-06-14':accesses}}
        rankings = dict()
        query = "SELECT r.DatasetName FROM (SELECT Datasets.DatasetId, Datasets.DatasetName, Replicas.Date, Replicas.Replicas FROM Replicas INNER JOIN Datasets ON Datasets.DatasetId=Replicas.DatasetId ORDER BY Replicas.Date DESC) r GROUP BY r.DatasetId"
        data = self.dbaccess.dbQuery(query)
        for dataset in (d[0] for d in data):
            replicas = self.getReplicas(dataset)
            if not replicas:
                continue
            size = self.getSize(dataset)
            if not size:
                continue
            # Dummy accesses
            accesses = dict()
            for i in range(1, 6):
                date = (datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                accesses[date] = 1
            rankings[dataset] = {'replicas':replicas, 'size':size, 'accesses':accesses}
        rankings = self.getAccesses(rankings)
        for dataset in iter(rankings):
            rank = self.getNaiveRank(rankings[dataset]['replicas'], rankings[dataset]['size'], rankings[dataset]['accesses'])
            rankings[dataset]['rank'] = rank
        return rankings


#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./datasetRanking.py
if __name__ == '__main__':
    if not (len(sys.argv) == 1):
        print "Usage: python ./datasetRanking.py"
        sys.exit(2)
    datasetranking = datasetRanking()
    data = datasetranking.getRankings()
    print data
    sys.exit(0)
