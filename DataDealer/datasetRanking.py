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
import phedex, dbAccess, popDb

class datasetRanking():
    def __init__(self):
        self.dbaccess = dbAccess.dbAccess()
        self.phdx = phedex.phedex()
        self.popdb = popDb.popDb()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def getReplicas(self, dataset):
        json_data = self.phdx.blockReplicas(self, dataset=dataset, group='AnalysisOps', show_dataset='y', created_since='0')
        data = json_data.get('phedex').get('dataset')[0]
        replicas = 0
        for replica in data.get('block')[0].get('replica'):
            replicas += 1
        return replicas

    def getSize(self, dataset):
        json_data = self.phdx.data(self, dataset=dataset, level='block', created_since='0')
        data = json_data.get('phedex').get('dbs')[0].get('dataset')[0].get('block')
        size = float(0)
        for block in data:
            size += block.get('bytes')
        # Make it into GB
        size = size / 10**9
        return int(size)

    def getAccesses(self, dataset):
        # Get number of accesses for the last 5 days
        # Example: {'2014-06-18':accesses, '2014-06-17':accesses, '2014-06-16':accesses, '2014-06-15':accesses, '2014-06-14':accesses}
        accesses = dict()
        json_data = self.popdb.getSingleDSstat(name=dataset, aggr='day', orderby='naccess')
        data = json_data.get('data')[0].get('data')[0]
        data = sorted(data, reversed=True, key=lambda date: date[0])
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        last_report = datetime.datetime.utcfromtimestamp(int(int(data[0][0])/1000)).strftime('%Y-%m-%d')
        if yesterday == last_report:
            for i in range(5):
                accesses[datetime.datetime.utcfromtimestamp(int(int(data[i][0])/1000)).strftime('%Y-%m-%d')] = data[i][1]
        return accesses

    def getNaiveRank(self, replicas, size, accesses):
        tstart = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        tstop = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        n_accesses = accesses[tstart]
        d_accesses = max(accesses[tstop] - accesses[tstart], 1)
        # rank = (log(n_accesses)*d_accesses)/(size*relpicas^2)
        rank = (math.log10(n_accesses)*d_accesses)(size*replicas**2)
        return rank

    def getRankings(self):
        # Example: {'rank':rank, 'replicas':replicas, 'size':size, 'accesses':{'2014-06-18':accesses, '2014-06-17':accesses, '2014-06-16':accesses, '2014-06-15':accesses, '2014-06-14':accesses}}
        rankings = dict()
        query = "SELECT Datasets.DatasetName FROM(SELECT * FROM Replicas ORDER BY Date DESC) r GROUP BY DatasetId INNER JOIN Datasets ON Datasets.DatasetId=Replicas.DatasetId"
        data = self.dbAcc.dbQuery(query)
        for dataset in data:
            rank = 0
            replicas = self.getReplicas(dataset)
            size = self.getSize(dataset)
            accesses = self.getAccesses(dataset)
            if (replicas and size and accesses):
                rank = self.getNaiveRank(replicas, size, accesses)
            else:
                continue
            rankings[dataset] = {'rank':rank, 'replicas':replicas, 'size':size, 'accesses':accesses}
        return rankings


#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./datasetRanking.py
if __name__ == '__main__':
    if not (len(sys.argv) == 4):
        print "Usage: python ./datasetRanking.py"
        sys.exit(2)
    datasetranking = datasetRanking()
    data = datasetranking.getRankings()
    print data
    sys.exit(0)
