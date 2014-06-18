#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Keeps a record of how many replicas exist for each dataset. Will only record changes and therefore
# an exact number of replicas can be extracted for any day. 
#
# Needs to be ran once a day to keep an accurate record.
#
#---------------------------------------------------------------------------------------------------
import sys, os
BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
for root, dirs, files in os.walk(BASEDIR):
    sys.path.append(root)
import dbAccess, phedex

class updateReplicasDB():
    def __init__(self):
        self.dbAcc = dbAccess.dbAccess()
        self.phdx = phedex.phedex()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def getReplicas(self):
        query = "SELECT Datasets.DatasetName, Replicas FROM(SELECT * FROM Replicas ORDER BY Date DESC) r GROUP BY DatasetId INNER JOIN Datasets ON Datasets.DatasetId=Replicas.DatasetId"
        data = self.dbAcc.dbQuery(query)
        oldReplicas = dict()
        for replicas in data:
            oldReplicas[replicas[0]] = replicas[1]
        return oldReplicas

    def insertReplicas(self, dataset, replicas):
        query = "INSERT INTO Replicas (DatasetId, Replicas) SELECT Datasets.DatasetId, %s FROM Datasets WHERE DatasetName=%s"
        values = [replicas, dataset]
        self.dbAcc.dbQuery(query, values=tuple(values))

    def updateReplicas(self):
        newReplicas = dict()
        json_data = self.phdx.blockReplicas(self, group='AnalysisOps', show_dataset='y', created_since='')
        data = json_data.get('phedex').get('dataset')
        for d in data:
            dataset = d.get('name')
            replicas = 0
            for replica in d.get('block')[0].get('replica'):
                replicas += 1
            newReplicas[dataset] = replicas
        oldReplicas = self.getReplicas()
        for dataset, replicas in newReplicas.iteritems():
            if (not (dataset in oldReplicas)) or (oldReplicas[dataset] != replicas):
                self.insertReplicas(dataset, replicas)

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./updateDB.py <function> <dataset> <replicas>
if __name__ == '__main__':
    if not (len(sys.argv) == 4):
        print "Usage: python ./updateDB.py <function> <dataset> <replicas>"
        sys.exit(2)
    dbUpdate = updateDB()
    func = getattr(dbUpdate, sys.argv[1], None)
    if not func:
        print "Function %s is not available" % (sys.argv[1])
        print "Usage: python ./updateDB.py <function> <dataset> <replicas>"
        sys.exit(3)
    args = []
    for arg in sys.argv[2:]:
        args.append(arg)
    try:
        data = func(*args)
    except TypeError, e:
        print e
        print "Usage: python ./updateDB.py <function> <dataset> <replicas>"
        sys.exit(3)
    print data
    sys.exit(0)
