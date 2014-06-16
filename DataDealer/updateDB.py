#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# DataDealer
#
#---------------------------------------------------------------------------------------------------
import sys
BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
for root, dirs, files in os.walk(BASEDIR):
    sys.path.append(root)
import dbAccess

class updateDB():
    def __init__(self):
        self.dbAcc = dbAccess.dbAccess()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def updateReplicas(self, dataset, replicas):
        # Change query when tables are updated
        query = "INSERT INTO Replicas (DatasetId, Replicas) SELECT Datasets.DatasetId, %s INNER JOIN WHERE GroupName=%s"
        values = [replicas, dataset]
        data = dbAcc.dbQuery(query, values=tuple(values))
        return [site[0] for site in data]

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./updateDB.py <function>
if __name__ == '__main__':
    if not (len(sys.argv) == 2):
        print "Usage: python ./updateDB.py <function>"
        sys.exit(2)
    dbUpdate = updateDB()
    func = getattr(dbUpdate, sys.argv[1], None)
    if not func:
        print "Function %s is not available" % (sys.argv[1])
        print "Usage: python ./updateDB.py <function>"
        sys.exit(3)
    data = func()
    print data
    sys.exit(0)