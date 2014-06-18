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

class datasetRanking():
    def __init__(self):
        self.dbAcc = dbAccess.dbAccess()
        self.phdx = phedex.phedex()

#===================================================================================================
#  H E L P E R S
#===================================================================================================


#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./datasetRanking.py <function> <dataset> <replicas>
if __name__ == '__main__':
    if not (len(sys.argv) == 4):
        print "Usage: python ./datasetRanking.py <function> <dataset> <replicas>"
        sys.exit(2)
    dbUpdate = updateDB()
    func = getattr(dbUpdate, sys.argv[1], None)
    if not func:
        print "Function %s is not available" % (sys.argv[1])
        print "Usage: python ./datasetRanking.py <function> <dataset> <replicas>"
        sys.exit(3)
    args = []
    for arg in sys.argv[2:]:
        args.append(arg)
    try:
        data = func(*args)
    except TypeError, e:
        print e
        print "Usage: python ./datasetRanking.py <function> <dataset> <replicas>"
        sys.exit(3)
    print data
    sys.exit(0)
