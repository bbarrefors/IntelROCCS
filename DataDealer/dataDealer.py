#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This is the main script of the DataDealer. It will run all other scripts and functions. It will
# generate a list of datasets to replicate and on which site to replicate onto and perform the
# replication.
# 
# At the end a summary is emailed out with what was done during the run.
#
#---------------------------------------------------------------------------------------------------
import sys, os
BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
for root, dirs, files in os.walk(BASEDIR):
    sys.path.append(root)
import popDB, phedex, updateReplicasDB, datasetRanking, siteRanking

# Setup parameters
# We would like to make these easier to change in the future
popdb = popDB.popDB()
phdx = phedex.phedex()
updatereplicasdb = updateReplicasDB.updateReplicas()
datasetranking = datasetRanking.datasetRanking()
siteranking = siteRanking.siteRanking()
#===================================================================================================
#  M A I N
#===================================================================================================
# Set up valid certificate and proxy for Popularity DB and PhEDEx
popdb.renewSSOCookie()
subprocess.call(["grid-proxy-init", "-valid", "24:00"])
updatereplicasdb.updateReplicas()
datasetRankings = datasetranking.getDatasetRankings()
# {dataset:{'rank':rank, 'replicas':replicas, 'size':size, 'accesses':{'2014-06-18':accesses, '2014-06-17':accesses, '2014-06-16':accesses, '2014-06-15':accesses, '2014-06-14':accesses}}, ...}
siteRanking = siteranking.getSiteRankings()
# {site:{'rank':rank, 'space':space, 'cpu':{'2014-06-18':cpu, '2014-06-17':cpu, '2014-06-16':cpu, '2014-06-15':cpu, '2014-06-14':cpu}}}
sys.exit(0)