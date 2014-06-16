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
import popDB, phedex, getSites, updateReplicasDB, datasetRanking

# Setup parameters
# We would like to make these easier to change in the future
popdb = popDB.popDB()
phdx = phedex.phedex()
siteReq = getSites.getSites()
updateReplicas = updateReplicasDB.updateReplicas()
dsetRank = datasetRanking.datasetRanking()
#===================================================================================================
#  M A I N
#===================================================================================================
# Set up valid certificate and proxy for Popularity DB and PhEDEx
popdb.renewSSOCookie()
subprocess.call(["grid-proxy-init", "-valid", "24:00"])
availableSites = siteReq.getAvailableSites()
dbUpdate.updateReplicas()
# {dataset:ranking, ...}
datasetRankings = dsetRank.getRankings()
# {dataset:{'replicas':replicas, 'size':size, 'n_accesses':n_accesses, 'd_accesses':d_accesss}, ...}
# This will be changed as the ranking algorithm changes
datasetStats = dsetRank.getStats()
sys.exit(0)