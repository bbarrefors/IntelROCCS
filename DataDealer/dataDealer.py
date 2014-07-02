#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. It will run all other scripts and functions. It will
# generate a list of datasets to replicate and on which site to replicate onto and perform the
# replication.
# 
# At the end a summary is emailed out with what was done during the run.
#---------------------------------------------------------------------------------------------------
import sys, os, subprocess, datetime
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import IntelROCCS.Api.popDb.getPopDbData as popDbData
import IntelROCCS.Api.phedex.getPhedexData as phedexData
#import updateDb, datasetRanking, siteRanking, select

# Setup parameters
# We would like to make these easier to change in the future
#popDb = popDbData.getPopDbData("%s/Cache/popDbCache" % (os.environ['INTELROCCS_BASE']), 12)
phedex = phedexData.getPhedexData("%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE']), 12)
# updateDb = updateDb.updateDb()
# datasetRanking = datasetRanking.datasetRanking()
# siteRanking = siteRanking.siteRanking()
# select = select.select()

threshold = 100 # TODO : Find threshold
budgetGb = 10000 # TODO : Decide on a budget
#===================================================================================================
#  M A I N
#===================================================================================================
date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
#popDbJsonData = popDb.getPopDbData(date)
phedexJsonData = phedex.getPhedexData("blockReplicas")

# Get dataset rankings
# {dataset:{'rank':rank, 'replicas':replicas, 'size':size, 'accesses':{'2014-06-18':accesses, '2014-06-17':accesses, '2014-06-16':accesses, '2014-06-15':accesses, '2014-06-14':accesses}}, ...}
# datasetRankings = datasetranking.getDatasetRankings()
# sortedDatasetRankings = []
# for dataset in iter(datasetRankings):
# 	#if datasetRankings[dataset]['rank'] >= threshold:
# 	sortedDatasetRankings.append((dataset, datasetRankings[dataset]['rank']))
# sortedDatasetRankings = set(sorted(sortedDatasetRankings, key=itemgetter(1)))
# print sortedDatasetRankings

# Get site rankings
# {site:{'rank':rank, 'space':space, 'cpu':{'2014-06-18':cpu, '2014-06-17':cpu, '2014-06-16':cpu, '2014-06-15':cpu, '2014-06-14':cpu}}}
# siteRanking = siteranking.getSiteRankings()
# sortedSiteRankings = []
# for site in iter(siteRankings):
# 	sortedSiteRankings.append((site, siteRankings[site]['rank']))
# sortedSiteRankings = set(sorted(sortedSiteRankings, key=itemgetter(1)))
# print sortedSiteRankings

# Select datasets and sites for subscriptions
# subscriptions = dict()
# while (selectedGB < budgetGB) and (sortedDatasetRankings):
# 	dataset, rank = select.weightedChoice(sortedDatasetRankings)
# 	site = select.weightedChoice(sortedSiteRankings)
# 	if site in subscriptions:
# 		subscriptions[site].append(dataset)
# 	else:
# 		subscriptions[site] = [dataset]
# 	sortedDatasetRankings.remove((dataset, rank))
# print subscriptions

# create subscriptions
# for site in iter(subscriptions):
# 	data = self.phdx.xmlData(subscriptions[site])
	# TODO : Improve comments
	# TODO : Check for errors
	#json_data = self.phdx.subscribe(node=site, data=data, level='file', group='AnalysisOps', request_only='y', comments='IntelROCCS DataDealer')
	# TODO : Insert subscription into db
	#self.updatedb(json_data)

# Send summary report
# TODO : Send daliy report

# DONE
sys.exit(0)
