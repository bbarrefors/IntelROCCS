#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# DataDealer
#
#---------------------------------------------------------------------------------------------------
import sys, os
BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
for root, dirs, files in os.walk(BASEDIR):
	sys.path.append(root)
import popDB, phedex

# Setup parameters
# We would like to make these easier to change in the future
popdb = popDB.popDB()
phdx = phedex.phedex()

#====================================================================================================
#  M A I N
#====================================================================================================
# Set up valid certificate and proxy for Popularity DB and PhEDEx
popdb.renewSSOCookie()
subprocess.call(["grid-proxy-init", "-valid", "24:00"])
sys.exit(0)