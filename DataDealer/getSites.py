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

class getSites():
    def __init__(self):
		self.dbAcc = dbAccess.dbAccess()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
	def getAllSites(self):
		query = "SELECT DatasetName FROM Sites"
		data = dbAcc.dbQuery(query)
		return [site[0] for site in data]

	def getBlacklistedSites(self):
		query = "SELECT DatasetName FROM SiteBlacklist"
		data = dbAcc.dbQuery(query)
		return [site[0] for site in data]

	def getAvailableSites(self):
		query = "SELECT Sites.DatasetName FROM Sites LEFT JOIN SiteBlacklist ON Sites.SiteId = SiteBlacklist.SiteId WHERE SiteBlacklist.SiteId IS NULL"
		data = dbAcc.dbQuery(query)
		return [site[0] for site in data]

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./getSites.py <function>
if __name__ == '__main__':
	if not (len(sys.argv) == 2):
        print "Usage: python ./getSites.py <function>"
        sys.exit(2)
    siteReq = getSites()
    func = getattr(siteReq, sys.argv[1], None)
    if not func:
        print "Function %s is not available" % (sys.argv[1])
        print "Usage: python ./getSites.py <function>"
        sys.exit(3)
    data = func()
    print data
    sys.exit(0)