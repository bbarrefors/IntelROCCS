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
		# Change query when tables are updated
		#query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId = Quotas.SiteId INNER JOIN Groups ON Quotas.GroupId = Groups.GroupId WHERE Groups.GroupName=%s"
		query = "SELECT SiteName FROM Quotas WHERE GroupName=%s"
		values = ('analysisOps')
		data = dbAcc.dbQuery(query)
		return [site[0] for site in data]

	def getBlacklistedSites(self):
		# Change query when tables are updated
		#query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId = Quotas.SiteId INNER JOIN Groups ON Quotas.GroupId = Groups.GroupId WHERE Groups.GroupName=%s AND Quotas.Status=%s"
		query = "SELECT SiteName FROM Quotas WHERE GroupName=%s AND Status=%s"
		values = ('AnalysisOps', '0')
		data = dbAcc.dbQuery(query)
		return [site[0] for site in data]

	def getAvailableSites(self):
		# Change query when tables are updated
		#query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId = Quotas.SiteId INNER JOIN Groups ON Quotas.GroupId = Groups.GroupId WHERE Groups.GroupName=%s AND Quotas.Status=%s"
		query = "SELECT SiteName FROM Quotas WHERE GroupName=%s AND Status=%s"
		values = ('AnalysisOps', '1')
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