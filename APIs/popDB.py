#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Python interface to access Popularity Database. See website for API documentation
# (https://cms-popularity.cern.ch/popdb/popularity/apidoc)
#
# Use SSO cookie to avoid password 
# (http://linux.web.cern.ch/linux/docs/cernssocookie.shtml)
#
# The API doesn't check to make sure correct values are passed or that any values are passed. All
# such checks needs to be done by the caller.
#
# It is up to the caller to make sure a valid SSO cookie is obtained before any calls are made. A
# SSO cookie is valid for 24h.
#
#---------------------------------------------------------------------------------------------------
import sys, os, re, urllib, urllib2, subprocess
try:
    import json
except ImportError:
    import simplejson as json

class popDB():
    def __init__(self):
        self.BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
        self.POPDB_BASE = "https://cms-popularity.cern.ch/popdb/popularity/"
        self.CERT = "%s/certs/myCert.pem" % (self.BASEDIR)
        self.KEY = "%s/certs/myCert.key" % (self.BASEDIR)
        self.COOKIE = "%s/certs/ssocookie.txt" % (self.BASEDIR)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def renewSSOCookie(self):
        subprocess.call(["cern-get-sso-cookie", "--cert", self.CERT, "--key", self.KEY, "-u", self.POPDB_BASE, "-o", self.COOKIE])

    def call(self, url, values):
        data = urllib.urlencode(values)
        request = urllib2.Request(url, data)
        full_url = request.get_full_url() + request.get_data()
        process = subprocess.Popen(["curl", "-k", "-s", "-L", "--cookie", self.COOKIE, "--cookie-jar", self.COOKIE, full_url], stdout=subprocess.PIPE)
        strout, error = process.communicate()
        if process.returncode != 0:
            raise Exception("FATAL - popularity failure, exit status %s" % (str(process.returncode)))
        try:
            json_data = json.loads(strout)
        except ValueError, e:
            raise Exception("FATAL - popularity failure, reason: %s" % (str(strout)))            

#===================================================================================================
#  A P I   C A L L S
#===================================================================================================
    def DSStatInTimeWindow(self, tstart='', tstop='', sitename=''):
        values = { 'sitename' : sitename, 'tstart' : tstart, 'tstop' : tstop }
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("DSStatInTimeWindow"))
        return self.call(url, values)

#====================================================================================================
#  M A I N
#====================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./popDB.py <api_call> [arg1_name:'arg1' arg2_name:'arg2' ...]
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python ./popDB.py <api_call> [arg1_name:'arg1' arg2_name:'arg2' ...]"
        sys.exit(2)
    popdb = popDB()
    func = getattr(popdb, sys.argv[1], None)
    if not func:
        print "%s is not a valid popularity db api call" % (sys.argv[1])
        print "Usage: python ./popDB.py <api_call> [arg1_name:'arg1' arg2_name:'arg2' ...]"
        sys.exit(3)
    args = dict()
    for arg in sys.argv[2:]:
        try:
            a, v = arg.split(':')
        except ValueError, e:
            print "Passed argument %s does not follow the correct usage" % (arg)
            print "Usage: python ./popDB.py <api_call> [arg1_name:'arg1' arg2_name:'arg2' ...]"
            sys.exit(2)
        args[a] = v
    popdb.renewSSOCookie()
    try:
        func(**args)
    except TypeError, e:
        print e
        print "Usage: python ./popDB.py <api_call> [arg1_name:'arg1' arg2_name:'arg2' ...]"
        sys.exit(3)
    sys.exit(0)