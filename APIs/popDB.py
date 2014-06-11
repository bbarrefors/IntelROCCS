#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Python interface to access Popularity Database. Use SSO cookie to avoid password 
# (http://linux.web.cern.ch/linux/docs/cernssocookie.shtml)
#
#---------------------------------------------------------------------------------------------------
import os, re, sys, urllib, urllib2, httplib, time, datetime, subprocess
try:
    import json
except ImportError:
    import simplejson as json
from subprocess import call, Popen, PIPE

################################################################################
#                                                                              #
#                             P o p   D B   A P I                              #
#                                                                              #
################################################################################

class PopDBAPI():
    # Useful variables
    # POP_DB_BASE = "https://cms-popularity.cern.ch/popdb/popularity/"
    # SITE = "T2_US_Nebraska"
    # CERT = "/grid_home/cmsphedex/gridcert/myCert.pem"
    # KEY = "/grid_home/cmsphedex/gridcert/myCert.key"
    # COOKIE = "/grid_home/cmsphedex/gridcert/ssocookie.txt"
    def __init__(self):
        self.logger      = DynDTALogger()
        self.POP_DB_BASE = "https://cms-popularity.cern.ch/popdb/popularity/"
        self.CERT        = "/home/bockelman/barrefors/certs/myCert.pem"
        self.KEY         = "/home/bockelman/barrefors/certs/myCert.key"
        self.COOKIE      = "/home/bockelman/barrefors/certs/ssocookie.txt"


    ############################################################################
    #                                                                          #
    #                      R E N E W   S S O   C O O K I E                     #
    #                                                                          #
    ############################################################################

    def renewSSOCookie(self):
        """
        _renewSSOCookie_

        Renew the SSO Cookie used for accessing popularity db
        """
        subprocess.call(["cern-get-sso-cookie", "--cert", self.CERT, "--key", self.KEY, "-u", self.POP_DB_BASE, "-o", self.COOKIE])


    ############################################################################
    #                                                                          #
    #                           P O P   D B   C A L L                          #
    #                                                                          #
    ############################################################################

    def PopDBCall(self, url, values):
        """
        _PopDBCall_

        cURL PopDB API call.
        """
        name = "PopDBAPICall"
        data = urllib.urlencode(values)
        request = urllib2.Request(url, data)
        full_url = request.get_full_url() + request.get_data()
        p1 = subprocess.Popen(["curl", "-k", "-s", "-L", "--cookie", self.COOKIE, "--cookie-jar", self.COOKIE, full_url], stdout=subprocess.PIPE)
        try:
            response = p1.communicate()[0]
        except ValueError:
            return 1, "Error"
        return 0, response


    ############################################################################
    #                                                                          #
    #                           G E T   D S   D A T A                          #
    #                                                                          #
    ############################################################################

    def getDSStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        """
        _getDSdata_

        Get data from popularity DB for a specified time window

        Keyword arguments:
        sitename -- Name of site to get values from, default is summary (all)
        tstart   -- Start date of time window
        tstop    -- End data of time window
        aggr     -- Aggregate results into intervals of day/week/quarter/year
        n        -- Number of sets to return
        orderby  -- Metric to use, totcpu/naccess/nusers

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- List of tuples with set and value
        """
        name = "getDSdata"
        values = { 'sitename' : sitename, 'tstart' : tstart, 'tstop' : tstop }
        dsdata_url = urllib.basejoin(self.POP_DB_BASE, "%s/?&" % ("DSStatInTimeWindow",))
        check, response = self.PopDBCall(dsdata_url, values)
        if check:
            self.logger.error(name, "getDSStatInTimeWindow call failed.")
            return 1, "Error"
        json_data = json.loads(response)
        data = json_data.get('DATA')
        return 0, data


################################################################################
#                                                                              #
#                                  M A I N                                     #
#                                                                              #
################################################################################

if __name__ == '__main__':
    """
    __main__

    For testing purpose only.
    """
    popdb = PopDBAPI()
    popdb.renewSSOCookie()
    tstop = datetime.date.today()
    tstart = tstop - datetime.timedelta(days=3)
    check, data = popdb.getDSStatInTimeWindow(tstart=tstart, tstop=tstop)
    if check:
        sys.exit(1)
        #print data
    sys.exit(0)
