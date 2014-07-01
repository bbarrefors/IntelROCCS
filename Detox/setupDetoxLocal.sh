#---------------------------------------------------------------------------------------------------
#
# Here the main parameters for Detox are defined. This is probably not the way we want to do it on
# the long run but let's start this way.
#
#---------------------------------------------------------------------------------------------------
# main directories

export DETOX_DB="/home/maxi/IntelROCCS-new/Detox"

export DETOX_SITESTORAGE_SERVER="t3btch039.mit.edu"
export DETOX_SITESTORAGE_DB="SiteStorage"
export DETOX_SITESTORAGE_USER="cmsSiteDb"
export DETOX_SITESTORAGE_PW="db78user?Cms"

export DETOX_HISTORY_DB="DetoxHistory"

# sub directories

export DETOX_QUOTAS="Quotas"
export DETOX_STATUS="status"
export DETOX_SNAPSHOTS="snapshots"
export DETOX_RESULT="result"

# standard filenames

export DETOX_PHEDEX_CACHE=DatasetsInPhedexAtSites.dat
export DETOX_USED_DATASETS=UsedDatasets.txt
export DETOX_DATASETS_TO_DELETE=RankedDatasets.txt

# Parameters for cleaning

export DETOX_CYCLE_HOURS=12
export DETOX_USAGE_MAX=0.90
export DETOX_USAGE_MIN=0.80
export DETOX_NCOPY_MIN=1

# What time window (in months) will be used for counting usage

export DETOX_TIME_WIND=10

# Certificate location

export DETOX_X509UP=/tmp/x509up_u`id -u`

# Local logging database config file

export DETOX_MYSQL_CONFIG=/etc/myIntelROCCS.cnf

# Email list to notify in case of problems

export DETOX_EMAIL_LIST=maxi@fnal.gov,maxi@mit.edu

# Paths

export DETOX_BASE=`pwd`
export DETOX_PYTHONPATH="$DETOX_BASE/python"

# Python path etc. (careful it might not be set)

if [ "`echo $PATH | grep /usr/local/bin`" == "" ] 
then
  export PATH="/usr/local/bin:${PATH}"
fi

export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH}"

if [ -z "$PYTHONPATH" ] 
then
  export PYTHONPATH="/usr/lib/python2.6:/usr/lib/python2.6/site-packages"
fi
export PYTHONPATH="${DETOX_PYTHONPATH}:$PYTHONPATH"
