#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Access to MIT database. IP address of machine where script is on need to have permission to access
# the database. Requires a login file with base64 encoded values for host, database, username, and 
# password stored in BASEDIR/db/
#
# Returns a list with touples of the data requested. Example of what should be passed:
# query="SELECT * WHERE DatasetName=%s", values=touple("Dataset1")
# Values are optional
#
# In case of error an exception is thrown. This needs to be dealt with by the caller.
#
#---------------------------------------------------------------------------------------------------
import sys, os, base64, MySQLdb

class dbAccess():
    def __init__(self):
        self.BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
        db_file = open('%s/db/login' % (self.BASEDIR))
        self.HOST = base64.b64decode(db_file.readline().strip())
        self.DB = base64.b64decode(db_file.readline().strip())
        self.USER = base64.b64decode(db_file.readline().strip())
        self.PASSWD = base64.b64decode(db_file.readline().strip())
        self.DB_CON = MySQLdb.connect(host=self.HOST, user=self.USER, passwd=self.PASSWD, db=self.DB)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def dbQuery(self, query, values=()):
        data = []
        values = tuple([str(value) for value in values])
        try:
            with self.DB_CON:
                cur = self.DB_CON.cursor()
                cur.execute(query, values)
                for i in range(cur.rowcount):
                    row = cur.fetchone()
                    data.append(row)
        except MySQLdb.Error, e:
            raise Exception("FATAL - Database failure:  %d: %s" % (e.args[0],e.args[1]))
        return data

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./dbAccess.py <'db query'> ['value1', 'value2', ...]
# Example: $ python ./dbAccess.py 'SELECT * WHERE DatasetName=%s' 'Dataset1'
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python ./dbAccess.py <'db query'> ['value1', 'value2', ...]"
        sys.exit(2)
    dbAcc = dbAccess()
    query = sys.argv[1]
    values = []
    for v in sys.argv[2:]:
        values.append(v)
    values = tuple(values)
    data = dbAcc.dbQuery(query, values=values)
    print data
    sys.exit(0)
