#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# dbAccess
#
#---------------------------------------------------------------------------------------------------
import sys, MySQLdb

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
	def dbQuery(self, query, values):
		cur = self.DB_CON.cursor()
		cur.execute(query, values))

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./dbAccess.py <query> [value1, value2, ...]
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python ./dbAccess.py <query> [value1, value2, ...]"
        sys.exit(2)
    mit_db = dbAccess()
    query = sys.argv[1]
    values = []
    for v in sys.argv[2:]:
        values.append(v)
    values = tuple(values)
    data = mit_db.dbQuery(query, values)
    print data
    sys.exit(0)