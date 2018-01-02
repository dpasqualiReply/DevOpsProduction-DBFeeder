import sys
import psycopg2
#from psycopg2 import sql
from datasetModel.M20Element import *


class M20PSQLConnector(object):
    def __init__(self, db_name, user, host, port, password, clearTableInit=False):

        self.connection = None
        self.cursor = None

        self.db_name = db_name
        self.user = user
        self.host = host
        self.port = port
        self.password = password

        self.createDict = dict()
        if (clearTableInit == False):
            self.createDict[0] = (M20GenomeTag.name, M20GenomeTag.createQuery())
            self.createDict[1] = (M20Movie.name, M20Movie.createQuery())
            self.createDict[2] = (M20Tag.name, M20Tag.createQuery())
            self.createDict[3] = (M20Link.name, M20Link.createQuery())
            self.createDict[4] = (M20Rating.name, M20Rating.createQuery())
            self.createDict[5] = (M20GenomeScore.name, M20GenomeScore.createQuery())


        self.dropDict = dict()
        if (clearTableInit == False):
            self.dropDict[5] = (M20GenomeTag.name, M20GenomeTag.dropQuery())
            self.dropDict[4] = (M20Movie.name, M20Movie.dropQuery())
            self.dropDict[3] = (M20Tag.name, M20Tag.dropQuery())
            self.dropDict[2] = (M20Link.name, M20Link.dropQuery())
            self.dropDict[1] = (M20Rating.name, M20Rating.dropQuery())
            self.dropDict[0] = (M20GenomeScore.name, M20GenomeScore.dropQuery())


    """ Connection Management =================================== """

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                "dbname='" + self.db_name + "' user='" + self.user + "' host='" + self.host + "' port='" + self.port + "' password='" + self.password + "'")
            self.cursor = self.connection.cursor()
            print "[ INFO ] Connection opened"

        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

    def close(self):
        try:
            self.connection.commit()
            self.cursor.close()
            self.connection.close()

            self.cursor = None
            self.connection = None

            print "[ INFO ] Connection closed"

        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

        # Create =================================================

    def initDB(self):
        try:

            if (self.connection == None):
                print "[ ERROR ] The connection is closed!!!!"
                sys.exit(1)

            print "[ INIT ] Init tables"

            self.cursor.execute("select exists(select * from information_schema.tables where table_name='movies')")

            if (self.cursor.fetchone()[0]):
                print "[ INIT ] Tables already exists"


            else:

                for create in self.createDict.keys():
                    print "[ INIT ] " + self.createDict[create][0] + " init..."
                    self.cursor.execute(self.createDict[create][1])
                    print "[ INIT ] " + self.createDict[create][0] + " creation done..."

            print "[ INIT ] Done."


        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

    def dropAll(self):

        try:

            if (self.connection == None):
                print "[ ERROR ] The connection is closed!!!!"
                sys.exit(1)

            for table in self.dropDict.keys():
                self.cursor.execute(self.dropDict[table][1])
                print "[ DROP ] " + self.dropDict[table][0] + " dropped..."


        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)


    """ Insert ================================================= """

    def insert(self, m20Element):
        try:

            if (self.connection == None):
                print "[ ERROR ] The connection is closed!!!!"
                sys.exit(1)

            self.cursor.execute(m20Element.insertQuery, m20Element.insertData)

            print ("[ INSERT ] New %s insert into table"%(m20Element.name,))

        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

    """ SelectAll ============================================== """

    def selectAll(self, m20Element):
        try:

            if (self.connection == None):
                print "[ ERROR ] The connection is closed!!!!"
                sys.exit(1)

            print "[ SELECT ] Select All from " + m20Element.name

            self.cursor.execute(m20Element.selectAllQuery())

            return self.cursor.fetchall()


        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

    def printAll(self, m20Element):
        try:

            for row in self.selectAll(m20Element):
                print "[ SELECT ] " + str(row)


        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)
