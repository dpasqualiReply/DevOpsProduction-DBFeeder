from DatasetReader import DatasetReader
from dbConnector.M20PSQLConnector import *

"""
    Lauch with DBFeeder.py fractionToRead
"""

def printUsage():
    print "Usage [OPTION] [fractionToRead]"
    print "[OPTION] = "
    print "-init to init tables an populate with first level tables"
    print "-dropall to drop all the tables and reset the system"
    print "fractionToRead = percentage of the dataset to read. Default = 1/20"


if __name__ == '__main__':

    # cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
    # cur.execute("SELECT * FROM test;")
    # print cur.fetchone()

    fractionToRead = 0.05

    initDB = False
    init_clear = False
    dropAll = False

    #dropAll = True
    #initDB = True


    if(len(sys.argv) == 2):

        if(str(sys.argv[1]) == '-init'):
            initDB = True

        elif (sys.argv[1] == '-initclear'):
            initDB = True
            init_clear = True;


        elif(sys.argv[1] == '-dropall'):
            dropAll = True

        else:
            fractionToRead = float(sys.argv[1])

    if(len(sys.argv) == 3):

        if (sys.argv[1] == '-initclear' or sys.argv[2] == '-initclear'):
            initDB = True
            init_clear = True

        if(sys.argv[1] == '-init' or sys.argv[2] == '-init'):
            initDB = True

        if(sys.argv[1] == '-dropall' or sys.argv[2] == '-dropall'):
            dropAll = True

        if(initDB == False and dropAll == False):
            printUsage()
            sys.exit(1)

        if((initDB and dropAll) == False):
            fractionToRead = float(sys.argv[1])



    #m20Connector = M20PSQLConnector('data_reply_db', 'dario', 'localhost', 'password')
    m20Connector = M20PSQLConnector('postgres', 'cloudera-scm', 'localhost', '7432', 'y6jOvCiNAz')
    m20Connector.connect()

    if (dropAll):
        m20Connector.dropAll()
        m20Connector.close()
        sys.exit(0)


    if(initDB):

        """
            Create tables
            insert first level tables = movies, genome_tags
        """
        m20Connector.initDB()

        # Init to read
        moviesDS = DatasetReader.initWithFraction('datasets/data/movies.csv', 1.0, ',', init=True)
        gtagsDS = DatasetReader.initWithFraction('datasets/data/genome-tags.csv', 1.0, ',', init=True)
        linksDS = DatasetReader.initWithFraction('datasets/data/links.csv', 1.0, ',', init=True)

        #Just init
        ratingsDS = DatasetReader("datasets/data/ratings.csv", init=True)
        tagsDS = DatasetReader("datasets/data/tags.csv", init=True)
        gscoresDS = DatasetReader("datasets/data/genome-scores.csv", init=True)

        if(init_clear == False):
            for movie in moviesDS.readPercentage():
                # print str(movie)
                m20Connector.insert(M20Movie(movie['movieId'], movie['title'], movie['genres']))

            for tag in gtagsDS.readPercentage():
                # print str(tag)
                m20Connector.insert(M20GenomeTag(tag['tagId'], tag['tag']))

            for link in linksDS.readPercentage():
                #print str(link)
                m20Connector.insert(M20Link(link['movieId'], link['imdbId'], link['tmdbId']))

    else:

        print "Load " + str(fractionToRead * 100) + "% of each Dataset into the Database"

        ratingsDS = DatasetReader.initWithFraction("datasets/data/ratings.csv", fractionToRead, ',')
        print "ratings loaded"
        for rat in ratingsDS.readPercentage():
            #print str(rat)
            m20Connector.insert(M20Rating(rat['userId'], rat['movieId'], rat['rating'], rat['timestamp']))


        tagsDS = DatasetReader.initWithFraction("datasets/data/tags.csv", fractionToRead, ',')
        print "tags loaded"
        for tag in tagsDS.readPercentage():
            #print str(tag)
            m20Connector.insert(M20Tag(tag['userId'], tag['movieId'], tag['tag'], tag['timestamp']))

        
        gscoresDS = DatasetReader.initWithFraction("datasets/data/genome-scores.csv", fractionToRead, ',')
        print "gscores loaded"       
        for score in gscoresDS.readPercentage():
            #print str(score)
            m20Connector.insert(M20GenomeScore(score['movieId'], score['tagId'], score['relevance']))


    m20Connector.close()
