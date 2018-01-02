from DatasetReader import DatasetReader
from dbConnector.M20PSQLConnector import *
import getopt

def printUsage():
    print "Usage --help | --init | --initclear | --dropall | [--fraction=FRACTION]  [--datasets=PATH] --psql_user=USER --psql_password=PASSWORD"
    print "--help                      show this message"
    print "--init                       to init tables an populate with first level tables"
    print "--initclear                  to init tables an populate with first level tables"
    print "--dropall                    to drop all the tables and reset the system"
    print ""
    print "--datasets=PATH              path of csv dataset files. Default = datasets/data/"
    print "--fraction=FRACTION          percentage of the dataset to read. Default = 0.05"
    print "--psql_user=USER             postgres DB user"
    print "--psql_password=PASSWORD     portegres DB password"


if __name__ == '__main__':

    fractionToRead = 0.05
    dataset_path = 'datasets/data/'
    initDB = False
    init_clear = False
    dropAll = False

    psql_user = ""
    psql_pass = ""


    #dropAll = True
    #initDB = True

    
    print "ARGV:    ", sys.argv[1:]

    opts, rem = getopt.getopt(sys.argv[1:], "", ['help',
     'init',
     'initclear',
     'dropall',
     'fraction=',
     'datasets=',
     'psql_user=',
     'psql_password='])


    print "OPTIONS: ", opts

    for opt, arg in opts:
        if opt == '--help':
            printUsage()
            sys.exit(0)
        if opt == '--init':
            initDB = True
        if opt == '--initclear':
            initDB = True
            init_clear = True
        if opt == '--dropall':
            dropAll = True
        if opt == '--fraction':
            fractionToRead = float(arg)
        if opt == '--datasets':
            dataset_path = arg
        if opt == '--psql_user':
            psql_user = arg
        if opt == '--psql_password':
            psql_pass = arg            
            

    print "init DB      ", str(initDB)
    print "init clear   ", str(init_clear)
    print "dropall      ", str(dropAll)
    print "fraction     ", str(fractionToRead)
    print "dataset path ", str(dataset_path)
    print "psql user    ", str(psql_user)
    print "psql pass    ", str(psql_pass)

    #m20Connector = M20PSQLConnector('data_reply_db', 'dario', 'localhost', 'password')
    #m20Connector = M20PSQLConnector('postgres', 'cloudera-scm', 'localhost', '7432', 'y6jOvCiNAz')
    m20Connector = M20PSQLConnector('postgres', psql_user, 'localhost', '7432', psql_pass)
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
        moviesDS = DatasetReader.initWithFraction(dataset_path + '/movies.csv', 1.0, ',', init=True)
        gtagsDS = DatasetReader.initWithFraction(dataset_path + '/genome-tags.csv', 1.0, ',', init=True)
        linksDS = DatasetReader.initWithFraction(dataset_path + '/links.csv', 1.0, ',', init=True)

        #Just init
        ratingsDS = DatasetReader(dataset_path + "/ratings.csv", init=True)
        tagsDS = DatasetReader(dataset_path + "/tags.csv", init=True)
        gscoresDS = DatasetReader(dataset_path + "/genome-scores.csv", init=True)

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

        ratingsDS = DatasetReader.initWithFraction(dataset_path + "/ratings.csv", fractionToRead, ',')
        print "ratings loaded"
        for rat in ratingsDS.readPercentage():
            #print str(rat)
            m20Connector.insert(M20Rating(rat['userId'], rat['movieId'], rat['rating'], rat['timestamp']))


        tagsDS = DatasetReader.initWithFraction(dataset_path + "/tags.csv", fractionToRead, ',')
        print "tags loaded"
        for tag in tagsDS.readPercentage():
            #print str(tag)
            m20Connector.insert(M20Tag(tag['userId'], tag['movieId'], tag['tag'], tag['timestamp']))

        
        gscoresDS = DatasetReader.initWithFraction(dataset_path + "/genome-scores.csv", fractionToRead, ',')
        print "gscores loaded"       
        for score in gscoresDS.readPercentage():
            #print str(score)
            m20Connector.insert(M20GenomeScore(score['movieId'], score['tagId'], score['relevance']))


    m20Connector.close()
