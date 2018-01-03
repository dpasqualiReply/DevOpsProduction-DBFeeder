import psycopg2
#from psycopg2 import sql

class M20Element(object):

    name = ""

    def __init__(self):
        self.createQuery = ""
        self.insertQuery = ""
        self.insertData = None

    @classmethod
    def createQuery(cls):
        return ""

    @classmethod
    def selectAllQuery(cls):
        return """SELECT * FROM %s;"""%cls.name

    @classmethod
    def dropQuery(cls):
        return """DROP TABLE IF EXISTS %s"""%cls.name



class M20Tag(M20Element):

    name = 'tags'

    def __init__(self, userid, movieid, tag, timestamp):
        M20Element.__init__(self)
        self.userId = userid
        self.movieId = movieid
        self.tag = tag
        self.timestamp = timestamp

        self.insertQuery = """INSERT INTO %s (userId, movieId, tag, timestamp)
                                      VALUES (%s, %s, %s, %s);"""%(self.name, '%s', '%s', '%s', '%s')

        self.insertData = (self.userId, self.movieId, self.tag, self.timestamp)


    @classmethod
    def createQuery(cls):
        return """CREATE TABLE IF NOT EXISTS %s 
                            (id SERIAL PRIMARY KEY,
                            userId INTEGER,
                            movieId INTEGER,
                            tag TEXT,
                            timestamp TEXT,
                            FOREIGN KEY (movieId) REFERENCES movies(movieId));"""%(cls.name)



class M20Movie(M20Element):

    name = 'movies'

    def __init__(self, movieid, title, genres):
        M20Element.__init__(self)
        self.movieId = movieid
        self.title = title
        self.genres = genres

        self.insertQuery = """INSERT INTO %s (movieId, title, genres)
                                              VALUES (%s, %s, %s);"""%(self.name, '%s', '%s', '%s')

        self.insertData = (self.movieId,
                    self.title,
                    self.genres)


    @classmethod
    def createQuery(cls):
        return """CREATE TABLE IF NOT EXISTS %s 
                                  (id serial PRIMARY  KEY,
                                  movieId INTEGER UNIQUE,
                                  title TEXT,
                                  genres TEXT);""" % cls.name




class M20Rating(M20Element):

    name = 'ratings'

    def __init__(self, userid, movieid, rating, timestamp):
        M20Element.__init__(self)
        self.userId = userid
        self.movieId = movieid
        self.rating = rating
        self.timestamp = timestamp

        self.insertQuery = """INSERT INTO %s (userId, movieId, rating, timestamp)
                                                      VALUES (%s, %s, %s, %s);"""\
                           %(self.name, '%s', '%s', '%s', '%s')

        self.insertData = (self.userId,
                            self.movieId,
                            self.rating,
                            self.timestamp)

    @classmethod
    def createQuery(cls):
        return """CREATE TABLE IF NOT EXISTS %s 
                            (id SERIAL PRIMARY KEY,
                            userId INTEGER,
                            movieId INTEGER REFERENCES movies(movieId),
                            rating FLOAT,
                            timestamp TEXT,
                            FOREIGN KEY (movieId) REFERENCES movies(movieId));"""%cls.name





class M20Link(M20Element):

    name = 'links'

    def __init__(self, movieid, imdbid, tmdbid):
        M20Element.__init__(self)
        self.movieId = movieid
        self.imdbId = imdbid
        self.tmdbId = tmdbid

        self.insertQuery = """INSERT INTO %s (movieId, imdbId, tmdbId)
                                                              VALUES (%s, %s, %s);"""\
                           %(self.name, '%s', '%s', '%s')

        self.insertData = (self.movieId,
                            self.imdbId,
                            self.tmdbId)



    @classmethod
    def createQuery(cls):
        return """CREATE TABLE IF NOT EXISTS %s 
                                    (id serial primary key,
                                    movieId INTEGER REFERENCES movies(movieId),
                                    imdbId TEXT,
                                    tmdbId TEXT,
                                    FOREIGN KEY (movieId) REFERENCES movies(movieId));"""%cls.name





class M20GenomeScore(M20Element):

    name = "genomescore"

    def __init__(self, movieid, tagid, relevance):
        M20Element.__init__(self)
        self.movieId = movieid
        self.tagId = tagid
        self.relevance = relevance


        self.insertQuery = """INSERT INTO %s (movieId, tagId, relevance)
                                                                      VALUES (%s, %s, %s);"""\
                           %(self.name, '%s', '%s', '%s')

        self.insertData = (self.movieId,
                            self.tagId,
                            self.relevance)

    @classmethod
    def createQuery(cls):
        return """CREATE TABLE IF NOT EXISTS %s 
                                    (id SERIAL PRIMARY KEY,
                                    movieId INTEGER,
                                    tagId INTEGER,
                                    relevance NUMERIC,
                                    FOREIGN KEY (movieId) REFERENCES movies(movieId),
                                    FOREIGN KEY (tagId) REFERENCES genometags(tagId));"""%cls.name



class M20GenomeTag(M20Element):

    name = "genometags"

    def __init__(self, tagid, tag):
        M20Element.__init__(self)
        self.tagId = tagid
        self.tag = tag

        self.insertQuery = """INSERT INTO %s (tagId, tag) VALUES (%s, %s);"""%\
                           (self.name, '%s', '%s')

        self.insertData = (self.tagId,
                            self.tag)

    @classmethod
    def createQuery(cls):
        return """CREATE TABLE IF NOT EXISTS %s 
                                            (id serial PRIMARY  KEY,
                                            tagId INT UNIQUE,
                                             tag TEXT);"""%cls.name

