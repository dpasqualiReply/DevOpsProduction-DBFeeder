import os
import csv


class DatasetReader(object):
    """
    For each file we have
    next#Next line to read
    """

    def __init__(self, filename, toread=0, delimiter=',', filesize=0, init=False):
        self.filename = filename
        self.filesize = filesize
        self.toRead = toread
        self.delimiter = delimiter
        self.info = dict()

        if os.path.exists(filename + '.info') | init is True:

            f = file(filename + '.info', "w")
            self.info['next'] = 0
            f.write('next#0\n')
            f.close()

            print filename +'.info created'

        else:

            f = file(filename + '.info', "r+")
            for line in f:
                s = line.split('#')
                self.info[s[0]] = int(s[1])
            f.close()


    def updateInfo(self):
        f = file(self.filename + '.info', "w")
        f.write('next#'+str(self.info['next'])+'\n')
        f.close()

    def readPercentage(self):
        
        print 'to read ---> ' + str(self.toRead)
        print 'next ---> ' + str(self.info['next'])
        print 'filesize ---> ' + str(self.filesize)
        
        with open(self.filename, 'rb') as dataset:

            readable = self.toRead            

            if (self.toRead + self.info['next']) > self.filesize:
                readable = self.filesize - self.info['next']
            
            next = self.info['next'] + readable

            subset = list()

            for i, line in enumerate(csv.DictReader(dataset, delimiter=self.delimiter)):
                if(i >= self.info['next'] and i < next):
                    subset.append(line)
                
                if(i >= next):
                    break
            
            self.info['next'] = next
            self.updateInfo()

            return subset
        
        """
        with open(self.filename, 'rb') as dataset:
            reader = csv.DictReader(dataset, delimiter=self.delimiter)

            rows = list()

            for row in reader:
                rows.append(row)

            readable = self.toRead

            

            if (self.toRead + self.info['next']) > self.filesize:
                readable = self.filesize - self.info['next']

            next = self.info['next'] + readable
            subset = rows[self.info['next'] : next]

            self.info['next'] = next
            self.updateInfo()

            return subset
        """

    @classmethod
    def initWithFraction(cls, filename, fractionToRead, delimiter, init=False):

        size = sum(1 for r in open(filename, 'rb'))
        print(size)
        return DatasetReader(filename, int(size*fractionToRead), delimiter, size, init)