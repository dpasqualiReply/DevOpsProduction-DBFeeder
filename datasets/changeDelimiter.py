import sys
import os

if __name__ == '__main__':

    if(len(sys.argv) == 1):
        print ("****** USAGE: python changeDelimiter.py filename currentDelimiter newDelimiter")
        sys.exit(1)

    print sys.argv[0]
    print sys.argv[1]
    print sys.argv[2]
    print sys.argv[3]


    findDel = sys.argv[2]
    changeDel = sys.argv[3]

    f = file(sys.argv[1], "r")
    l = list()

    for line in f:
        print line
        changed = line.replace(findDel, changeDel)
        print changed
        l.append(changed)

    f.close()
    f = file(sys.argv[1], "w")
    for line in l:
        f.write(line)
    f.close()

