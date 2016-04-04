#! /usr/bin/python3

import traceback
import os
import sys
from subprocess import check_call


def runImportBatch (dbname, dirs):

    #TODO: better exception handling
    try:
        cmd = ["python3", os.path.dirname(__file__)+"/../scripts/import-batch.py", dbname, dirs]
        check_call (cmd)
    except Exception:
        print "Error running import_batch"
        traceback.print_exc()

def main ():
    dbname = sys.argv[1]
    dirs = sys.argv[2]
    print ("main")
    runImportBatch(dbname, dirs)
    print "runImportBatch Done"

main()
#runImportBatch()



