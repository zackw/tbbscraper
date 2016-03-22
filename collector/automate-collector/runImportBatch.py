#! /usr/bin/python3

import traceback
from subprocess import check_call


def runImportBatch (dbname, dirs)

    #TODO: better exception handling
    try:
        cmd = ["python", "../scripts/import_batch", dbname, dirs]
        check_call (cmd)
    except Exception.
        print "Error running import_batch"
        traceback.print_exc()

def main ():
    dbname = sys.argv[1]
    dirs = sys.argv[2:]
    runImportBatch(dbname, dirs)

main()
#runImportBatch()



