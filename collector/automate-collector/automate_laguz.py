#! /usr/bin/python3

import shutil
import sys
import traceback
from subprocess import check_call

def runCollector(location, url, dbname) :


    runCount = 0
    while runCount < 3:
        try:
            # TODO: make location and urls args
            # run the capture
            cmd = ["../url-source", "capture", location, url, "CaptureResults"]
            check_call (cmd)

            # Rsync it back to kenaz
            rsyncCmd = ["rsync", "-r", "CaptureResults", "dbreceiver@kenaz.ece.cmu.edu:CaptureResults"]
            check_call (rsyncCmd)

            # Run import_batch on kenaz
            # TODO: Runs it in the background so running delete immediately after
            # should not be a problem?
            runKenaz = ["ssh", "dbreceiver@kenaz.ece.cmu.edu", "nohup", "python",
                    "runImportBatch.py", dbname, "~/CaptureResults", "&"]
            check_call (runKenaz)

            # Delete files
            shutil.rmtree ("CaptureResults");
        except Exception:
            traceback.print_exc()
            #print traceback.print_exc()
        runCount += 1

def main ():
    if (len(sys.argv) < 4):
        print ("usage: python automate_laguz <location_file> <url_file> <dbname>")
        return
    location_file = sys.argv[1]
    url_file = sys.argv[2]
    dbname = sys.argv[3]
    #dirs = sys.argv[4:]
    print (location_file)
    print (url_file)
    runCollector (location_file, url_file, dbname)

main()
