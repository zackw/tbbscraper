#! /usr/bin/python3

import shutil
import sys
import traceback
import os
from subprocess import check_call

def runCollector(location, url, dbname) :


    runCount = 0
    while runCount < 1:
        try:
            cmd = [os.path.dirname(__file__)+"/../url-source", "capture",
                    location, url, "CaptureResults"]
            check_call (cmd)
            print ("Capture results done")

            # Rsync it back to kenaz
            rsyncCmd = ["rsync", "-r", "CaptureResults",
                    "dbreceiver@kenaz.ece.cmu.edu:CaptureResults"]
            check_call (rsyncCmd)

            print ("Rysc Done")

            # Run import_batch on kenaz
            # TODO: Runs it in the background so running delete immediately after
            # should not be a problem?
            runKenaz = ["ssh", "dbreceiver@kenaz.ece.cmu.edu", "nohup", "python",
                    "tbbscraper/collector/automate-collector/runImportBatch.py",
                    dbname, "~/CaptureResults", "&"]
            check_call (runKenaz)

            # Delete files
            shutil.rmtree ("CaptureResults");
        except Exception:
            traceback.print_exc()
        runCount += 1

def main ():
    if (len(sys.argv) < 4):
        print ("usage: python automate_laguz <location_file> <url_file> <dbname>")
        return
    location_file = sys.argv[1]
    url_file = sys.argv[2]
    dbname = sys.argv[3]
    runCollector (location_file, url_file, dbname)

main()
