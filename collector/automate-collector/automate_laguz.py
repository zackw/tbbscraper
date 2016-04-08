#! /usr/bin/python3

import shutil
import sys
import traceback
import os
from subprocess import check_call

def runCollector(location, url, dbname) :


    runCount = 0
    while runCount < 3:
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
        except Exception:
            traceback.print_exc()


        tries = 0;
        while (tries < 3):
            try:
                runKenaz = ["ssh", "dbreceiver@kenaz.ece.cmu.edu", "nohup", "python",
                        "tbbscraper/collector/automate-collector/runImportBatch.py",
                        dbname, "CaptureResults", "&"]
                check_call (runKenaz)
                break
            except Exception as e:
                print e
                print ("SSH failed. Trying again in 5 mins...")
                time.sleep (300)
                tries+=1
                if (tries == 3):
                    check_call ('/usr/sbin/sendmail speddada@andrew.cmu.edu < toEmail.txt',
                            shell = True)


        try:
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
