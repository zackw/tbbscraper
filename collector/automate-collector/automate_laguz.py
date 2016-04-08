#! /usr/bin/python3

import shutil
import sys
import traceback
import os
import logging
from subprocess import check_call

def runCollector(location, url, dbname) :

    LOG_FILE = os.path.dirname(__file__)+"/collectorErrors.log"
    logging.basicConfig(filename=LOG_FILE, level = logging.WARNING)

    runCount = 0
    timeFile = open ("timingLog.txt", 'w')

    while runCount < 3:
        startTime = time.time()
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
            logging.exception ('Exception running capture')
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
                logging.exception ('Failed to SSH to kenaz')
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
            logging.exception ('Exception on remove tree')
            traceback.print_exc()
        runCount += 1
        timeFile.write ("RUN " + runCount + ": Time: " + (time.time() - startTime) + '\n')
    timeFile.close()

def main ():
    if (len(sys.argv) < 4):
        print ("usage: python automate_laguz <location_file> <url_file> <dbname>")
        return
    location_file = sys.argv[1]
    url_file = sys.argv[2]
    dbname = sys.argv[3]
    runCollector (location_file, url_file, dbname)

main()
