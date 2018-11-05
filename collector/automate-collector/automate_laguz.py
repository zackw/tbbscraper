#! /usr/bin/python3

import shutil
import sys
import traceback
import os
import logging
import time
import datetime
from subprocess import check_call


def runCollector(location, url, dbname, results_dir, ssh_dest, log_dest,
        log_level, emailId, quiet) :

    LOG_FILE = log_dest + ".log"
    logging.basicConfig(filename=LOG_FILE, level = log_level)

    startTime = time.time()
    try:
        cmd = [os.path.dirname(__file__)+"/../url-source", "capture",
            location, url, results_dir]
        if (quiet):
            cmd += ["-q"]
        logging.info (str(datetime.datetime.now()))
        logging.info ("Running collector")
        check_call (cmd)
        logging.debug ("Capture results done")

        # Rsync it back to kenaz
        rsyncCmd = ["rsync", "-r", results_dir,
                ssh_dest  + ":" + results_dir]
        check_call (rsyncCmd)
        logging.debug ("Rysc Done")

    except Exception:
        logging.exception ('Exception running capture')


    # SSH to kenaz. Retry at most 3 times. Email if it fails 3 times
    tries = 0;
    while (tries < 3):
        try:
            runKenaz = ["ssh", ssh_dest, "nohup", "python",
                     "tbbscraper/collector/automate-collector/runImportBatch.py",
                    dbname, results_dir, log_dest, logging.getLevelName(log_level), "&"]
            check_call (runKenaz)
            break
        except Exception as e:
            logging.exception ('Failed to SSH to kenaz')
            logging.debug ("SSH failed. Trying again in 5 mins...")
            time.sleep (300)
            tries+=1
            if (tries == 3):
                check_call (('/usr/sbin/sendmail ' + emailId + ' < ' +
                    os.path.dirname(__file__) +'/toEmail.txt'),shell = True)

    try:
        # Delete files
        shutil.rmtree (results_dir);
    except Exception:
        logging.exception ('Exception on remove tree')

    logging.info ("Time to completion: %s\n" % (time.time() - startTime))

def get_log_level (level):
    return {
            "CRITICAL" : logging.CRITICAL,
            "ERROR"    : logging.ERROR,
            "WARNING"  : logging.WARNING,
            "INFO"     : logging.INFO,
            "DEBUG"    : logging.DEBUG,
            }[level]


def main ():
    if (len(sys.argv) < 9):
        # log levels CRITICAL, ERROR, WARNING, INFO, DEBUG
        print ("usage: python automate_laguz.py <location_file> <url_file> <dbname>" +
        "<results_dir> <ssh_dest> <log_dest> <log_level> <email>")
        return
    quiet = ((len(sys.argv) == 10) and (sys.argv[9] == "-q"))

    location_file = sys.argv[1]
    url_file = sys.argv[2]
    dbname = sys.argv[3]
    results_dir = sys.argv[4]
    ssh_dest = sys.argv[5]
    log_dest = sys.argv[6]
    log_level = get_log_level(sys.argv[7].upper())
    emailId = sys.argv[8]

    runCollector (location_file, url_file, dbname, results_dir, ssh_dest,
            log_dest, log_level, emailId, quiet)

main()
