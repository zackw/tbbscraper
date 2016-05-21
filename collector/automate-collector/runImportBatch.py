#! /usr/bin/python3

import traceback
import os
import sys
import logging
from subprocess import check_call


def runImportBatch (dbname, dirs, log_dest, log_level):

    LOG_FILE = log_dest + ".log"
    logging.basicConfig (filename=LOG_FILE, level = log_level)

    try:
        cmd = ["python3", os.path.dirname(__file__)+"/../scripts/import-batch.py", dbname, dirs]
        check_call (cmd)
    except Exception:
        logging.exception ('Error running import_batch')

def get_log_level (level):
    return {
            "CRITICAL" : logging.CRITICAL,
            "ERROR"    : logging.ERROR,
            "WARNING"  : logging.WARNING,
            "INFO"     : logging.INFO,
            "DEBUG"    : logging.DEBUG,
            }[level]

def main ():
    dbname = sys.argv[1]
    dirs = sys.argv[2]
    log_dest = sys.argv[3]
    log_level = get_log_level (sys.argv[4].upper())
    runImportBatch(dbname, dirs, log_dest, log_level)

main()



