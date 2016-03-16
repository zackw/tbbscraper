#! /usr/bin/python3

import shutil
import traceback
from subprocess import check_call

def runCollector() :


    runCount = 0
    while runCount < 3:
        try:
            # TODO: make location and urls args
            # run the capture
            cmd = ["../url-source", "capture", location, urls, "CaptureResults"]
            check_call (cmd)

            # Rsync it back to kenaz
            rsyncCmd = ["rsync", "-r", "CaptureResults", "dbreceiver@kenaz.ece.cmu.edu:CaptureResults"]
            check_call (rsyncCmd)

            # Run import_batch on kenaz
            # TODO: Runs it in the background so running delete immediately after
            # should not be a problem?
            runKenaz = ["ssh", "dbreceiver@kenaz.ece.cmu.edu", "nohup", "python", "runImportBatch.py", "&"]
            check_call (runKenaz)

            # Delete files
            shutil.rmtree ("CaptureResults");
        except Exception:
            print traceback.print_exc()
        runCount++

runCollector()

