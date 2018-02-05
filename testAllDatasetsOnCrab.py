# use this script to test the status of the submitted tasks on CRAB

# NB: you need to source crab environment before lauching this script:
# source /cvmfs/cms.cern.ch/crab3/crab.sh

import os, glob, sys
import time
import datetime as dt
from subprocess import Popen, PIPE
import argparse

class ProcDataset:
    
    def __init__ (self):
        self.FolderName = ""
        self.DASurl = ""
        self.DatasetName = ""
        self.Nfailed = ""
        self.Nrunning = ""
        self.Nfinished = ""
        self.TaskStatus = ""
        self.TaskStatusOnCRAB = None
        self.subindex = None
        self.failureMessage = None

    def addFolderName (self, name):
        self.FolderName = name

    def addDASurl (self, url):
        self.DASurl = url
    
    def addDatasetName (self, dname):
        self.DatasetName = dname
    
    def setFailed (self, num):
        self.Nfailed = num

    def setRunning (self, num):
        self.Nrunning = num

    def setFinished (self, num):
        self.Nfinished = num
    
parser = argparse.ArgumentParser(description='Command line parser of CRAB submitter')
parser.add_argument('--tag',      dest='tag',      help='production tag', default=None)
args = parser.parse_args()
if not args.tag:
    print "Please provide a name (--tag) for this production"
    sys.exit()

path = args.tag
if path.split('_')[0] != 'crab3':
    path = 'crab3_' + path

if not os.path.isdir(path):
    print "Folder %s does not exist" % path
    sys.exit()

filesLevel1 = glob.glob('%s/*' % path)
dirsLevel1 = filter(lambda f: os.path.isdir(f), filesLevel1) # this is the list of folders under "path"

#os.system ("voms-proxy-init -voms cms")

datasets = [] #list of all processed datasets, filled from the list of folders

localtime = time.asctime( time.localtime(time.time()) )
fulllogname = (path + "/fullStatusLog_{}.txt").format( dt.datetime.now().strftime('%Y.%m.%d_%H.%M.%S') )
fulllog = open (fulllogname, "w")
fulllog.write ("Local current time : %s\n" % localtime)

for dirr in dirsLevel1:
    print dirr
    fulllog.write ("=== DIRECTORY TASK IS: %s\n" % dirr)
    procdataset = ProcDataset()
    procdataset.addFolderName (dirr)
    command = "crab status -d %s" % dirr
    procdataset.subindex = int(dirr.split('_')[-1])
    pipe = Popen(command, shell=True, stdout=PIPE)
    for line in pipe.stdout:
        fulllog.write(line)
        line = line.strip()
        print line
        blocks = line.split(':', 1) # split at 1st occurrence
        if blocks [0] == "Output dataset":
            dtsetName = blocks[1].strip()
            procdataset.addDatasetName (dtsetName)
        elif blocks [0] == "Task status":
            status = blocks[1].strip()
            procdataset.TaskStatus = status
        elif blocks [0] == "Output dataset DAS URL":
            procdataset.DASurl = blocks[1].strip()
        elif blocks [0] == "Status on the CRAB server":
            procdataset.TaskStatusOnCRAB = blocks[1].strip()
        elif blocks [0] == "Failure message from the server":
            procdataset.failureMessage = blocks[1].strip()
    datasets.append(procdataset)
    print "\n\n"
    fulllog.write("\n\n")

print "\n =================  ALL TASK STATUS (SHORT) ==================== \n\n"
for dt in datasets:
    print dt.subindex , " : " , dt.FolderName, dt.TaskStatusOnCRAB

print "\n =================  ALL TASK STATUS (LONG) ==================== \n\n"
for dt in datasets:
    print dt.subindex , " : " , dt.FolderName, dt.TaskStatusOnCRAB
    print "     failureMessage: " , dt.failureMessage

print "\n =================  TASK STATUS  ==================== \n\n"
print "failed: "
for dt in datasets:
    status = dt.TaskStatus
    if status == "FAILED":
        print "crab resubmit -d %s" % dt.FolderName

print "\n =================  PUBLICATION  ==================== \n\n"

noDataset = []
for dt in datasets:
    name = dt.DatasetName
    if name:
        print name
    else:
        noDataset.append(dt)
        print "\n"

print "\nNo Dataset name found for:"
for dt in noDataset:
    print dt.FolderName


print "\n =================  DAS URLS  ==================== \n\n"

#noUrl = []
for dt in datasets:
    url = dt.DASurl
    if url:
        print url
    else:
        #noUrl.append(dt)
        print "\n"

#print "\nNo Dataset name found for:"
#for dt in noDataset:
#    print dt.FolderName

