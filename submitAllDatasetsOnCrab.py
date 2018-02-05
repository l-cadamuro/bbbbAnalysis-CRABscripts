# NB: you need to source crab environment before lauching this script:
# source /cvmfs/cms.cern.ch/crab3/crab.sh

import os
import sys
import re
import argparse

###################################################################
#### Parameters to configure

# PROCESS = ["DATA_BTAGCSV_80X", "DATA_JETHT_80X", "MC_SIGNALS_80X"]
PROCESS = ["DATA_BTAGCSV_80X_RESUB", "DATA_JETHT_80X_RESUB"]
datasetsFile = "datasets.txt"
lumiMaskFileName = 'Cert_271036-284044_13TeV_23Sep2016ReReco_Collisions16_JSON.txt'

cmsRun_data = '../test_data_80X_NANO.py'
cmsRun_MC   = '../test80X_NANO.py'

##################################################################

parser = argparse.ArgumentParser(description='Command line parser of CRAB submitter')
parser.add_argument('--tag',      dest='tag',      help='production tag', default=None)
parser.add_argument('--isData',   dest='is_data',  help='mark as data (overrides deduction from process name)', action = 'store_true',  default=None)
parser.add_argument('--isMC',     dest='is_MC',    help='mark as MC   (overrides deduction from process name)', action = 'store_true',  default=None)
parser.add_argument('--no-exec',  dest='execCRAB', help='do not launch CRAB (for debug)', action = 'store_false',  default=True)
parser.add_argument('--no-proxy', dest='execProy', help='skip proxy creation',   action = 'store_false',  default=True)
parser.add_argument('--nunits',   dest='nunits',   help='units per job in CRAB (to override the default in CRAB_template', type=int, default=None)

args = parser.parse_args()

if not args.tag:
    print "Please provide a name (--tag) for this production"
    sys.exit()

if args.is_data and args.is_MC:
    print "The sample is flagged at the same time as data (--isData) and MC (--isMC)"
    sys.exit()

isData = None
isData = args.is_data   if args.is_data else isData
isData = not args.is_MC if args.is_MC   else isData

if isData != None:
    print "** This entire production was manually flagged as isData? =", isData

tag = args.tag
print "** Production tag:", tag

#twiki page with JSON files info https://twiki.cern.ch/twiki/bin/viewauth/CMS/PdmV2015Analysis
#50ns JSON file to be used on 2015B and 2015C PDs - integrated luminosity: 71.52/pb - 18/09/2015
#lumiMaskFileName = "/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/Cert_246908-255031_13TeV_PromptReco_Collisions15_50ns_JSON_v2.txt"

#25ns JSON file to be used on 2015C and 2015D PDs - integrated luminosity: 2.11/fb - 13/11/2015
#lumiMaskFileName = "/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/Cert_246908-260627_13TeV_PromptReco_Collisions15_25ns_JSON.txt"
#lumiMaskFileName = "/home/llr/cms/cadamuro/HiggsTauTauFramework/CMSSW_7_4_7/src/LLRHiggsTauTau/NtupleProducer/test/diffLumiMasks/LumiMask_Diff_2p11fb_minus_1p56_13Nov2015.txt"
#lumiMaskFileName  = "/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/Cert_246908-260627_13TeV_PromptReco_Collisions15_25ns_JSON.txt"

#25ns SILVER JSON : 2.46/fb
# lumiMaskFileName  = "/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/Cert_246908-260627_13TeV_PromptReco_Collisions15_25ns_JSON_Silver.txt"

#25ns SILVER JSON : 2.63/fb - dec2016 re-reco
# lumiMaskFileName = "/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/Reprocessing/Cert_13TeV_16Dec2015ReReco_Collisions15_25ns_JSON_Silver.txt"

# 16 giu Golden JSON 2016
# lumiMaskFileName = "/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Cert_271036-274443_13TeV_PromptReco_Collisions16_JSON.txt"

# 22 giu Golden JSON 2016 -- 3.99/fb
#lumiMaskFileName = "/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Cert_271036-275125_13TeV_PromptReco_Collisions16_JSON.txt"

# # 22 giu Golden JSON 2016 MINUS 16 giu Golden JSON 2016
# lumiMaskFileName = "22giuJSON_diff_16giuJSON.txt"

## 8 lug JSON MINUS 22 giu JSON
# compareJSON.py --sub /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Cert_271036-275783_13TeV_PromptReco_Collisions16_JSON.txt /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Cert_271036-275125_13TeV_PromptReco_Collisions16_JSON.txt 8lugJSON_diff_22giuJSON.txt
# lumiMaskFileName = "15lug_NoL1TJSON_diff_8lugJSON.txt"
# lumiMaskFileName = "20lug_NoL1TJSON_diff_15lug_NoL1TJSON.txt"
# lumiMaskFileName = 'Cert_271036-284044_13TeV_23Sep2016ReReco_Collisions16_JSON.txt'

# FastJobs          = False # controls number of jobs - true if skipping SVfit, false if computing it (jobs will be smaller)
# VeryLong          = True # controls time for each job - set to true if jobs contain many real lepton pairs --> request for more grid time
# EnrichedToNtuples = False # use only False! Do not create ntuples on CRAB because it is very slow, use tier3
# PublishDataset    = False # publish dataset; set to false if producing ntuples


###################################################################
#### Automated script starting

# dataset block definition
comment = "#"
sectionBeginEnd = "==="

PublishDataset = False

# check if file with dataset exist
if not os.path.isfile(datasetsFile):
    print "File %s not found!!!" % datasetsFile
    sys.exit()

#check if directory exists
crabJobsFolder = "crab3_" + tag
if os.path.isdir(crabJobsFolder):
    print "Folder %s already exists, please change tag name or delete it" % crabJobsFolder
    sys.exit()

# grep all datasets names, skip lines with # as a comment
# block between === * === are "sections" to be processed

### determine if a process is data or MC from the name
### unless it is overridden from cmd line

proc_isData = {}
for pr in PROCESS:
    if isData != None: ## overridden from cmd line
        proc_isData[pr] = isData
    else:
        thetag = pr.split('_')[0]
        if thetag == "MC":
            proc_isData[pr] = False
        elif thetag == "Data" or thetag == "DATA":
            proc_isData[pr] = True
        else:
            print " >>> cannot infer if process", pr, 'is Data or MC, assuming it is MC'
            proc_isData[pr] = False

currSection = ""
dtsetToLaunch = []
dtsetIsData   = []

print " =========  Starting submission on CRAB ========"
print " Parameters: "
print " PROCESS: "
for idx, pr in enumerate(PROCESS): print "   * {: <30} is data? {:}".format(pr, proc_isData[pr])
print " tag: " , tag
# print " Fast jobs?: " , FastJobs
print " Publish?: "   , PublishDataset

# READ INPUT FILE
with open(datasetsFile) as fIn:
    for line in fIn:
        line = line.strip() # remove newline at the end and leding/trailing whitespaces
        
        if not line: #skip empty lines
            continue

        if comment in line:
            continue
        
        #print line        
        words = line.split()
        if len(words) >= 3:
            if words[0] == sectionBeginEnd and words[2] == sectionBeginEnd: 
                currSection = words[1]
        else:
            if currSection in PROCESS:
                dtsetToLaunch.append(line)
                dtsetIsData.append(proc_isData[currSection])

# CREATE CRAB JOBS
os.system ('echo "Sourcing crab3 environment"; source /cvmfs/cms.cern.ch/crab3/crab.sh')
if args.execProy: os.system ("voms-proxy-init -voms cms")

for name in PROCESS: crabJobsFolder + "_" + name
print "** Will execute CRAB in folder:", crabJobsFolder
os.system ("mkdir %s" % crabJobsFolder)

counter = 1 # appended to the request name to avoid overlaps between datasets with same name e.g. /DoubleEG/Run2015B-17Jul2015-v1/MINIAOD vs /DoubleEG/Run2015B-PromptReco-v1/MINIAOD
outlog = open ((crabJobsFolder + "/submissionLog.txt"), "w")
outlog.write (" =========  Starting submission on CRAB ========\n")
outlog.write (" Parameters: \n")
outlog.write (" PROCESS: \n")
# for pr in PROCESS: outlog.write ("   * %s\n" % pr)
for idx, pr in enumerate(PROCESS): outlog.write("   * {: <30} is data? {:}\n".format(pr, proc_isData[pr]))
outlog.write (" tag: %s\n" % tag)
# outlog.write (" Fast jobs?: %s\n" % str(FastJobs))
outlog.write (" Publish?: %s\n"   % str(PublishDataset))
outlog.write (" ===============================================\n\n\n")

for idx, dtset in enumerate(dtsetToLaunch):
    thisIsData = dtsetIsData[idx]
    dtsetNames = dtset
    if '/MINIAODSIM' in dtset:
        dtsetNames = dtset.replace('/MINIAODSIM', "")
    elif '/MINIAOD' in dtset:
        dtsetNames = dtset.replace('/MINIAOD', "")
    dtsetNames = dtsetNames.replace('/', "__")
    dtsetNames = dtsetNames.strip("__") # remove leading and trailing double __ 
    shortName = dtset.split('/')[1]

    if (len(shortName) > 95): # requestName not exceed 100 Characters!
        toRemove = len (shortName) - 95
        shortName = shortName[toRemove:]

    #dtSetName = dtsetNames[1]
    command = "crab submit -c crab3_template.py"
    #command += " General.requestName=%s" % (dtsetNames + "_" + tag + "_" + str(counter))
    command += " General.requestName=%s" % (shortName + "_" + str(counter))
    command += " General.workArea=%s" % crabJobsFolder
    command += " Data.inputDataset=%s" % dtset
    command += " Data.outLFNDirBase=/store/user/lcadamur/HHNtuples/%s/%s" % (tag , str(counter)+"_"+dtsetNames)
    command += " Data.outputDatasetTag=%s" % (shortName + "_" + tag + "_" + str(counter))
    # if (EnrichedToNtuples): command += " Data.inputDBS=phys03" # if I published the dataset need to switch from global (default)
    # if (EnrichedToNtuples): command += " JobType.psetName=ntuplizer.py" # run a different python config for enriched
    command += " JobType.psetName=%s" % (cmsRun_data if thisIsData else cmsRun_MC)

    command += " Data.publication=%s" % ("True" if PublishDataset else "False") # cannot publish flat root ntuples
    if args.nunits        : command += " Data.unitsPerJob=%i" % args.nunits
    # if VeryLong           : command += " JobType.maxJobRuntimeMin=2500" # 32 hours, default is 22 hours -- can do up to 2800 hrs
    if thisIsData         : command += " Data.lumiMask=%s" % lumiMaskFileName
    print command ,  "\n"
    if args.execCRAB: os.system (command)
    outlog.write(command + "\n\n")
    counter = counter + 1