import os
import sys
import re
import argparse
import collections
from subprocess import Popen, PIPE

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split('(\d+)', text) ]


parser = argparse.ArgumentParser(description='Command line parser of CRAB submitter')
parser.add_argument('--tag',      dest='tag',              help='production tag',           default=None, required=True)
parser.add_argument('--out',      dest='out',              help='path to output folder',    default=None, required=True)
parser.add_argument('--no-xrootd',  dest='addxrootd',       help='add xrootd serv to LFN',   default=True, action='store_false')
parser.add_argument('--plain-list', dest='eoslist',         help='use plain ls instead of eos', default=True, action='store_false')
# parser.add_argument('--skip',     dest='skip', nargs='+',  help='list of elements to skip (do all the others)', default=None)
# parser.add_argument('--keep',     dest='keep', nargs='+',  help='list of elements to dp (skip all the others)', default=None)
args = parser.parse_args()

tag = args.tag
outFolder = args.out

if not os.path.isdir(outFolder):
    print "** folder", outFolder, "does not exist, exiting"
    sys.exit()

if tag.startswith("crab3_"):
    tag = tag.replace("crab3_","",1)

print "... listing the production", tag

# # areEnrichedMiniAOD = False; # if true:  add a header and the /store.. etc to run ntuplizer on Tier3 on CMSSW
#                                  # if false: only add the polgrid server to run the skim and submit on root
# # ====================================================================

# def formatName (name, pathToRem, patToPrepend):
#     name.strip(pathToRem)
#     name = "root://polgrid4.in2p3.fr/" + name
#     return name

def saveToFile (lista, filename, forCMSSW=False):
    f = open (filename, "w")

    if not forCMSSW: ## plain write
        for elem in lista:
            f.write("%s\n" % elem) #vintage
    
#     else: ## fragmento to copy in a CMSSW py config
#         f.write("FILELIST = cms.untracked.vstring()\n")
#         f.write("FILELIST.extend ([\n")
#         for elem in lista:
#             elem = elem.replace ("root://polgrid4.in2p3.fr//dpm/in2p3.fr/home/cms/trivcat", "")
#             f.write ("    '")
#             f.write("%s',\n" % elem) #vintage  
#         f.write("])\n")
 
#     f.close()


# # ====================================================================

## the part below works (07/02/2018) but I don't know how to use it
# toRun  = []
# toSkip = []
#
# if args.skip and args.keep:
#     print "** both --keep and --skip options given, don't know what to do"
#     sys.exit()
#
# if args.keep:
#     print "... doing only the folders:"
#     for k in args.keep:
#         if 'crab_' in k:
#             k = k.replace("crab_: ","",1)
#         print '...', k
#         toRun.append(k)
#
# if args.skip:
#     print "... skipping the folders:"
#     for s in args.skip:
#         if 'crab_' in s:
#             s = s.replace("crab_: ","",1)
#         print '...', s
#         toSkip.append(k)


# print useOnly
# dpmhome = "/dpm/in2p3.fr/home/cms/trivcat"
# partialPath = "/store/user/lcadamur/HHNtuples/" #folder contenente la produzione
# # partialPath = "/store/user/salerno/HHNtuples/"
# #partialPath = "/store/user/davignon/EnrichedMiniAOD/"
# #partialPath = "/store/user/gortona/HHNtuples/"


## for standard ls
if not args.eoslist:
    dpmhome = "/eos/uscms"
    command_proto = "ls -v -1 {0}"

## for eosls 
else:
    dpmhome = ""
    command_proto = "eos root://cmseos.fnal.gov ls {0}"

partialPath = "/store/user/lcadamur/HHNtuples/"
xrootd_path = 'root://cmsxrootd.fnal.gov/'

path = dpmhome + partialPath + tag
if outFolder[-1] != "/":
    outFolder += '/'

print ".. searching in", path

# command = "/usr/bin/rfdir %s | awk '{print $9}'" % path
# print command
# pipe = Popen(command, shell=True, stdout=PIPE)
command = command_proto.format(path)
pipe = Popen(command, shell=True, stdout=PIPE)

allLists = collections.OrderedDict() #dictionary

for line in pipe.stdout:
    line = line.strip()
    # if toRun and not line in toRun:
    #         continue
    # if toSkip and line in toSkip:
    #         continue
    # print line
    samplesPath = (path + "/" + line).strip()    
    sampleName = line
    allLists[sampleName] = []
    print sampleName.strip()
    for level in range(0, 3): #sampleName, tag, hash subfolders
        # comm = "/usr/bin/rfdir %s | awk '{print $9}'" % samplesPath.strip()
        comm = command_proto.format(samplesPath.strip())
        #print "comm: ==> " , comm
        pipeNested = Popen (comm, shell=True, stdout=PIPE)
        out = pipeNested.stdout.readlines()
        numLines = len (out)
        if numLines > 0 :
            if numLines > 1 : print "  *** WARNING: In %s too many subfolders, using last one (most recent submission)" % samplesPath        
            samplesPath = samplesPath + "/" + out[-1].strip()
    #print samplesPath
    # now I have to loop over the folders 0000, 1000, etc...
    # comm = "/usr/bin/rfdir %s | awk '{print $9}'" % samplesPath.strip()
    comm = command_proto.format(samplesPath.strip())
    pipeNested = Popen (comm, shell=True, stdout=PIPE)
    out = pipeNested.stdout.readlines()
    for subfol in out:
        finalDir = samplesPath + "/" + subfol.strip()
        # getFilesComm = ""
        # if areEnrichedMiniAOD :
        #     getFilesComm = "/usr/bin/rfdir %s | grep Enriched_miniAOD | awk '{print $9}'" % finalDir.strip()
        # else :
        #     getFilesComm = "/usr/bin/rfdir %s | grep HTauTauAnalysis | awk '{print $9}'" % finalDir.strip()
        getFilesComm = command_proto.format(finalDir.strip()) + '/*.root'
            #print getFilesComm
        pipeGetFiles = Popen (getFilesComm, shell=True, stdout=PIPE)
        outGetFiles = pipeGetFiles.stdout.readlines()
        for filename in outGetFiles:
            # name = formatName (finalDir + "/" + filename.strip(), dpmhome)
            if args.eoslist: name = finalDir + "/" + filename.strip()
            else:            name = filename.strip()
            if args.addxrootd:
                name = name.replace(dpmhome, '', 1)
                name = xrootd_path + name
            allLists[sampleName].append (name)

# now I have all file lists, save them
for sample, lista in allLists.iteritems():
   if lista:
      lista.sort(key=natural_keys) ## to get 1,2..9,10,11.. order instea of 1,11,12..19,2,20,..
      outName = outFolder + sample.strip()+".txt"
      # if areEnrichedMiniAOD : outName = outFolder + sample.strip()+".py"
      # saveToFile (lista, outName, areEnrichedMiniAOD)
      saveToFile (lista, outName)
   else:
      print "  *** WARNING: Folder for dataset      {:<50} is empty".format(sample)
