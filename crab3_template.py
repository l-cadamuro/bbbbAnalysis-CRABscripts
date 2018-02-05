from CRABClient.UserUtilities import config, getUsernameFromSiteDB
print "your username is:", getUsernameFromSiteDB()

config = config()

config.General.requestName = 'MY_REQUEST_NAME'
config.General.workArea = 'crab3'
# config.General.transferOutputs = True
# config.General.transferLogs = True

config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'test80X_NANO.py'
#config.JobType.outputFiles = ['lzma.root']

config.Data.inputDataset = '/MY/PRECIOUS/DATASET'
config.Data.inputDBS = 'global'
config.Data.splitting = 'EventAwareLumiBased'
config.Data.unitsPerJob = 30000 ## remember than n_max_job = 10000. NOTEL 30k / job is OK (a few hours to run)
config.Data.totalUnits = -1
config.Data.outLFNDirBase = '/store/user/%s/NanoTest/' % (getUsernameFromSiteDB())
config.Data.publication = False
config.Data.outputDatasetTag = 'MY_DATASET_TAG'

config.Site.storageSite = 'T3_US_FNALLPC'
