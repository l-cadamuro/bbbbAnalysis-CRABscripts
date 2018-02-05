# bbbbAnalysis-CRABscripts

Tools to create nanoAODs from miniAOD using CRAB.
Instructions to setup the nanoAOD code are here:
https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookNanoAOD

Follow the examples on the twiki to generate a cmsRun configuration file for data and MC in the release that corresponds to the samples you want to process.
These commands should create the files ``testXXX.py`` and ``test_data_XXX.py`` where ``XXX = 80X, 92X, 94X``

In both files add to the ``NanoAODOutputModule`` the following line to allow running on CRAB:

```fakeNameForCrab =cms.untracked.bool(True),```

Once completed the installation, do
```
cd ${CMSSW_BASE}/src/PhysicsTools/NanoAOD/test
git clone https://github.com/l-cadamuro/bbbbAnalysis-CRABscripts
cd bbbbAnalysis-CRABscripts
```
