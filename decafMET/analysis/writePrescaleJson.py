import os, sys

import ROOT

import array
import json
import numpy as np

from processor import prescales

import argparse
import time

# Define input arguments
parser = argparse.ArgumentParser(description='')
parser.add_argument('-i', '--inFile', type=str, default='', help='Input json file')
parser.add_argument('-p', '--path', type=str, default='', help='HLT path')
args = parser.parse_args()

inFile = args.inFile
path = args.path

ps_dict = dict()
rootFile = ROOT.TFile.Open(inFile)
events = rootFile.Events
print(str(events.GetEntries())+" events in file")
i = 0
for event in events:
	tic1 = time.time()
	i += 1
	run_tmp = event.run
	lumi_tmp = event.luminosityBlock
	if not str(run_tmp) in ps_dict.keys():
		tic2 = time.time()
		print(run_tmp,lumi_tmp)
		ps_dict[str(run_tmp)] = dict()
		print(prescales.getPrescale(str(run_tmp),lumi_tmp,path,"/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json"))
		ps_dict[str(run_tmp)][str(lumi_tmp)] = prescales.getPrescale(str(run_tmp),lumi_tmp,path,"/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
		print("New run, "+str(time.time()-tic2)+" s")
	else:
		if not str(lumi_tmp) in ps_dict[str(run_tmp)].keys():
			tic3 = time.time()
			print(run_tmp,lumi_tmp)
			print(prescales.getPrescale(str(run_tmp),lumi_tmp,path,"/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json"))
			ps_dict[str(run_tmp)][str(lumi_tmp)] = prescales.getPrescale(str(run_tmp),lumi_tmp,path,"/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
			print("New lumi, "+str(time.time()-tic3)+" s")
	print("Event "+str(i)+", "+str(time.time()-tic1)+" s")

print(type(ps_dict),ps_dict)
