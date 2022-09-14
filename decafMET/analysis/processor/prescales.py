#! /usr/bin/env python
import json
import time

def rm_hlt_version(name):
	version_start = name.rfind("_v")
	if version_start == -1:
		return name
	else:
		return name[:version_start+2]

def get_pathname_from_ps_tbl(entry):
	hlt_path = entry.split()[0]
	return rm_hlt_version(hlt_path)

def get_hlt_prescales(ps_tbl,pathname):
	for line in ps_tbl:
		if get_pathname_from_ps_tbl(line[1]) == pathname:
			return line
	return None

def get_l1_prescales(l1_ps_tbl,l1_seed):
	for line in l1_ps_tbl:
		if line[1] == l1_seed:
			return line
	return None

def getPrescale(run, lumi, path, run_json, ps_json):
	all_runs_info = {}
	with open(run_json) as f:
		all_runs_info = json.load(f)

	ps_data = {}
	with open(ps_json) as f:
		ps_data = json.load(f)

	hlt_menu = all_runs_info[run]["hlt_menu"]
	ps_column = "-1"

	for key in all_runs_info[run]["ps_cols"]:
		if ps_column != "-1": break
		for lumi_range in all_runs_info[run]["ps_cols"][key]:
			if (lumi >= lumi_range[0]) and (lumi <= lumi_range[1]):
				ps_column = key
				break

	if ps_column == "-1": return 1
	else:
		ps_tbl = ps_data[hlt_menu]
		path_prescales = get_hlt_prescales(ps_tbl,path)
		return int(path_prescales[int(ps_column)+2])

#if __name__ == '__main__':
#	print(getPrescale("273158",3,"HLT_Photon50_R9Id90_HE10_IsoM_v","../data/triggerData2016_runInfo.json","../data/triggerData2016_hltprescales.json"))
