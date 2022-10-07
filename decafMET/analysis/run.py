import os, sys

import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea import processor, hist, util
from coffea.nanoevents.methods import vector
from coffea.nanoevents.methods.vector import ThreeVector

import uproot3
import hist2root
import ROOT
from ROOT import TFile

import array
import json
import numpy as np
import argparse

import time

from dask.distributed import Client
from dask_jobqueue.htcondor import HTCondorCluster

import performance

def read_json_file(filename):
    with open(filename, 'r') as f:
        cache = f.read()
        data = eval(cache)
    return data

# Define input arguments
parser = argparse.ArgumentParser(description='')
parser.add_argument('-i', '--inFile', type=str, default='', help='Input json file')
args = parser.parse_args()

inFile = args.inFile

with open(inFile) as json_file:
	tic = time.time()

	#cluster = HTCondorCluster(cores=8,memory='2GB',disk='1 GB',log_directory='condor_log')
	#cluster.adapt(minimum=0, maximum=10)
	#client = Client(cluster)

	data = json.load(json_file)
	for a in data.keys():
		print(a)
		fileset = {a: data[a]}
		exe_args = {
			"skipbadfiles": True,
			"schema": NanoAODSchema,
			"workers": 10,
			"savemetrics": True,
		}
		#exe_args = {
		#	"client": client,
		#	"skipbadfiles": True,
		#	"savemetrics": True,
		#	"schema": NanoAODSchema,
		#	"align_clusters": True,
		#}
		result, metrics = processor.run_uproot_job(
			fileset,
			"Events",
			processor_instance=performance.performanceProcessor(),
			#executor=processor.iterative_executor,
			executor=processor.futures_executor,
			#executor=processor.dask_executor,
			executor_args=exe_args
		)
		util.save(result,'results/2022/'+a+'.coffea')
		output_root_file = 'results/2022/'+a+'.root'

		elapsed = time.time() - tic
		print(f"Output: {result}")
		print(f"Metrics: {metrics}")
		print(f"Finished in {elapsed:.1f}s")
		print(f"Events/s: {metrics['entries'] / elapsed:.0f}")
		
		outputfile = uproot3.recreate(output_root_file)
		for var in result.keys():
			if var == 'histo1':
				outputfile['met_raw_phi'] = hist.export1d(result[var].sum('dataset','met_pf_phi','met_puppi_phi'))
				outputfile['met_pf_phi'] = hist.export1d(result[var].sum('dataset','met_raw_phi','met_puppi_phi'))
				outputfile['met_puppi_phi'] = hist.export1d(result[var].sum('dataset','met_raw_phi','met_pf_phi'))
			elif var == 'histo2':
				outputfile['met_pf_phi_JECCorr'] = hist.export1d(result[var].sum('dataset','met_puppi_phi_JECCorr'))
				outputfile['met_puppi_phi_JECCorr'] = hist.export1d(result[var].sum('dataset','met_pf_phi_JECCorr'))
			elif var == 'histo3':
				outputfile['qt'] = hist.export1d(result[var].sum('dataset','pv','response_raw','met_raw'))
				outputfile['pv'] = hist.export1d(result[var].sum('dataset','qt','response_raw','met_raw'))
				outputfile['met_raw'] = hist.export1d(result[var].sum('dataset','qt','pv','response_raw'))
				outputfile['response_raw'] = hist.export1d(result[var].sum('dataset','qt','pv','met_raw'))
			elif var == 'histo4':
				outputfile['met_pf'] = hist.export1d(result[var].sum('dataset','qt','pv','response_pf'))
				outputfile['response_pf'] = hist.export1d(result[var].sum('dataset','qt','pv','met_pf'))
			elif var == 'histo5':
				outputfile['met_pf_JECCorr'] = hist.export1d(result[var].sum('dataset','qt','pv','response_pf_JECCorr'))
				outputfile['response_pf_JECCorr'] = hist.export1d(result[var].sum('dataset','qt','pv','met_pf_JECCorr'))
			elif var == 'histo6':
				outputfile['met_puppi'] = hist.export1d(result[var].sum('dataset','qt','pv','response_puppi'))
				outputfile['response_puppi'] = hist.export1d(result[var].sum('dataset','qt','pv','met_puppi'))
			elif var == 'histo7':
				outputfile['met_puppi_JECCorr'] = hist.export1d(result[var].sum('dataset','qt','pv','response_puppi_JECCorr'))
				outputfile['response_puppi_JECCorr'] = hist.export1d(result[var].sum('dataset','qt','pv','met_puppi_JECCorr'))
			elif var == 'histo8':
				outputfile['upar_raw_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','pv'))
			elif var == 'histo9':
				outputfile['upar_pf_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','pv','upar_pf_plus_qt_JECCorr'))
				outputfile['upar_pf_plus_qt_JECCorr'] = hist.export1d(result[var].sum('dataset','qt','pv','upar_pf_plus_qt'))
			elif var == 'histo10':
				outputfile['upar_puppi_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','pv','upar_puppi_plus_qt_JECCorr'))
				outputfile['upar_puppi_plus_qt_JECCorr'] = hist.export1d(result[var].sum('dataset','qt','pv','upar_puppi_plus_qt'))
			elif var == 'histo11':
				outputfile['uperp_raw'] = hist.export1d(result[var].sum('dataset','qt','pv'))
			elif var == 'histo12':
				outputfile['uperp_pf'] = hist.export1d(result[var].sum('dataset','qt','pv','uperp_pf_JECCorr'))
				outputfile['uperp_pf_JECCorr'] = hist.export1d(result[var].sum('dataset','qt','pv','uperp_pf'))
			elif var == 'histo13':
				outputfile['uperp_puppi'] = hist.export1d(result[var].sum('dataset','qt','pv','uperp_puppi_JECCorr'))
				outputfile['uperp_puppi_JECCorr'] = hist.export1d(result[var].sum('dataset','qt','pv','uperp_puppi'))
			elif var == 'histo14':
				outputfile['nEventsTot'] = hist.export1d(result[var])
			elif var == 'histo15':
				outputfile['nEventsLumi'] = hist.export1d(result[var])
			elif var == 'histo16':
				outputfile['nEventsFilters'] = hist.export1d(result[var])
			elif var == 'histo17':
				outputfile['nEventsTriggers'] = hist.export1d(result[var])
			elif var == 'histo18':
				outputfile['nEventsNPV'] = hist.export1d(result[var])
			elif var == 'histo19':
				outputfile['nEventsMuon'] = hist.export1d(result[var])
		outputfile.close()
		
		infile = TFile.Open(output_root_file,"update")
		infile.cd()

		# Define 2D histos
		h_2D_response_raw_vs_qt = hist2root.convert(result['histo3'].sum('dataset','pv','met_raw'))
		h_2D_response_pf_vs_qt = hist2root.convert(result['histo4'].sum('dataset','pv','met_pf'))
		h_2D_response_pf_JECCorr_vs_qt = hist2root.convert(result['histo5'].sum('dataset','pv','met_pf_JECCorr'))
		h_2D_response_puppi_vs_qt = hist2root.convert(result['histo6'].sum('dataset','pv','met_puppi'))
		h_2D_response_puppi_JECCorr_vs_qt = hist2root.convert(result['histo7'].sum('dataset','pv','met_puppi_JECCorr'))

		h_2D_response_raw_vs_pv = hist2root.convert(result['histo3'].sum('dataset','pv','met_raw'))
		h_2D_response_pf_vs_pv = hist2root.convert(result['histo4'].sum('dataset','pv','met_pf'))
		h_2D_response_pf_JECCorr_vs_pv = hist2root.convert(result['histo5'].sum('dataset','pv','met_pf_JECCorr'))
		h_2D_response_puppi_vs_pv = hist2root.convert(result['histo6'].sum('dataset','pv','met_puppi'))
		h_2D_response_puppi_JECCorr_vs_pv = hist2root.convert(result['histo7'].sum('dataset','pv','met_puppi_JECCorr'))

		h_2D_upar_raw_vs_qt = hist2root.convert(result['histo8'].sum('dataset','pv'))
		h_2D_upar_pf_vs_qt = hist2root.convert(result['histo9'].sum('dataset','pv','upar_pf_plus_qt_JECCorr'))
		h_2D_upar_pf_JECCorr_vs_qt = hist2root.convert(result['histo9'].sum('dataset','pv','upar_pf_plus_qt'))
		h_2D_upar_puppi_vs_qt = hist2root.convert(result['histo10'].sum('dataset','pv','upar_puppi_plus_qt_JECCorr'))
		h_2D_upar_puppi_JECCorr_vs_qt = hist2root.convert(result['histo10'].sum('dataset','pv','upar_puppi_plus_qt'))

		h_2D_upar_raw_vs_pv = hist2root.convert(result['histo8'].sum('dataset','qt'))
		h_2D_upar_pf_vs_pv = hist2root.convert(result['histo9'].sum('dataset','qt','upar_pf_plus_qt_JECCorr'))
		h_2D_upar_pf_JECCorr_vs_pv = hist2root.convert(result['histo9'].sum('dataset','qt','upar_pf_plus_qt'))
		h_2D_upar_puppi_vs_pv = hist2root.convert(result['histo10'].sum('dataset','qt','upar_puppi_plus_qt_JECCorr'))
		h_2D_upar_puppi_JECCorr_vs_pv = hist2root.convert(result['histo10'].sum('dataset','qt','upar_puppi_plus_qt'))

		h_2D_uperp_raw_vs_qt = hist2root.convert(result['histo11'].sum('dataset','pv'))
		h_2D_uperp_pf_vs_qt = hist2root.convert(result['histo12'].sum('dataset','pv','uperp_pf_JECCorr'))
		h_2D_uperp_pf_JECCorr_vs_qt = hist2root.convert(result['histo12'].sum('dataset','pv','uperp_pf'))
		h_2D_uperp_puppi_vs_qt = hist2root.convert(result['histo13'].sum('dataset','pv','uperp_puppi_JECCorr'))
		h_2D_uperp_puppi_JECCorr_vs_qt = hist2root.convert(result['histo13'].sum('dataset','pv','uperp_puppi'))

		h_2D_uperp_raw_vs_pv = hist2root.convert(result['histo11'].sum('dataset','qt'))
		h_2D_uperp_pf_vs_pv = hist2root.convert(result['histo12'].sum('dataset','qt','uperp_pf_JECCorr'))
		h_2D_uperp_pf_JECCorr_vs_pv = hist2root.convert(result['histo12'].sum('dataset','qt','uperp_pf'))
		h_2D_uperp_puppi_vs_pv = hist2root.convert(result['histo13'].sum('dataset','qt','uperp_puppi_JECCorr'))
		h_2D_uperp_puppi_JECCorr_vs_pv = hist2root.convert(result['histo13'].sum('dataset','qt','uperp_puppi'))

		# Write histos to file
		h_2D_response_raw_vs_qt.Write("response_raw_vs_qt")
		h_2D_response_pf_vs_qt.Write("response_pf_vs_qt")
		h_2D_response_pf_JECCorr_vs_qt.Write("response_pf_JECCorr_vs_qt")
		h_2D_response_puppi_vs_qt.Write("response_puppi_vs_qt")
		h_2D_response_puppi_JECCorr_vs_qt.Write("response_puppi_JECCorr_vs_qt")
		h_2D_response_raw_vs_pv.Write("response_raw_vs_pv")
		h_2D_response_pf_vs_pv.Write("response_pf_vs_pv")
		h_2D_response_pf_JECCorr_vs_pv.Write("response_pf_JECCorr_vs_pv")
		h_2D_response_puppi_vs_pv.Write("response_puppi_vs_pv")
		h_2D_response_puppi_JECCorr_vs_pv.Write("response_puppi_JECCorr_vs_pv")
		h_2D_upar_raw_vs_qt.Write("upar_raw_vs_qt")
		h_2D_upar_pf_vs_qt.Write("upar_pf_vs_qt")
		h_2D_upar_pf_JECCorr_vs_qt.Write("upar_pf_JECCorr_vs_qt")
		h_2D_upar_puppi_vs_qt.Write("upar_puppi_vs_qt")
		h_2D_upar_puppi_JECCorr_vs_qt.Write("upar_puppi_JECCorr_vs_qt")
		h_2D_upar_raw_vs_pv.Write("upar_raw_vs_pv")
		h_2D_upar_pf_vs_pv.Write("upar_pf_vs_pv")
		h_2D_upar_pf_JECCorr_vs_pv.Write("upar_pf_JECCorr_vs_pv")
		h_2D_upar_puppi_vs_pv.Write("upar_puppi_vs_pv")
		h_2D_upar_puppi_JECCorr_vs_pv.Write("upar_puppi_JECCorr_vs_pv")
		h_2D_uperp_raw_vs_qt.Write("uperp_raw_vs_qt")
		h_2D_uperp_pf_vs_qt.Write("uperp_pf_vs_qt")
		h_2D_uperp_pf_JECCorr_vs_qt.Write("uperp_pf_JECCorr_vs_qt")
		h_2D_uperp_puppi_vs_qt.Write("uperp_puppi_vs_qt")
		h_2D_uperp_puppi_JECCorr_vs_qt.Write("uperp_puppi_JECCorr_vs_qt")
		h_2D_uperp_raw_vs_pv.Write("uperp_raw_vs_pv")
		h_2D_uperp_pf_vs_pv.Write("uperp_pf_vs_pv")
		h_2D_uperp_pf_JECCorr_vs_pv.Write("uperp_pf_JECCorr_vs_pv")
		h_2D_uperp_puppi_vs_pv.Write("uperp_puppi_vs_pv")
		h_2D_uperp_puppi_JECCorr_vs_pv.Write("uperp_puppi_JECCorr_vs_pv")

		infile.Close()
