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

from dask.distributed import Client
from dask_jobqueue.htcondor import HTCondorCluster

import gammaJets

class performanceProcessor(processor.ProcessorABC):
    def __init__(self):
        self._histo1 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("met_raw", "Raw missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
            hist.Bin("met_pf", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
            hist.Bin("met_puppi", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
        )
        self._histo2 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("met_raw_phi", "Phi (Raw missing ET)", 40, -3.2, 3.2),
            hist.Bin("met_pf_phi", "Phi (PF missing ET)", 40, -3.2, 3.2),
            hist.Bin("met_puppi_phi", "Phi (Puppi missing ET)", 40, -3.2, 3.2),
        )
        self._histo3 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
            hist.Bin("neg_upar_raw", "Negative parallel recoil (RAW)", 120, -200, 1000),
            hist.Bin("upar_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
        )
        self._histo4 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
            hist.Bin("neg_upar_pf", "Negative parallel recoil (PF)", 120, -200, 1000),
            hist.Bin("upar_pf_plus_qt", "Parallel recoil (PF)", 100, -200, 200),
        )
        self._histo5 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
            hist.Bin("neg_upar_puppi", "Negative parallel recoil (PUPPI)", 120, -200, 1000),
            hist.Bin("upar_puppi_plus_qt", "Parallel recoil (PUPPI)", 100, -200, 200),
        )
        self._histo6 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
            hist.Bin("neg_upar_raw", "Negative parallel recoil (RAW)", 120, -200, 1000),
            hist.Bin("upar_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
        )
        self._histo7 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
            hist.Bin("neg_upar_pf", "Negative parallel recoil (PF)", 120, -200, 1000),
            hist.Bin("upar_pf_plus_qt", "Parallel recoil (PF)", 100, -200, 200),
        )
        self._histo8 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
            hist.Bin("neg_upar_puppi", "Negative parallel recoil (PUPPI)", 120, -200, 1000),
            hist.Bin("upar_puppi_plus_qt", "Parallel recoil (PUPPI)", 100, -200, 200),
        )
        self._histo9 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
            hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
            hist.Bin("uperp_raw", "Perpendicular recoil (RAW)", 50, -200, 200),
        )
        self._histo10 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
            hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
            hist.Bin("uperp_pf", "Perpendicular recoil (PF)", 50, -200, 200),
        )
        self._histo11 = hist.Hist(
            "Events",
            hist.Cat("dataset", "Dataset"),
            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
            hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
            hist.Bin("uperp_puppi", "Perpendicular recoil (PUPPI)", 50, -200, 200),
        )
        self._histo12 = hist.Hist(
            "Events",
            hist.Bin("nEvents", "Total number of events", 1, 0, 1),
        )

        self._accumulator = processor.dict_accumulator({
            'histo1':self._histo1,
            'histo2':self._histo2,
            'histo3':self._histo3,
            'histo4':self._histo4,
            'histo5':self._histo5,
            'histo6':self._histo6,
            'histo7':self._histo7,
            'histo8':self._histo8,
            'histo9':self._histo9,
            'histo10':self._histo10,
            'histo11':self._histo11,
            'histo12':self._histo12,
        })

    @property
    def accumulator(self):
        #return self._histo
        return self._accumulator

    # we will receive a NanoEvents instead of a coffea DataFrame
    def process(self, events):
        out = self.accumulator.identity()

        nEvents = np.zeros(len(events))

        events = gammaJets.gammaJetsSelection(events)['events']

        photon = gammaJets.gammaJetsSelection(events)['boson']
        pv = gammaJets.gammaJetsSelection(events)['pv']
        met_raw = gammaJets.gammaJetsSelection(events)['met_raw']
        met_pf = gammaJets.gammaJetsSelection(events)['met_pf']
        met_puppi = gammaJets.gammaJetsSelection(events)['met_puppi']

        # Compute hadronic recoil and components
        vec_photon = ak.zip(
            {
                "x": photon.px,
                "y": photon.py,
            },
            with_name="TwoVector",
        )
        vec_photon_unit = vec_photon.unit

        # Raw quantities
        vec_u_raw = ak.zip(
            {
                "x": -met_raw.pt*np.cos(met_raw.phi) - vec_photon.px,
                "y": -met_raw.pt*np.sin(met_raw.phi) - vec_photon.py,
            },
            with_name="TwoVector",
        )
        upar_raw = (vec_u_raw.px*vec_photon_unit.px) + (vec_u_raw.py*vec_photon_unit.py)
        upar_raw_plus_qt = upar_raw + vec_photon.pt
        upar_raw = -upar_raw
        uperp_raw = (vec_u_raw.px*vec_photon_unit.py) - (vec_u_raw.py*vec_photon_unit.px)
        # PF quantities
        vec_u_pf = ak.zip(
            {
                "x": -met_pf.pt*np.cos(met_pf.phi) - vec_photon.px,
                "y": -met_pf.pt*np.sin(met_pf.phi) - vec_photon.py,
            },
            with_name="TwoVector",
        )
        upar_pf = (vec_u_pf.px*vec_photon_unit.px) + (vec_u_pf.py*vec_photon_unit.py)
        upar_pf_plus_qt = upar_pf + vec_photon.pt
        upar_pf = -upar_pf
        uperp_pf = (vec_u_pf.px*vec_photon_unit.py) - (vec_u_pf.py*vec_photon_unit.px)
        # PUPPI quantities
        vec_u_puppi = ak.zip(
            {
                "x": -met_puppi.pt*np.cos(met_puppi.phi) - vec_photon.px,
                "y": -met_puppi.pt*np.sin(met_puppi.phi) - vec_photon.py,
            },
            with_name="TwoVector",
        )
        upar_puppi = (vec_u_puppi.px*vec_photon_unit.px) + (vec_u_puppi.py*vec_photon_unit.py)
        upar_puppi_plus_qt = upar_puppi + vec_photon.pt
        upar_puppi = -upar_puppi
        uperp_puppi = (vec_u_puppi.px*vec_photon_unit.py) - (vec_u_puppi.py*vec_photon_unit.px)

        weights = gammaJets.gammaJetsSelection(events)['weights']

        out['histo1'].fill(
            dataset=events.metadata["dataset"],
            met_raw=met_raw.pt,
            met_pf=met_pf.pt,
            met_puppi=met_puppi.pt,
            weight=weights,
        )
        out['histo2'].fill(
            dataset=events.metadata["dataset"],
            met_raw_phi=met_raw.phi,
            met_pf_phi=met_pf.phi,
            met_puppi_phi=met_puppi.phi,
            weight=weights,
        )
        out['histo3'].fill(
            dataset=events.metadata["dataset"],
            qt=photon.pt[:,0],
            neg_upar_raw=upar_raw[:,0],
            upar_raw_plus_qt=upar_raw_plus_qt[:,0],
            weight=weights,
        )
        out['histo4'].fill(
            dataset=events.metadata["dataset"],
            qt=photon.pt[:,0],
            neg_upar_pf=upar_pf[:,0],
            upar_pf_plus_qt=upar_pf_plus_qt[:,0],
            weight=weights,
        )
        out['histo5'].fill(
            dataset=events.metadata["dataset"],
            qt=photon.pt[:,0],
            neg_upar_puppi=upar_puppi[:,0],
            upar_puppi_plus_qt=upar_puppi_plus_qt[:,0],
            weight=weights,
        )
        out['histo6'].fill(
            dataset=events.metadata["dataset"],
            pv=pv.npvs,
            neg_upar_raw=upar_raw[:,0],
            upar_raw_plus_qt=upar_raw_plus_qt[:,0],
            weight=weights,
        )
        out['histo7'].fill(
            dataset=events.metadata["dataset"],
            pv=pv.npvs,
            neg_upar_pf=upar_pf[:,0],
            upar_pf_plus_qt=upar_pf_plus_qt[:,0],
            weight=weights,
        )
        out['histo8'].fill(
            dataset=events.metadata["dataset"],
            pv=pv.npvs,
            neg_upar_puppi=upar_puppi[:,0],
            upar_puppi_plus_qt=upar_puppi_plus_qt[:,0],
            weight=weights,
        )
        out['histo9'].fill(
            dataset=events.metadata["dataset"],
            qt=photon.pt[:,0],
            pv=pv.npvs,
            uperp_raw=uperp_raw[:,0],
            weight=weights,
        )
        out['histo10'].fill(
            dataset=events.metadata["dataset"],
            qt=photon.pt[:,0],
            pv=pv.npvs,
            uperp_pf=uperp_pf[:,0],
            weight=weights,
        )
        out['histo11'].fill(
            dataset=events.metadata["dataset"],
            qt=photon.pt[:,0],
            pv=pv.npvs,
            uperp_puppi=uperp_puppi[:,0],
            weight=weights,
        )
        out['histo12'].fill(
            nEvents=nEvents
        )

        return out

    def postprocess(self, accumulator):
        return accumulator

def read_json_file(filename):
    with open(filename, 'r') as f:
        cache = f.read()
        data = eval(cache)
    return data


# Define input arguments
parser = argparse.ArgumentParser(description='')
parser.add_argument('-f', '--inFile', type=str, default='', help='Input json file')
args = parser.parse_args()

inFile = args.inFile

with open(inFile) as json_file:
	data = json.load(json_file)
	for a in data.keys():
		print(a)
		fileset = {a: data[a]}
		exe_args = {
			"skipbadfiles": True,
			"schema": NanoAODSchema,
			"workers": 10,
		}
		result = processor.run_uproot_job(
			fileset,
			"Events",
			performanceProcessor(),
			executor=processor.futures_executor,
			#executor=processor.iterative_executor,
			executor_args=exe_args
		)
		util.save('results/'+result,a+'.coffea')
		output_root_file = 'results/'+a+'.root'
		
		outputfile = uproot3.recreate(output_root_file)
		for var in result.keys():
			if var == 'histo1':
				outputfile['met_raw'] = hist.export1d(result[var].sum('dataset','met_pf','met_puppi'))
				outputfile['met_pf'] = hist.export1d(result[var].sum('dataset','met_raw','met_puppi'))
				outputfile['met_puppi'] = hist.export1d(result[var].sum('dataset','met_raw','met_pf'))
			elif var == 'histo2':
				outputfile['met_raw_phi'] = hist.export1d(result[var].sum('dataset','met_pf_phi','met_puppi_phi'))
				outputfile['met_pf_phi'] = hist.export1d(result[var].sum('dataset','met_raw_phi','met_puppi_phi'))
				outputfile['met_puppi_phi'] = hist.export1d(result[var].sum('dataset','met_raw_phi','met_pf_phi'))
			elif var == 'histo3':
				outputfile['qt'] = hist.export1d(result[var].sum('dataset','neg_upar_raw','upar_raw_plus_qt'))
				outputfile['neg_upar_raw'] = hist.export1d(result[var].sum('dataset','qt','upar_raw_plus_qt'))
				outputfile['upar_raw_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','neg_upar_raw'))
			elif var == 'histo4':
				outputfile['neg_upar_pf'] = hist.export1d(result[var].sum('dataset','qt','upar_pf_plus_qt'))
				outputfile['upar_pf_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','neg_upar_pf'))
			elif var == 'histo5':
				outputfile['neg_upar_puppi'] = hist.export1d(result[var].sum('dataset','qt','upar_puppi_plus_qt'))
				outputfile['upar_puppi_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','neg_upar_puppi'))
			elif var == 'histo9':
				outputfile['uperp_raw'] = hist.export1d(result[var].sum('dataset','qt','pv'))
			elif var == 'histo10':
				outputfile['uperp_pf'] = hist.export1d(result[var].sum('dataset','qt','pv'))
			elif var == 'histo11':
				outputfile['uperp_puppi'] = hist.export1d(result[var].sum('dataset','qt','pv'))
			elif var == 'histo12':
				outputfile['nEvents'] = hist.export1d(result[var])
		outputfile.close()
		
		infile = TFile.Open(output_root_file,"update")
		infile.cd()
		# Define 2D histos
		h_2D_neg_upar_raw_vs_qt = hist2root.convert(result['histo3'].sum('dataset','upar_raw_plus_qt'))
		h_2D_neg_upar_pf_vs_qt = hist2root.convert(result['histo4'].sum('dataset','upar_pf_plus_qt'))
		h_2D_neg_upar_puppi_vs_qt = hist2root.convert(result['histo5'].sum('dataset','upar_puppi_plus_qt'))
		h_2D_upar_raw_vs_qt = hist2root.convert(result['histo3'].sum('dataset','neg_upar_raw'))
		h_2D_upar_pf_vs_qt = hist2root.convert(result['histo4'].sum('dataset','neg_upar_pf'))
		h_2D_upar_puppi_vs_qt = hist2root.convert(result['histo5'].sum('dataset','neg_upar_puppi'))
		h_2D_upar_raw_vs_pv = hist2root.convert(result['histo6'].sum('dataset','neg_upar_raw'))
		h_2D_upar_pf_vs_pv = hist2root.convert(result['histo7'].sum('dataset','neg_upar_pf'))
		h_2D_upar_puppi_vs_pv = hist2root.convert(result['histo8'].sum('dataset','neg_upar_puppi'))
		h_2D_uperp_raw_vs_qt = hist2root.convert(result['histo9'].sum('dataset','pv'))
		h_2D_uperp_pf_vs_qt = hist2root.convert(result['histo10'].sum('dataset','pv'))
		h_2D_uperp_puppi_vs_qt = hist2root.convert(result['histo11'].sum('dataset','pv'))
		h_2D_uperp_raw_vs_pv = hist2root.convert(result['histo9'].sum('dataset','qt'))
		h_2D_uperp_pf_vs_pv = hist2root.convert(result['histo10'].sum('dataset','qt'))
		h_2D_uperp_puppi_vs_pv = hist2root.convert(result['histo11'].sum('dataset','qt'))
		# Write histos to file
		h_2D_neg_upar_raw_vs_qt.Write("neg_upar_raw_vs_qt")
		h_2D_neg_upar_pf_vs_qt.Write("neg_upar_pf_vs_qt")
		h_2D_neg_upar_puppi_vs_qt.Write("neg_upar_puppi_vs_qt")
		h_2D_upar_raw_vs_qt.Write("upar_raw_vs_qt")
		h_2D_upar_pf_vs_qt.Write("upar_pf_vs_qt")
		h_2D_upar_puppi_vs_qt.Write("upar_puppi_vs_qt")
		h_2D_upar_raw_vs_pv.Write("upar_raw_vs_pv")
		h_2D_upar_pf_vs_pv.Write("upar_pf_vs_pv")
		h_2D_upar_puppi_vs_pv.Write("upar_puppi_vs_pv")
		h_2D_uperp_raw_vs_qt.Write("uperp_raw_vs_qt")
		h_2D_uperp_pf_vs_qt.Write("uperp_pf_vs_qt")
		h_2D_uperp_puppi_vs_qt.Write("uperp_puppi_vs_qt")
		h_2D_uperp_raw_vs_pv.Write("uperp_raw_vs_pv")
		h_2D_uperp_pf_vs_pv.Write("uperp_pf_vs_pv")
		h_2D_uperp_puppi_vs_pv.Write("uperp_puppi_vs_pv")
		infile.Close()
