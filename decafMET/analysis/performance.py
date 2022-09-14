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

from dask.distributed import Client
from dask_jobqueue.htcondor import HTCondorCluster

from processor import gammaJets
import METCorrections

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
#        self._histo3 = hist.Hist(
#            "Events",
#            hist.Cat("dataset", "Dataset"),
#            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
#            hist.Bin("neg_upar_raw", "Negative parallel recoil (RAW)", 120, -200, 1000),
#            hist.Bin("upar_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
#        )
#        self._histo4 = hist.Hist(
#            "Events",
#            hist.Cat("dataset", "Dataset"),
#            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
#            hist.Bin("neg_upar_pf", "Negative parallel recoil (PF)", 120, -200, 1000),
#            hist.Bin("upar_pf_plus_qt", "Parallel recoil (PF)", 100, -200, 200),
#        )
#        self._histo5 = hist.Hist(
#            "Events",
#            hist.Cat("dataset", "Dataset"),
#            hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
#            hist.Bin("neg_upar_puppi", "Negative parallel recoil (PUPPI)", 120, -200, 1000),
#            hist.Bin("upar_puppi_plus_qt", "Parallel recoil (PUPPI)", 100, -200, 200),
#        )
		self._histo3 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("response_raw", "Response (RAW)", 60, -2, 4),
			hist.Bin("upar_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
		)
		self._histo4 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("response_pf", "Response (PF)", 60, -2, 4),
			hist.Bin("upar_pf_plus_qt", "Parallel recoil (PF)", 100, -200, 200),
		)
		self._histo5 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("response_puppi", "Response (PUPPI)", 60, -2, 4),
			hist.Bin("upar_puppi_plus_qt", "Parallel recoil (PUPPI)", 100, -200, 200),
		)
		self._histo6 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
			#hist.Bin("neg_upar_raw", "Negative parallel recoil (RAW)", 120, -200, 1000),
			hist.Bin("response_raw", "Response (RAW)", 120, -200, 1000),
			hist.Bin("upar_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
		)
		self._histo7 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
			#hist.Bin("neg_upar_pf", "Negative parallel recoil (PF)", 120, -200, 1000),
			hist.Bin("response_pf", "Response (PF)", 120, -200, 1000),
			hist.Bin("upar_pf_plus_qt", "Parallel recoil (PF)", 100, -200, 200),
		)
		self._histo8 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("pv", "Number of reconstructed PV", 15, 0, 60),
			#hist.Bin("neg_upar_puppi", "Negative parallel recoil (PUPPI)", 120, -200, 1000),
			hist.Bin("response_puppi", "Response (PUPPI)", 120, -200, 1000),
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
	
		# Implement MET XY corrections
		#print(type(METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv.npvs,events.run,False,"2016preVFP",True,ispuppi=False)),METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv.npvs,events.run,False,"2016preVFP",True,ispuppi=False))
		#met_pf.px = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,0]
		#met_pf.py = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,1]
		#met_pf.pt = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,2]
		#met_pf.phi = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,3]
		#met_puppi = METCorrections.correctedMET(met_puppi.pt,met_puppi.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)
	
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
				#"x": -met_raw.pt*np.cos(met_raw.phi) - vec_photon.px,
				#"y": -met_raw.pt*np.sin(met_raw.phi) - vec_photon.py,
				"x": -met_raw.px - vec_photon.x,
				"y": -met_raw.py - vec_photon.y,
			},
			with_name="TwoVector",
		)
		upar_raw = (vec_u_raw.x*vec_photon_unit.x) + (vec_u_raw.y*vec_photon_unit.y)
		upar_raw_plus_qt = upar_raw + photon.pt
		upar_raw = -upar_raw
		response_raw = upar_raw/photon.pt
		uperp_raw =  (vec_u_raw.y*vec_photon_unit.x) - (vec_u_raw.x*vec_photon_unit.y)
		# PF quantities
		vec_u_pf = ak.zip(
			{
				#"x": -met_pf.pt*np.cos(met_pf.phi) - vec_photon.px,
				#"y": -met_pf.pt*np.sin(met_pf.phi) - vec_photon.py,
				"x": -met_pf.px - vec_photon.x,
				"y": -met_pf.py - vec_photon.y,
			},
			with_name="TwoVector",
		)
		upar_pf = (vec_u_pf.x*vec_photon_unit.x) + (vec_u_pf.y*vec_photon_unit.y)
		upar_pf_plus_qt = upar_pf + photon.pt
		upar_pf = -upar_pf
		response_pf = upar_pf/photon.pt
		uperp_pf = (vec_u_pf.y*vec_photon_unit.x) - (vec_u_pf.x*vec_photon_unit.y)
		# PUPPI quantities
		vec_u_puppi = ak.zip(
			{
				#"x": -met_puppi.pt*np.cos(met_puppi.phi) - vec_photon.px,
				#"y": -met_puppi.pt*np.sin(met_puppi.phi) - vec_photon.py,
				"x": -met_puppi.px - vec_photon.x,
				"y": -met_puppi.py - vec_photon.y,
			},
			with_name="TwoVector",
		)
		upar_puppi = (vec_u_puppi.x*vec_photon_unit.x) + (vec_u_puppi.y*vec_photon_unit.y)
		upar_puppi_plus_qt = upar_puppi + photon.pt
		upar_puppi = -upar_puppi
		response_puppi = upar_puppi/photon.pt
		uperp_puppi = (vec_u_puppi.y*vec_photon_unit.x) - (vec_u_puppi.x*vec_photon_unit.y)
		
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
			#neg_upar_raw=upar_raw[:,0],
			response_raw=response_raw[:,0],
			upar_raw_plus_qt=upar_raw_plus_qt[:,0],
			weight=weights,
		)
		out['histo4'].fill(
			dataset=events.metadata["dataset"],
			qt=photon.pt[:,0],
			#neg_upar_pf=upar_pf[:,0],
			response_pf=response_pf[:,0],
			upar_pf_plus_qt=upar_pf_plus_qt[:,0],
			weight=weights,
		)
		out['histo5'].fill(
			dataset=events.metadata["dataset"],
			qt=photon.pt[:,0],
			#neg_upar_puppi=upar_puppi[:,0],
			response_puppi=response_puppi[:,0],
			upar_puppi_plus_qt=upar_puppi_plus_qt[:,0],
			weight=weights,
		)
		out['histo6'].fill(
			dataset=events.metadata["dataset"],
			pv=pv.npvs,
			#neg_upar_raw=upar_raw[:,0],
			response_raw=response_raw[:,0],
			upar_raw_plus_qt=upar_raw_plus_qt[:,0],
			weight=weights,
		)
		out['histo7'].fill(
			dataset=events.metadata["dataset"],
			pv=pv.npvs,
			#neg_upar_pf=upar_pf[:,0],
			response_pf=response_pf[:,0],
			upar_pf_plus_qt=upar_pf_plus_qt[:,0],
			weight=weights,
		)
		out['histo8'].fill(
			dataset=events.metadata["dataset"],
			pv=pv.npvs,
			#neg_upar_puppi=upar_puppi[:,0],
			response_puppi=response_puppi[:,0],
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
