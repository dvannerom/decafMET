import os, sys

import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea import processor, hist, util
from coffea.nanoevents.methods import vector
from coffea.nanoevents.methods.vector import ThreeVector
from coffea.jetmet_tools import FactorizedJetCorrector, JetCorrectionUncertainty
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory
from coffea.lookup_tools import extractor
from coffea.analysis_tools import PackedSelection

import uproot3
import hist2root
import ROOT
from ROOT import TFile

import array
import json
import numpy as np

from processor import gammaJets, dyJets
import METCorrections

import utilsJEC

class performanceProcessor(processor.ProcessorABC):
	def __init__(self):
		self._histo1 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("met_pf_raw_phi", "Phi (Raw PF missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_pf_phi", "Phi (PF missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_puppi_raw_phi", "Phi (Raw Puppi missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_puppi_phi", "Phi (Puppi missing ET)", 40, -3.2, 3.2),
		)
		self._histo2 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("met_pf_phi_JECCorr", "Phi (PF missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_puppi_phi_JECCorr", "Phi (Puppi missing ET)", 40, -3.2, 3.2),
		)
		self._histo3 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf_raw", "Raw missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_pf_raw", "Response (RAW)", 500, -50, 50),
			#hist.Bin("response_pf_raw", "Response (RAW)", 60, -2, 4),
			#hist.Bin("response_pf_raw", "Response (RAW)", np.array([-200,-100,-50,-40,-30,-25,-20,-15,-10,-9,-8,-7,-6,-5,-4,-3.5,-3,-2.9,-2.8,-2.7,-2.6,-2.5,-2.4,-2.3,-2.2,-2.1,-2,-1.9,-1.8,-1.7,-1.6,-1.5,-1.4,-1.3,-1.2,-1.1,-1,-0.9,-0.8,-0.7,-0.6,-0.5,-0.4,-0.3,-0.2,-0.1,0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1,1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8,1.9,2,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,3,3.5,4,5,6,7,8,9,10,15,20,25,30,40,50,100,200])),
		)
		self._histo4 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi_raw", "Raw missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			#hist.Bin("response_puppi_raw", "Response (RAW)", 60, -2, 4),
			hist.Bin("response_puppi_raw", "Response (RAW)", 500, -50, 50),
		)
		self._histo5 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			#hist.Bin("response_pf", "Response (PF)", 60, -2, 4),
			hist.Bin("response_pf", "Response (PF)", 500, -50, 50),
		)
		self._histo6 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf_JECCorr", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			#hist.Bin("response_pf_JECCorr", "Response (PF)", 60, -2, 4),
			hist.Bin("response_pf_JECCorr", "Response (PF)", 500, -50, 50),
		)
		self._histo7 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			#hist.Bin("response_puppi", "Response (PUPPI)", 60, -2, 4),
			hist.Bin("response_puppi", "Response (PUPPI)", 500, -50, 50),
		)
		self._histo8 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi_JECCorr", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			#hist.Bin("response_puppi_JECCorr", "Response (PUPPI)", 60, -2, 4),
			hist.Bin("response_puppi_JECCorr", "Response (PUPPI)", 500, -50, 50),
		)
		self._histo9 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_pf_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
			hist.Bin("upar_puppi_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
		)
		self._histo10 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_pf_plus_qt", "Parallel recoil (PF)", 100, -200, 200),
			hist.Bin("upar_pf_plus_qt_JECCorr", "Parallel recoil (PF)", 100, -200, 200),
		)
		self._histo11 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_puppi_plus_qt", "Parallel recoil (PUPPI)", 100, -200, 200),
			hist.Bin("upar_puppi_plus_qt_JECCorr", "Parallel recoil (PUPPI)", 100, -200, 200),
		)
		self._histo12 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_pf_raw", "Perpendicular recoil (RAW)", 50, -200, 200),
			hist.Bin("uperp_puppi_raw", "Perpendicular recoil (RAW)", 50, -200, 200),
		)
		self._histo13 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_pf", "Perpendicular recoil (PF)", 50, -200, 200),
			hist.Bin("uperp_pf_JECCorr", "Perpendicular recoil (PF)", 50, -200, 200),
		)
		self._histo14 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_puppi", "Perpendicular recoil (PUPPI)", 50, -200, 200),
			hist.Bin("uperp_puppi_JECCorr", "Perpendicular recoil (PUPPI)", 50, -200, 200),
		)
		self._histo15 = hist.Hist(
			"Events",
			hist.Bin("nEventsTot", "Total number of events", 1, 0, 1),
		)
		self._histo16 = hist.Hist(
			"Events",
			hist.Bin("nEventsLumi", "Number of events after json", 1, 0, 1),
		)
		self._histo17 = hist.Hist(
			"Events",
			hist.Bin("nEventsFilters", "Number of events after MET filters", 1, 0, 1),
		)
		self._histo18 = hist.Hist(
			"Events",
			hist.Bin("nEventsTriggers", "Number of events after triggers", 1, 0, 1),
		)
		self._histo19 = hist.Hist(
			"Events",
			hist.Bin("nEventsNPV", "Number of events after npv cut", 1, 0, 1),
		)
		self._histo20 = hist.Hist(
			"Events",
			hist.Bin("nEventsMuon", "Number of events after muon selection", 1, 0, 1),
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
			'histo13':self._histo13,
			'histo14':self._histo14,
			'histo15':self._histo15,
			'histo16':self._histo16,
			'histo17':self._histo17,
			'histo18':self._histo18,
			'histo19':self._histo19,
			'histo20':self._histo20,
		})

	@property
	def accumulator(self):
		#return self._histo
		return self._accumulator
	
	# we will receive a NanoEvents instead of a coffea DataFrame
	def process(self, events):
		out = self.accumulator.identity()

		nEventsTot = dyJets.dyJetsSelection(events)['nEvents_tot']
		nEventsLumi = dyJets.dyJetsSelection(events)['nEvents_lumi']
		nEventsFilters = dyJets.dyJetsSelection(events)['nEvents_filters']
		nEventsTriggers = dyJets.dyJetsSelection(events)['nEvents_trigger']
		nEventsNPV = dyJets.dyJetsSelection(events)['nEvents_npv']
		nEventsMuon = dyJets.dyJetsSelection(events)['nEvents_muon']
		nEventsTot = np.zeros(nEventsTot)
		nEventsLumi = np.zeros(nEventsLumi)
		nEventsFilters = np.zeros(nEventsFilters)
		nEventsTriggers = np.zeros(nEventsTriggers)
		nEventsNPV = np.zeros(nEventsNPV)
		nEventsMuon = np.zeros(nEventsMuon)
	
		#events = gammaJets.gammaJetsSelection(events)['events']
		#
		#boson = gammaJets.gammaJetsSelection(events)['boson']
		#pv = gammaJets.gammaJetsSelection(events)['pv']
		#met_raw = gammaJets.gammaJetsSelection(events)['met_raw']
		#met_pf = gammaJets.gammaJetsSelection(events)['met_pf']
		#met_puppi = gammaJets.gammaJetsSelection(events)['met_puppi']

		events = dyJets.dyJetsSelection(events)['events']
		
		boson = dyJets.dyJetsSelection(events)['boson']
		boson_pt = boson.pt # For DY
		#boson_pt = boson.pt[:,0] # For Gamma
		pv = dyJets.dyJetsSelection(events)['pv']
		met_pf_raw = dyJets.dyJetsSelection(events)['met_pf_raw']
		met_pf = dyJets.dyJetsSelection(events)['met_pf']
		met_puppi_raw = dyJets.dyJetsSelection(events)['met_puppi_raw']
		met_puppi = dyJets.dyJetsSelection(events)['met_puppi']

		chsJets = events.JetCHS[utilsJEC.isGoodJet(events.JetCHS)]
		puppiJets = events.Jet[utilsJEC.isGoodJet(events.Jet)]

		corrected_chsJets = utilsJEC.correctedMCChsJets(events,chsJets)
		corrected_puppiJets = utilsJEC.correctedMCPuppiJets(events,puppiJets)

		# Sum x and y components over all jets in the event
		raw_chsJets_px = chsJets['pt']*np.cos(chsJets['phi'])
		raw_chsJets_px = ak.sum(raw_chsJets_px,-1)
		raw_chsJets_py = chsJets['pt']*np.sin(chsJets['phi'])
		raw_chsJets_py = ak.sum(raw_chsJets_py,-1)
		raw_puppiJets_px = puppiJets['pt']*np.cos(puppiJets['phi'])
		raw_puppiJets_px = ak.sum(raw_puppiJets_px,-1)
		raw_puppiJets_py = puppiJets['pt']*np.sin(puppiJets['phi'])
		raw_puppiJets_py = ak.sum(raw_puppiJets_py,-1)
		corrected_chsJets_px = corrected_chsJets['pt']*np.cos(corrected_chsJets['phi'])
		corrected_chsJets_px = ak.sum(corrected_chsJets_px,-1)
		corrected_chsJets_py = corrected_chsJets['pt']*np.sin(corrected_chsJets['phi'])
		corrected_chsJets_py = ak.sum(corrected_chsJets_py,-1)
		corrected_puppiJets_px = corrected_puppiJets['pt']*np.cos(corrected_puppiJets['phi'])
		corrected_puppiJets_px = ak.sum(corrected_puppiJets_px,-1)
		corrected_puppiJets_py = corrected_puppiJets['pt']*np.sin(corrected_puppiJets['phi'])
		corrected_puppiJets_py = ak.sum(corrected_puppiJets_py,-1)

		# Define Type-I corrected METs
		vec_met_pf_JECCorr = ak.zip(
			{
				"x": met_pf.pt*np.cos(met_pf.phi) + raw_chsJets_px - corrected_chsJets_px,
				"y": met_pf.pt*np.sin(met_pf.phi) + raw_chsJets_py - corrected_chsJets_py,
			},
			with_name="TwoVector",
		)
		vec_met_puppi_JECCorr = ak.zip(
			{
				"x": met_puppi.pt*np.cos(met_puppi.phi) + raw_puppiJets_px - corrected_puppiJets_px,
				"y": met_puppi.pt*np.sin(met_puppi.phi) + raw_puppiJets_py - corrected_puppiJets_py
			},
			with_name="TwoVector",
		)
	
		# Implement MET XY corrections
		#print(type(METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv.npvs,events.run,False,"2016preVFP",True,ispuppi=False)),METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv.npvs,events.run,False,"2016preVFP",True,ispuppi=False))
		#met_pf.px = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,0]
		#met_pf.py = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,1]
		#met_pf.pt = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,2]
		#met_pf.phi = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,3]
		#met_puppi = METCorrections.correctedMET(met_puppi.pt,met_puppi.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)
	
		# Compute hadronic recoil and components
		vec_boson = ak.zip(
			{
				"x": boson.px,
				"y": boson.py,
			},
			with_name="TwoVector",
		)
		vec_boson_unit = vec_boson.unit
	
		# Raw quantities
		# PF
		vec_u_pf_raw = ak.zip(
			{
				"x": -met_pf_raw.px - vec_boson.x,
				"y": -met_pf_raw.py - vec_boson.y,
			},
			with_name="TwoVector",
		)
		upar_pf_raw = (vec_u_pf_raw.x*vec_boson_unit.x) + (vec_u_pf_raw.y*vec_boson_unit.y)
		upar_pf_raw_plus_qt = upar_pf_raw + boson_pt
		upar_pf_raw = -upar_pf_raw
		response_pf_raw = upar_pf_raw/boson_pt
		uperp_pf_raw =  (vec_u_pf_raw.y*vec_boson_unit.x) - (vec_u_pf_raw.x*vec_boson_unit.y)
		# Puppi
		vec_u_puppi_raw = ak.zip(
			{
				"x": -met_puppi_raw.px - vec_boson.x,
				"y": -met_puppi_raw.py - vec_boson.y,
			},
			with_name="TwoVector",
		)
		upar_puppi_raw = (vec_u_puppi_raw.x*vec_boson_unit.x) + (vec_u_puppi_raw.y*vec_boson_unit.y)
		upar_puppi_raw_plus_qt = upar_puppi_raw + boson_pt
		upar_puppi_raw = -upar_puppi_raw
		response_puppi_raw = upar_puppi_raw/boson_pt
		uperp_puppi_raw =  (vec_u_puppi_raw.y*vec_boson_unit.x) - (vec_u_puppi_raw.x*vec_boson_unit.y)
		# PF quantities
		# Uncorrected
		vec_u_pf = ak.zip(
			{
				"x": -met_pf.px - vec_boson.x,
				"y": -met_pf.py - vec_boson.y,
			},
			with_name="TwoVector",
		)
		upar_pf = (vec_u_pf.x*vec_boson_unit.x) + (vec_u_pf.y*vec_boson_unit.y)
		upar_pf_plus_qt = upar_pf + boson_pt
		upar_pf = -upar_pf
		response_pf = upar_pf/boson_pt
		uperp_pf = (vec_u_pf.y*vec_boson_unit.x) - (vec_u_pf.x*vec_boson_unit.y)
		# JEC corrected
		vec_u_pf_JECCorr = ak.zip(
			{
				"x": -vec_met_pf_JECCorr.x - vec_boson.x,
				"y": -vec_met_pf_JECCorr.y - vec_boson.y,
			},
			with_name="TwoVector",
		)
		upar_pf_JECCorr = (vec_u_pf_JECCorr.x*vec_boson_unit.x) + (vec_u_pf_JECCorr.y*vec_boson_unit.y)
		upar_pf_plus_qt_JECCorr = upar_pf_JECCorr + boson_pt
		upar_pf_JECCorr = -upar_pf_JECCorr
		response_pf_JECCorr = upar_pf_JECCorr/boson_pt
		uperp_pf_JECCorr = (vec_u_pf_JECCorr.y*vec_boson_unit.x) - (vec_u_pf_JECCorr.x*vec_boson_unit.y)
		# PUPPI quantities
		# Uncorrected
		vec_u_puppi = ak.zip(
			{
				"x": -met_puppi.px - vec_boson.x,
				"y": -met_puppi.py - vec_boson.y,
			},
			with_name="TwoVector",
		)
		upar_puppi = (vec_u_puppi.x*vec_boson_unit.x) + (vec_u_puppi.y*vec_boson_unit.y)
		upar_puppi_plus_qt = upar_puppi + boson_pt
		upar_puppi = -upar_puppi
		response_puppi = upar_puppi/boson_pt
		uperp_puppi = (vec_u_puppi.y*vec_boson_unit.x) - (vec_u_puppi.x*vec_boson_unit.y)
		# JEC corrected
		vec_u_puppi_JECCorr = ak.zip(
			{
				"x": -vec_met_puppi_JECCorr.x - vec_boson.x,
				"y": -vec_met_puppi_JECCorr.y - vec_boson.y,
			},
			with_name="TwoVector",
		)
		upar_puppi_JECCorr = (vec_u_puppi_JECCorr.x*vec_boson_unit.x) + (vec_u_puppi_JECCorr.y*vec_boson_unit.y)
		upar_puppi_plus_qt_JECCorr = upar_puppi_JECCorr + boson_pt
		upar_puppi_JECCorr = -upar_puppi_JECCorr
		response_puppi_JECCorr = upar_puppi_JECCorr/boson_pt
		uperp_puppi_JECCorr = (vec_u_puppi_JECCorr.y*vec_boson_unit.x) - (vec_u_puppi_JECCorr.x*vec_boson_unit.y)
		
		#weights = gammaJets.gammaJetsSelection(events)['weights']
		weights = dyJets.dyJetsSelection(events)['weights']
		
		out['histo1'].fill(
			dataset=events.metadata["dataset"],
			met_pf_raw_phi=met_pf_raw.phi,
			met_pf_phi=met_pf.phi,
			met_puppi_phi=met_puppi.phi,
			met_puppi_raw_phi=met_puppi_raw.phi,
			weight=weights,
		)
		out['histo2'].fill(
			dataset=events.metadata["dataset"],
			#met_pf_phi_JECCorr=vec_met_pf_JECCorr.phi[:,0],
			#met_puppi_phi_JECCorr=vec_met_puppi_JECCorr.phi[:,0],
			met_pf_phi_JECCorr=vec_met_pf_JECCorr.phi,
			met_puppi_phi_JECCorr=vec_met_puppi_JECCorr.phi,
			weight=weights,
		)
		out['histo3'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_pf_raw=met_pf_raw.pt,
			#response_raw=response_raw[:,0],
			response_pf_raw=response_pf_raw,
			weight=weights,
		)
		out['histo4'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_puppi_raw=met_puppi_raw.pt,
			response_puppi_raw=response_puppi_raw,
			weight=weights,
		)
		out['histo5'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_pf=met_pf.pt,
			response_pf=response_pf,
			weight=weights,
		)
		out['histo6'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#met_pf_JECCorr=vec_met_pf_JECCorr.pt[:,0],
			#response_pf_JECCorr=response_pf_JECCorr[:,0],
			met_pf_JECCorr=vec_met_pf_JECCorr.pt,
			response_pf_JECCorr=response_pf_JECCorr,
			weight=weights,
		)
		out['histo7'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_puppi=met_puppi.pt,
			response_puppi=response_puppi,
			weight=weights,
		)
		out['histo8'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#met_puppi_JECCorr=vec_met_puppi_JECCorr.pt[:,0],
			#response_puppi_JECCorr=response_puppi_JECCorr[:,0],
			met_puppi_JECCorr=vec_met_puppi_JECCorr.pt,
			response_puppi_JECCorr=response_puppi_JECCorr,
			#response_puppi=response_puppi[:,0],
			weight=weights,
		)
		out['histo9'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			upar_pf_raw_plus_qt=upar_pf_raw_plus_qt,
			#upar_raw_plus_qt=upar_raw_plus_qt[:,0],
			upar_puppi_raw_plus_qt=upar_puppi_raw_plus_qt,
			weight=weights,
		)
		out['histo10'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			upar_pf_plus_qt=upar_pf_plus_qt,
			#upar_pf_plus_qt_JECCorr=upar_pf_plus_qt_JECCorr[:,0],
			upar_pf_plus_qt_JECCorr=upar_pf_plus_qt_JECCorr,
			#upar_pf_plus_qt=upar_pf_plus_qt[:,0],
			weight=weights,
		)
		out['histo11'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			upar_puppi_plus_qt=upar_puppi_plus_qt,
			#upar_puppi_plus_qt_JECCorr=upar_puppi_plus_qt_JECCorr[:,0],
			upar_puppi_plus_qt_JECCorr=upar_puppi_plus_qt_JECCorr,
			#upar_puppi_plus_qt=upar_puppi_plus_qt[:,0],
			weight=weights,
		)
		out['histo12'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			uperp_pf_raw=uperp_pf_raw,
			#uperp_raw=uperp_raw[:,0],
			uperp_puppi_raw=uperp_puppi_raw,
			weight=weights,
		)
		out['histo13'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			uperp_pf=uperp_pf,
			#uperp_pf_JECCorr=uperp_pf_JECCorr[:,0],
			uperp_pf_JECCorr=uperp_pf_JECCorr,
			#uperp_pf=uperp_pf[:,0],
			weight=weights,
		)
		out['histo14'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			uperp_puppi=uperp_puppi,
			#uperp_puppi_JECCorr=uperp_puppi_JECCorr[:,0],
			uperp_puppi_JECCorr=uperp_puppi_JECCorr,
			#uperp_puppi=uperp_puppi[:,0],
			weight=weights,
		)
		out['histo15'].fill(
			nEventsTot=nEventsTot
		)
		out['histo16'].fill(
			nEventsLumi=nEventsLumi
		)
		out['histo17'].fill(
			nEventsFilters=nEventsFilters
		)
		out['histo18'].fill(
			nEventsTriggers=nEventsTriggers
		)
		out['histo19'].fill(
			nEventsNPV=nEventsNPV
		)
		out['histo20'].fill(
			nEventsMuon=nEventsMuon
		)
		
		return out
	
	def postprocess(self, accumulator):
		return accumulator
