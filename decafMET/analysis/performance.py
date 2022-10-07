import os, sys

import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea import processor, hist, util
from coffea.nanoevents.methods import vector
from coffea.nanoevents.methods.vector import ThreeVector
from coffea.jetmet_tools import FactorizedJetCorrector, JetCorrectionUncertainty
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory
from coffea.lookup_tools import extractor

import uproot3
import hist2root
import ROOT
from ROOT import TFile

import array
import json
import numpy as np

from dask.distributed import Client
from dask_jobqueue.htcondor import HTCondorCluster

from processor import gammaJets, dyJets
import METCorrections

def correctedPuppiJets(events,jets):
	ext = extractor()
	ext.add_weight_sets([
		"* * data/JEC/Winter22Run3_RunC_V1_DATA_L1FastJet_AK4PFPuppi.txt",
		"* * data/JEC/Winter22Run3_RunC_V1_DATA_L2Relative_AK4PFPuppi.txt",
		"* * data/JEC/Winter22Run3_RunC_V1_DATA_L3Absolute_AK4PFPuppi.txt",
		"* * data/JEC/Winter22Run3_RunC_V1_DATA_L2L3Residual_AK4PFPuppi.txt"
	])

	ext.finalize()
	
	jec_stack_names = [
		"Winter22Run3_RunC_V1_DATA_L1FastJet_AK4PFPuppi",
		"Winter22Run3_RunC_V1_DATA_L2Relative_AK4PFPuppi",
		"Winter22Run3_RunC_V1_DATA_L3Absolute_AK4PFPuppi",
		"Winter22Run3_RunC_V1_DATA_L2L3Residual_AK4PFPuppi"
	]
	
	evaluator = ext.make_evaluator()
	
	jec_inputs = {name: evaluator[name] for name in jec_stack_names}
	jec_stack = JECStack(jec_inputs)

	name_map = jec_stack.blank_name_map
	name_map['JetPt'] = 'pt'
	name_map['JetMass'] = 'mass'
	name_map['JetEta'] = 'eta'
	name_map['JetA'] = 'area'
	
	jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
	jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
	#jets['pt_gen'] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
	jets['rho'] = ak.broadcast_arrays(events.Rho.fixedGridRhoFastjetAll, jets.pt)[0]
	#name_map['ptGenJet'] = 'pt_gen'
	name_map['ptRaw'] = 'pt_raw'
	name_map['massRaw'] = 'mass_raw'
	name_map['Rho'] = 'rho'
	
	events_cache = events.caches[0]
	corrector = FactorizedJetCorrector(
		Winter22Run3_RunC_V1_DATA_L1FastJet_AK4PFPuppi=evaluator['Winter22Run3_RunC_V1_DATA_L1FastJet_AK4PFPuppi'],
		Winter22Run3_RunC_V1_DATA_L2Relative_AK4PFPuppi=evaluator['Winter22Run3_RunC_V1_DATA_L2Relative_AK4PFPuppi'],
		Winter22Run3_RunC_V1_DATA_L3Absolute_AK4PFPuppi=evaluator['Winter22Run3_RunC_V1_DATA_L3Absolute_AK4PFPuppi'],
		Winter22Run3_RunC_V1_DATA_L2L3Residual_AK4PFPuppi=evaluator['Winter22Run3_RunC_V1_DATA_L2L3Residual_AK4PFPuppi']
	)
#	uncertainties = JetCorrectionUncertainty(
#	    Fall17_17Nov2017_V32_MC_Uncertainty_AK4PFPuppi=evaluator['Fall17_17Nov2017_V32_MC_Uncertainty_AK4PFPuppi']
#	)
	
	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)

	return corrected_jets

def correctedChsJets(events,jets):
	ext = extractor()
	ext.add_weight_sets([
		"* * data/JEC/Winter22Run3_RunC_V1_DATA_L1FastJet_AK4PFchs.txt",
		"* * data/JEC/Winter22Run3_RunC_V1_DATA_L2Relative_AK4PFchs.txt",
		"* * data/JEC/Winter22Run3_RunC_V1_DATA_L3Absolute_AK4PFchs.txt"
	])

	ext.finalize()
	
	jec_stack_names = [
		"Winter22Run3_RunC_V1_DATA_L1FastJet_AK4PFchs",
		"Winter22Run3_RunC_V1_DATA_L2Relative_AK4PFchs",
		"Winter22Run3_RunC_V1_DATA_L3Absolute_AK4PFchs"
	]
	
	evaluator = ext.make_evaluator()
	
	jec_inputs = {name: evaluator[name] for name in jec_stack_names}
	jec_stack = JECStack(jec_inputs)

	name_map = jec_stack.blank_name_map
	name_map['JetPt'] = 'pt'
	name_map['JetMass'] = 'mass'
	name_map['JetEta'] = 'eta'
	name_map['JetA'] = 'area'
	
	jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
	jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
	#jets['pt_gen'] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
	jets['rho'] = ak.broadcast_arrays(events.Rho.fixedGridRhoFastjetAll, jets.pt)[0]
	#name_map['ptGenJet'] = 'pt_gen'
	name_map['ptRaw'] = 'pt_raw'
	name_map['massRaw'] = 'mass_raw'
	name_map['Rho'] = 'rho'
	
	events_cache = events.caches[0]
	corrector = FactorizedJetCorrector(
		Winter22Run3_RunC_V1_DATA_L1FastJet_AK4PFchs=evaluator['Winter22Run3_RunC_V1_DATA_L1FastJet_AK4PFchs'],
		Winter22Run3_RunC_V1_DATA_L2Relative_AK4PFchs=evaluator['Winter22Run3_RunC_V1_DATA_L2Relative_AK4PFchs'],
		Winter22Run3_RunC_V1_DATA_L3Absolute_AK4PFchs=evaluator['Winter22Run3_RunC_V1_DATA_L3Absolute_AK4PFchs']
	)
#	uncertainties = JetCorrectionUncertainty(
#	    Fall17_17Nov2017_V32_MC_Uncertainty_AK4PFPuppi=evaluator['Fall17_17Nov2017_V32_MC_Uncertainty_AK4PFPuppi']
#	)
	
	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)

	return corrected_jets

class performanceProcessor(processor.ProcessorABC):
	def __init__(self):
		self._histo1 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("met_raw_phi", "Phi (Raw missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_pf_phi", "Phi (PF missing ET)", 40, -3.2, 3.2),
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
			hist.Bin("met_raw", "Raw missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_raw", "Response (RAW)", 60, -2, 4),
		)
		self._histo4 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_pf", "Response (PF)", 60, -2, 4),
		)
		self._histo5 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf_JECCorr", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_pf_JECCorr", "Response (PF)", 60, -2, 4),
		)
		self._histo6 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_puppi", "Response (PUPPI)", 60, -2, 4),
		)
		self._histo7 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi_JECCorr", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_puppi_JECCorr", "Response (PUPPI)", 60, -2, 4),
		)
		self._histo8 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
		)
		self._histo9 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_pf_plus_qt", "Parallel recoil (PF)", 100, -200, 200),
			hist.Bin("upar_pf_plus_qt_JECCorr", "Parallel recoil (PF)", 100, -200, 200),
		)
		self._histo10 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_puppi_plus_qt", "Parallel recoil (PUPPI)", 100, -200, 200),
			hist.Bin("upar_puppi_plus_qt_JECCorr", "Parallel recoil (PUPPI)", 100, -200, 200),
		)
		self._histo11 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_raw", "Perpendicular recoil (RAW)", 50, -200, 200),
		)
		self._histo12 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_pf", "Perpendicular recoil (PF)", 50, -200, 200),
			hist.Bin("uperp_pf_JECCorr", "Perpendicular recoil (PF)", 50, -200, 200),
		)
		self._histo13 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_puppi", "Perpendicular recoil (PUPPI)", 50, -200, 200),
			hist.Bin("uperp_puppi_JECCorr", "Perpendicular recoil (PUPPI)", 50, -200, 200),
		)
		self._histo14 = hist.Hist(
			"Events",
			hist.Bin("nEventsTot", "Total number of events", 1, 0, 1),
		)
		self._histo15 = hist.Hist(
			"Events",
			hist.Bin("nEventsLumi", "Number of events after json", 1, 0, 1),
		)
		self._histo16 = hist.Hist(
			"Events",
			hist.Bin("nEventsFilters", "Number of events after MET filters", 1, 0, 1),
		)
		self._histo17 = hist.Hist(
			"Events",
			hist.Bin("nEventsTriggers", "Number of events after triggers", 1, 0, 1),
		)
		self._histo18 = hist.Hist(
			"Events",
			hist.Bin("nEventsNPV", "Number of events after npv cut", 1, 0, 1),
		)
		self._histo19 = hist.Hist(
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
		met_raw = dyJets.dyJetsSelection(events)['met_raw']
		met_pf = dyJets.dyJetsSelection(events)['met_pf']
		met_puppi = dyJets.dyJetsSelection(events)['met_puppi']

		corrected_chsJets = correctedChsJets(events,events.JetCHS)
		corrected_puppiJets = correctedPuppiJets(events,events.Jet)

		vec_met_pf_JECCorr = ak.zip(
			{
				"x": met_pf.pt*np.cos(met_pf.phi) + events.JetCHS.pt*np.cos(events.JetCHS.phi) - corrected_chsJets.pt*np.cos(corrected_chsJets.phi),
				"y": met_pf.pt*np.sin(met_pf.phi) + events.JetCHS.pt*np.sin(events.JetCHS.phi) - corrected_chsJets.pt*np.sin(corrected_chsJets.phi),
			},
			with_name="TwoVector",
		)
		vec_met_puppi_JECCorr = ak.zip(
			{
				"x": met_puppi.pt*np.cos(met_puppi.phi) + events.Jet.pt*np.cos(events.Jet.phi) - corrected_puppiJets.pt*np.cos(corrected_puppiJets.phi),
				"y": met_puppi.pt*np.sin(met_puppi.phi) + events.Jet.pt*np.sin(events.Jet.phi) - corrected_puppiJets.pt*np.sin(corrected_puppiJets.phi),
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
		vec_u_raw = ak.zip(
			{
				"x": -met_raw.px - vec_boson.x,
				"y": -met_raw.py - vec_boson.y,
			},
			with_name="TwoVector",
		)
		upar_raw = (vec_u_raw.x*vec_boson_unit.x) + (vec_u_raw.y*vec_boson_unit.y)
		upar_raw_plus_qt = upar_raw + boson_pt
		upar_raw = -upar_raw
		response_raw = upar_raw/boson_pt
		uperp_raw =  (vec_u_raw.y*vec_boson_unit.x) - (vec_u_raw.x*vec_boson_unit.y)
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
			met_raw_phi=met_raw.phi,
			met_pf_phi=met_pf.phi,
			met_puppi_phi=met_puppi.phi,
			weight=weights,
		)
		out['histo2'].fill(
			dataset=events.metadata["dataset"],
			met_pf_phi_JECCorr=vec_met_pf_JECCorr.phi[:,0],
			met_puppi_phi_JECCorr=vec_met_puppi_JECCorr.phi[:,0],
			weight=weights,
		)
		out['histo3'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_raw=met_raw.pt,
			#response_raw=response_raw[:,0],
			response_raw=response_raw,
			weight=weights,
		)
		out['histo4'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_pf=met_pf.pt,
			response_pf=response_pf,
			weight=weights,
		)
		out['histo5'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_pf_JECCorr=vec_met_pf_JECCorr.pt[:,0],
			response_pf_JECCorr=response_pf_JECCorr[:,0],
			weight=weights,
		)
		out['histo6'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_puppi=met_puppi.pt,
			response_puppi=response_puppi,
			weight=weights,
		)
		out['histo7'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_puppi_JECCorr=vec_met_puppi_JECCorr.pt[:,0],
			response_puppi_JECCorr=response_puppi_JECCorr[:,0],
			#response_puppi=response_puppi[:,0],
			weight=weights,
		)
		out['histo8'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			upar_raw_plus_qt=upar_raw_plus_qt,
			#upar_raw_plus_qt=upar_raw_plus_qt[:,0],
			weight=weights,
		)
		out['histo9'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			upar_pf_plus_qt=upar_pf_plus_qt,
			upar_pf_plus_qt_JECCorr=upar_pf_plus_qt_JECCorr[:,0],
			#upar_pf_plus_qt=upar_pf_plus_qt[:,0],
			weight=weights,
		)
		out['histo10'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			upar_puppi_plus_qt=upar_puppi_plus_qt,
			upar_puppi_plus_qt_JECCorr=upar_puppi_plus_qt_JECCorr[:,0],
			#upar_puppi_plus_qt=upar_puppi_plus_qt[:,0],
			weight=weights,
		)
		out['histo11'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			uperp_raw=uperp_raw,
			#uperp_raw=uperp_raw[:,0],
			weight=weights,
		)
		out['histo12'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			uperp_pf=uperp_pf,
			uperp_pf_JECCorr=uperp_pf_JECCorr[:,0],
			#uperp_pf=uperp_pf[:,0],
			weight=weights,
		)
		out['histo13'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			uperp_puppi=uperp_puppi,
			uperp_puppi_JECCorr=uperp_puppi_JECCorr[:,0],
			#uperp_puppi=uperp_puppi[:,0],
			weight=weights,
		)
		out['histo14'].fill(
			nEventsTot=nEventsTot
		)
		out['histo15'].fill(
			nEventsLumi=nEventsLumi
		)
		out['histo16'].fill(
			nEventsFilters=nEventsFilters
		)
		out['histo17'].fill(
			nEventsTriggers=nEventsTriggers
		)
		out['histo18'].fill(
			nEventsNPV=nEventsNPV
		)
		out['histo19'].fill(
			nEventsMuon=nEventsMuon
		)
		
		return out
	
	def postprocess(self, accumulator):
		return accumulator
