import os, sys

import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea import processor, hist, util
from coffea.nanoevents.methods import vector
from coffea.nanoevents.methods.vector import ThreeVector

import array
import json
import numpy as np

def METCleaning(events):
    return (events.Flag.goodVertices &
            events.Flag.globalSuperTightHalo2016Filter &
            events.Flag.HBHENoiseFilter &
            events.Flag.HBHENoiseIsoFilter &
            events.Flag.BadPFMuonFilter &
            events.Flag.ecalBadCalibFilter &
            events.Flag.EcalDeadCellTriggerPrimitiveFilter &
            events.Flag.BadPFMuonDzFilter &
            events.Flag.eeBadScFilter
    )

def HLT_SinglePhoton(events):
    return (events.HLT.Photon50_R9Id90_HE10_IsoM |
            events.HLT.Photon75_R9Id90_HE10_IsoM |
            events.HLT.Photon90_R9Id90_HE10_IsoM |
            events.HLT.Photon120_R9Id90_HE10_IsoM |
            events.HLT.Photon165_R9Id90_HE10_IsoM 
    )

def prescale(events):
	ps = np.ones(len(events.HLT.Photon165_R9Id90_HE10_IsoM))
	for i in range(len(events.HLT.Photon165_R9Id90_HE10_IsoM)):
		if events.HLT.Photon165_R9Id90_HE10_IsoM[i]: ps[i] = 1
		else:
			if events.HLT.Photon120_R9Id90_HE10_IsoM[i]: ps[i] = 10
			else:
				if events.HLT.Photon90_R9Id90_HE10_IsoM[i]: ps[i] = 31
				else:
					if events.HLT.Photon75_R9Id90_HE10_IsoM[i]: ps[i] = 62
					else:
						if events.HLT.Photon50_R9Id90_HE10_IsoM[i]: ps[i] = 259
						else: ps[i] = 0
	return ps

def gammaJetsSelection(events):
        # Apply trigger and MET filters
        events = events[METCleaning(events)]
        events = events[HLT_SinglePhoton(events)]

        # Define photon selection
        selectedPhotons = events.Photon
        selectedPhotons = selectedPhotons[(selectedPhotons.pt >= 50) & (np.abs(selectedPhotons.eta) < 1.44) & (selectedPhotons.cutBased == 3)]

        # Define lepton veto
        selectedElectrons = events.Electron
        selectedElectrons = selectedElectrons[(selectedElectrons.pt >= 10) & (selectedElectrons.cutBased == 2)]
        selectedMuons = events.Muon
        selectedMuons = selectedMuons[(selectedMuons.pt >= 10) & selectedMuons.looseId]

        # Define jet selection
        selectedJets = events.Jet
        selectedJets = selectedJets[(selectedJets.pt >= 40) & (np.abs(selectedJets.eta) < 2.5) & (selectedJets.jetId == 6)]

        # Compute deltaR between photon and leading jet to mitigate QCD
        events = events[(ak.num(selectedPhotons) == 1) & (ak.num(selectedElectrons) == 0) & (ak.num(selectedMuons) == 0) & (ak.num(selectedJets) >= 1)]
        photon = events.Photon[(events.Photon.pt >= 50) & (np.abs(events.Photon.eta) < 1.44) & (events.Photon.cutBased == 3)]
        leadingJet = events.Jet[:,0]
        photonLorentz = ak.zip(
            {
                "pt": photon.pt,
                "eta": photon.eta,
                "phi": photon.phi,
                "mass": photon.mass,
            },
            with_name="PtEtaPhiMLorentzVector",
        )
        leadingJetLorentz = ak.zip(
            {
                "pt": leadingJet.pt,
                "eta": leadingJet.eta,
                "phi": leadingJet.phi,
                "mass": leadingJet.mass,
            },
            with_name="PtEtaPhiMLorentzVector",
        )
        deltaR_photon_jet = photonLorentz.delta_r(leadingJetLorentz)

        # Apply event selection and redefine useful collections
        events = events[deltaR_photon_jet[:,0] > 0.5]
        #print('Number of selected events = '+str(len(events)))
        #print('Selection efficiency = '+str(len(events)/len(nEvents)))
        photon = events.Photon
        pv = events.PV
        met_raw = events.RawMET
        met_pf = events.MET
        met_puppi = events.PuppiMET

        # Retrieve prescale
        #weights = processor.Weights(len(events))
        #weights.add("prescale",prescale(events))
        weights = prescale(events)
        #weights = np.ones(len(events))


        gammaDict = {'events': events, 'weights': weights, 'boson': photon, 'pv': pv, 'met_raw': met_raw, 'met_pf': met_pf, 'met_puppi': met_puppi}

        return gammaDict
