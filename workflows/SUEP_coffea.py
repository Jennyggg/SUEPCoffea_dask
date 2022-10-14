"""
SUEP_coffea.py
Coffea producer for SUEP analysis. Uses fastjet package to recluster large jets:
https://github.com/scikit-hep/fastjet
Chad Freer and Luca Lavezzo, 2021
"""
from coffea import processor, lumi_tools
from typing import List, Optional
import awkward as ak
import pandas as pd
import numpy as np
import fastjet
import vector
vector.register_awkward()

#Importing SUEP specific functions
from workflows.pandas_utils import *
from workflows.SUEP_utils import *
from workflows.ML_utils import *
from workflows.CMS_corrections.golden_jsons_utils import *
from workflows.CMS_corrections.jetmet_utils import *
from workflows.CMS_corrections.track_killing_utils import *

class SUEP_cluster(processor.ProcessorABC):
    def __init__(self, isMC: int, era: int, scouting: int, sample: str,  do_syst: bool, syst_var: str, weight_syst: bool, flag: bool, do_inf: bool, output_location: Optional[str]) -> None:
        self._flag = flag
        self.output_location = output_location
        self.do_syst = do_syst
        self.gensumweight = 1.0
        self.scouting = scouting
        self.era = int(era)
        self.isMC = bool(isMC)
        self.sample = sample
        self.syst_var, self.syst_suffix = (syst_var, f'_sys_{syst_var}') if do_syst and syst_var else ('', '')
        self.weight_syst = weight_syst
        self.do_inf = do_inf
        self.prefixes = {"SUEP": "SUEP"}
        
        self.out_vars = pd.DataFrame()

        if self.do_inf:
            
            # ML settings
            self.batch_size = 1024
            
            # GNN settings
            # model names and configs should be in data/GNN/
            self.dgnn_model_names = ['single_l5_bPfcand_S1']#Name for output
            self.configs = ['config.yml']#config paths
            self.obj = 'bPFcand'
            self.coords = 'cyl'

            # SSD settings
            self.ssd_models = []#Add to this list. There will be an output for each
            self.eta_pix = 280
            self.phi_pix = 360
            self.eta_span = (-2.5, 2.5)
            self.phi_span = (-np.pi, np.pi)
            self.eta_scale = self.eta_pix/(self.eta_span[1]-self.eta_span[0])
            self.phi_scale = self.phi_pix/(self.phi_span[1]-self.phi_span[0])
        
        #Set up for the histograms
        self._accumulator = processor.dict_accumulator({})
        
    @property
    def accumulator(self):
        return self._accumulator
    
    def jet_awkward(self,Jets):
        """"
        Create awkward array of jets. Applies basic selections.
        Returns: awkward array of dimensions (events x jets x 4 momentum)
        """
        Jets_awk = ak.zip({
            "pt": Jets.pt,
            "eta": Jets.eta,
            "phi": Jets.phi,
            "mass": Jets.mass,
        })
        jet_awk_Cut = (Jets.pt > 30) & (abs(Jets.eta)<2.4)
        Jets_correct = Jets_awk[jet_awk_Cut]
        return Jets_correct
  
    def eventSelection(self, events):
        """
        Applies trigger, returns events.
        """
        if self.scouting != 1:
            if self.era == 2016:
                trigger = (events.HLT.PFHT900 == 1)
            else:
                trigger = (events.HLT.PFHT1050 == 1)
            events = events[trigger]

        return events
    
    def getGenTracks(self, events):
        genParts = events.GenPart
        genParts = ak.zip({
            "pt": genParts.pt,
            "eta": genParts.eta,
            "phi": genParts.phi,
            "mass": genParts.mass,
            "pdgID": genParts.pdgId
        }, with_name="Momentum4D")
        return genParts
    
    def getTracks(self, events):
        Cands = ak.zip({
            "pt": events.PFCands.trkPt,
            "eta": events.PFCands.trkEta,
            "phi": events.PFCands.trkPhi,
            "mass": events.PFCands.mass
        }, with_name="Momentum4D")
        cut = (events.PFCands.fromPV > 1) & \
                 (events.PFCands.trkPt >= 0.75) & \
                 (abs(events.PFCands.trkEta) <= 2.5) & \
                 (abs(events.PFCands.dz) < 10) & \
                 (events.PFCands.dzErr < 0.05)
        Cleaned_cands = Cands[cut]
        Cleaned_cands = ak.packed(Cleaned_cands)

        #Prepare the Lost Track collection
        LostTracks = ak.zip({
            "pt": events.lostTracks.pt,
            "eta": events.lostTracks.eta,
            "phi": events.lostTracks.phi,
            "mass": 0.0
        }, with_name="Momentum4D")
        cut = (events.lostTracks.fromPV > 1) & \
            (events.lostTracks.pt >= 0.75) & \
            (abs(events.lostTracks.eta) <= 1.0) & \
            (abs(events.lostTracks.dz) < 10) & \
            (events.lostTracks.dzErr < 0.05)
        Lost_Tracks_cands = LostTracks[cut]
        Lost_Tracks_cands = ak.packed(Lost_Tracks_cands)

        # select which tracks to use in the script
        # dimensions of tracks = events x tracks in event x 4 momenta
        tracks = ak.concatenate([Cleaned_cands, Lost_Tracks_cands], axis=1)
        
        return tracks, Cleaned_cands
        
    def getScoutingTracks(self, events):
        Cands = ak.zip({
            "pt": events.PFcand.pt,
            "eta": events.PFcand.eta,
            "phi": events.PFcand.phi,
            "mass": events.PFcand.mass
        }, with_name="Momentum4D")
        cut = (events.PFcand.pt >= 0.75) & \
                (abs(events.PFcand.eta) <= 2.5) & \
                (events.PFcand.vertex == 0) & \
                (events.PFcand.q != 0)
        Cleaned_cands = Cands[cut]
        tracks =  ak.packed(Cleaned_cands)
        return tracks, Cleaned_cands
    
    def storeEventVars(self, events, tracks, 
                       ak_inclusive_jets, ak_inclusive_cluster,
                       out_label=""):
        
        # select out ak4jets
        ak4jets = self.jet_awkward(events.Jet)        
        
        # work on JECs and systematics
        jets_c = apply_jecs(isMC=self.isMC, Sample=self.sample, era=self.era, events=events)
        jets_jec = self.jet_awkward(jets_c)
        if self.isMC:
            jets_jec_JERUp   = self.jet_awkward(jets_c["JER"].up)
            jets_jec_JERDown = self.jet_awkward(jets_c["JER"].down)
            jets_jec_JESUp   = self.jet_awkward(jets_c["JES_jes"].up)
            jets_jec_JESDown = self.jet_awkward(jets_c["JES_jes"].down)
        # For data set these all to nominal so we can plot without switching all of the names
        else: 
            jets_jec_JERUp   = jets_jec
            jets_jec_JERDown = jets_jec
            jets_jec_JESUp   = jets_jec
            jets_jec_JESDown = jets_jec
            
        # save per event variables to a dataframe
        self.out_vars["ntracks"+out_label] = ak.num(tracks).to_list()
        self.out_vars["ngood_fastjets"+out_label] = ak.num(ak_inclusive_jets).to_list()
        if out_label == "":
            self.out_vars["ht"+out_label] = ak.sum(ak4jets.pt,axis=-1).to_list()
            self.out_vars["ht_JEC"+out_label] = ak.sum(jets_jec.pt,axis=-1).to_list()
            self.out_vars["ht_JEC"+out_label+"_JER_up"] = ak.sum(jets_jec_JERUp.pt,axis=-1).to_list()
            self.out_vars["ht_JEC"+out_label+"_JER_down"] = ak.sum(jets_jec_JERDown.pt,axis=-1).to_list()
            self.out_vars["ht_JEC"+out_label+"_JES_up"] = ak.sum(jets_jec_JESUp.pt,axis=-1).to_list()
            self.out_vars["ht_JEC"+out_label+"_JES_down"] = ak.sum(jets_jec.pt,axis=-1).to_list()

            if self.era == 2016 and self.scouting == 0:
                self.out_vars["HLT_PFHT900"+out_label] = events.HLT.PFHT900
            elif self.scouting == 0:
                self.out_vars["HLT_PFHT1050"+out_label] = events.HLT.PFHT1050
            self.out_vars["ngood_ak4jets"+out_label] = ak.num(ak4jets).to_list()
            if self.scouting == 1:
                self.out_vars["PV_npvs"+out_label] = ak.num(events.Vertex.x)
            else:
                if self.isMC:
                    self.out_vars["Pileup_nTrueInt"+out_label] = events.Pileup.nTrueInt
                    if len(events.PSWeight[0])==4:
                        self.out_vars["PSWeight"+out_label+"_ISR_up"] = events.PSWeight[:,0]
                        self.out_vars["PSWeight"+out_label+"_ISR_down"] = events.PSWeight[:,2]
                        self.out_vars["PSWeight"+out_label+"_FSR_up"] = events.PSWeight[:,1]
                        self.out_vars["PSWeight"+out_label+"_FSR_down"] = events.PSWeight[:,3]
                    else:
                        self.out_vars["PSWeight"+out_label] = events.PSWeight[:,0]
                self.out_vars["PV_npvs"+out_label] = events.PV.npvs
                self.out_vars["PV_npvsGood"+out_label] = events.PV.npvsGood
                
        # get gen SUEP mass
        SUEP_genMass = len(events)*[0]
        if self.isMC and not self.scouting:
            genParts = self.getGenTracks(events)
            genSUEP = genParts[(abs(genParts.pdgID) == 25)]
            # we need to grab the last SUEP in the chain for each event
            SUEP_genMass = [g[-1].mass if len(g) > 0 else 0 for g in genSUEP]
        self.out_vars["SUEP_genMass"+out_label] = SUEP_genMass
    
    def initializeColumns(self, label=""):
        # need to add these to dataframe when no events pass to make the merging work
        # for some reason, initializing these as empty and then trying to fill them doesn't work
        self.columns_IRM = [
                "SUEP_nconst_IRM", "SUEP_ntracks_IRM", 
                "SUEP_pt_avg_IRM", "SUEP_pt_avg_b_IRM",
                "SUEP_S1_IRM", "SUEP_rho0_IRM", "SUEP_rho1_IRM", 
                "SUEP_pt_IRM", "SUEP_eta_IRM", "SUEP_phi_IRM", "SUEP_mass_IRM",
                "dphi_SUEP_ISR_IRM"
        ]
        self.columns_CL = [c.replace("IRM", "CL") for c in self.columns_IRM]
        self.columns_CL_ISR = [c.replace("IRM", "CL".replace("SUEP", "ISR")) for c in self.columns_IRM]
        self.columns_ML = []
        if self.do_inf: 
            self.columns_ML = [m+"_GNN" for m in self.dgnn_model_names] + ['SUEP_S1_GNN', 'SUEP_nconst_GNN']
            self.columns_ML += [m+"_ssd" for m in self.ssd_models] 
        self.columns = self.columns_CL + self.columns_CL_ISR + self.columns_ML
        
        # add a specific label to all columns
        for iCol in range(len(self.columns)): self.columns[iCol] = self.columns[iCol] + label
    
    def analysis(self, events, do_syst=False, col_label=""):
        #####################################################################################
        # ---- Trigger event selection
        # Cut based on ak4 jets to replicate the trigger
        #####################################################################################
        
        # golden jsons for offline data
        if not self.isMC and self.scouting!=1: events = applyGoldenJSON(self, events)
        events = self.eventSelection(events)
        
        # output empty dataframe if no events pass trigger
        if len(events) == 0:
            print("No events passed trigger. Saving empty outputs.")
            self.out_vars = pd.DataFrame(['empty'], columns=['empty'])
            return
        
        #####################################################################################
        # ---- Track selection
        # Prepare the clean PFCand matched to tracks collection     
        #####################################################################################

        if self.scouting == 1: tracks, Cleaned_cands = self.getScoutingTracks(events)
        else: tracks, Cleaned_cands = self.getTracks(events)
            
        if self.isMC and do_syst:
            tracks = tracksSystematics(self, tracks)
            Cleaned_cands = tracksSystematics(self, Cleaned_cands)
        
        #####################################################################################
        # ---- FastJet reclustering
        # The jet clustering part
        #####################################################################################
        
        ak_inclusive_jets, ak_inclusive_cluster = FastJetReclustering(self, tracks, r=1.5, minPt=150)
        
        #####################################################################################
        # ---- Event level information
        #####################################################################################
                
        self.storeEventVars(events, tracks,
                            ak_inclusive_jets, ak_inclusive_cluster,
                            out_label=col_label)

        # indices of events in tracks, used to keep track which events pass selections
        indices = np.arange(0,len(tracks))
        
        # initialize the columns with all the variables that you want to fill
        self.initializeColumns(col_label)       
                
        #####################################################################################
        # ---- Cut Based Analysis
        #####################################################################################
        
        # remove events with at least 2 clusters (i.e. need at least SUEP and ISR jets for IRM)
        clusterCut = (ak.num(ak_inclusive_jets, axis=1)>1)
        ak_inclusive_cluster = ak_inclusive_cluster[clusterCut]
        ak_inclusive_jets = ak_inclusive_jets[clusterCut]
        tracks = tracks[clusterCut]
        indices = indices[clusterCut]
        
        # output file if no events pass selections, avoids errors later on
        if len(tracks) == 0:
            print("No events pass clusterCut.")
            for c in self.columns: self.out_vars[c] = np.nan
            return
        
        tracks, indices, topTwoJets = getTopTwoJets(self, tracks, indices, ak_inclusive_jets, ak_inclusive_cluster)
        SUEP_cand, ISR_cand, SUEP_cluster_tracks, ISR_cluster_tracks = topTwoJets
        
        # self.ISRRemovalMethod(indices, tracks, 
        #                      SUEP_cand, ISR_cand)
        
        ClusterMethod(self, indices, tracks, 
                           SUEP_cand, ISR_cand, 
                           SUEP_cluster_tracks, ISR_cluster_tracks, 
                           do_inverted=True,
                           out_label=col_label)
        
        DGNNMethod(self, indices, tracks, SUEP_cluster_tracks, SUEP_cand, 
                       out_label=col_label)
                
        # self.ConeMethod(indices, tracks, 
        #                 SUEP_cand, ISR_cand)

    def process(self, events):
        output = self.accumulator.identity()
        dataset = events.metadata['dataset']

        # gen weights
        if self.isMC and self.scouting==1: self.gensumweight = ak.num(events.PFcand.pt,axis=0)
        elif self.isMC: self.gensumweight = ak.sum(events.genWeight)

        # run the anlaysis with the track systematics applied
        if self.isMC and self.do_syst:
            self.analysis(events, do_syst=True, col_label='_track_down')
        
        # run the analysis
        self.analysis(events)
        
        # save the out_vars object as a Pandas DataFrame
        save_dfs(self, [self.out_vars],["vars"], events.behavior["__events_factory__"]._partition_key.replace("/", "_")+".hdf5")
        return output

    def postprocess(self, accumulator):
        return accumulator