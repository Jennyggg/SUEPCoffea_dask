import argparse
import os

# Import coffea specific features
import ROOT
import math
# SUEP Repo Specific

# Begin argparse
parser = argparse.ArgumentParser("")
parser.add_argument("--infile", required=True, type=str, default=None, help="")
args = parser.parse_args()

fFile = ROOT.TFile(args.infile,'READ')
fTree = fFile.Get("mmtree/tree")
h2 = ROOT.TH2D("h2","Charged PFCand phi-eta",50,-2.4,2.4,50,-math.pi,math.pi)
fTree.Draw("PFcand_phi:PFcand_eta>>h2","(PFcand_q!=0)&&(PFcand_pt>0.75)&&(abs(PFcand_eta)<2.4)&&(PFcand_vertex==0)","goff")
oFile = ROOT.TFile("out.root","RECREATE")
h2.Write()
