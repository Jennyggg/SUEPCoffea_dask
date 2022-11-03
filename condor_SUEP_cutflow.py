import os, sys, glob
import pandas as pd
import json
import argparse
import time
import numpy
from tqdm import tqdm

#Import coffea specific features
import coffea
from coffea.processor import run_uproot_job, futures_executor, iterative_executor

#SUEP Repo Specific
from workflows.SUEP_coffea_cutflow import cutflow_cluster
from workflows.merger import *

#Begin argparse
parser = argparse.ArgumentParser("")
parser.add_argument('--isMC', type=int, default=1, help="")
parser.add_argument('--jobNum', type=int, default=1, help="")
parser.add_argument('--era', type=str, default="2018", help="")
parser.add_argument('--doSyst', type=int, default=1, help="")
parser.add_argument('--infile', type=str, default=None, help="")
parser.add_argument('--dataset', type=str, default="X", help="")
parser.add_argument('--nevt', type=str, default=-1, help="")
parser.add_argument('--doInf', type=bool, default=False, help="")
options = parser.parse_args()

out_dir = os.getcwd()
modules_era = []

modules_era.append(cutflow_cluster(isMC=options.isMC, era=int(options.era), scouting=0, do_syst=options.doSyst,  syst_var='', sample=options.dataset, weight_syst='' , flag=False, do_inf=options.doInf, output_location=out_dir))

for instance in modules_era:
    output = run_uproot_job(
        {instance.sample: [options.infile]},
        treename='Events',
        processor_instance=instance,
        executor=futures_executor,
        executor_args={'workers': 2,
                       'schema': processor.NanoAODSchema,
                       'xrootdtimeout': 10,
        },
        chunksize=100000,
    )
    
coffea.util.save(output, "output.coffea")
