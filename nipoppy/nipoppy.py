import os
from pathlib import Path
import argparse
import json
import workflow.logger as my_logger
from workflow.tabular import generate_manifest
from workflow.dicom_org import run_dicom_org
from workflow.dicom_org import check_dicom_status
from workflow.bids_conv import run_bids_conv

# argparse
HELPTEXT = """
Top level script to orchestrate workflows as specified in the global_config.json
"""
parser = argparse.ArgumentParser(description=HELPTEXT)
parser.add_argument('--global_config', type=str, required=True, help='path to global config file for your nipoppy dataset')
parser.add_argument('--session', type=str, required=True, help='current session or visit ID for the dataset')
parser.add_argument('--n_jobs', type=int, default=4, help='number of parallel processes')

args = parser.parse_args()

# read global configs
global_config_file = args.global_config
with open(global_config_file, 'r') as f:
    global_configs = json.load(f)

DATASET_ROOT = global_configs["DATASET_ROOT"]
log_dir = f"{DATASET_ROOT}/scratch/logs/"
log_file = f"{log_dir}/nipoppy.log"

session = args.session
n_jobs = args.n_jobs

logger = my_logger.get_logger(log_file)

logger.info("-"*75)
logger.info(f"Starting nipoppy for {DATASET_ROOT} dataset...")
logger.info("-"*75)

logger.info(f"dataset session (i.e visit): {session}")
logger.info(f"Running {n_jobs} jobs in parallel")

workflows = global_configs["WORKFLOWS"]
logger.info(f"Running {workflows} serially")

for wf in workflows:
    logger.info("-"*50)
    logger.info(f"Starting workflow: {wf}")
    logger.info("-"*50)

    if wf == "generate_manifest":
        logger.info(f"***All sessions are fetched while generating manifest***")
        generate_manifest.run(global_configs, task="regenerate", dash_bagel=True, logger=logger)
        check_dicom_status.run(global_config_file, regenerate=True, empty=False)

    elif wf == "dicom_org":        
        run_dicom_org.run(global_configs, session, n_jobs=n_jobs)
    elif wf == "bids_conv": 
        run_bids_conv.run(global_configs, session, n_jobs=n_jobs)
    else:
        logger.error(f"Unknown workflow: {wf}")

    logger.info("-"*50)
    logger.info(f"Finishing workflow: {wf}")
    logger.info("-"*50)

logger.info("-"*75)
logger.info(f"Finishing nipoppy run...")
logger.info("-"*75)