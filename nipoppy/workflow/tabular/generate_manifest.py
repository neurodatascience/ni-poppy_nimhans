import os
from pathlib import Path
import argparse
import json
import nipoppy.workflow.logger as my_logger
import nipoppy.workflow.utils as utils
import requests
import urllib3
import ssl
import pandas as pd
from datetime import datetime

# Global variables
MANIFEST_COLUMNS = ["participant_id", "visit", "session", "datatype"]
DATATYPES = ["anat","dwi","fmap","func","perf"]

# Map all clinical visits to imaging (BIDS) sessions
VISIT_SESSION_MAP = {
    'Intake': 'ses-01',
    'Follow up 1': 'ses-02',
    'Follow up 2': 'ses-03'
}

# Dashboard variables
DASH_INDEX_COLUMNS = ["participant_id", "session", "bids_id"]
DASH_NAME_COL = "assessment_name"
DASH_VAL_COL = "assessment_score"

class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)
        
def get_legacy_session():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount('https://', CustomHttpAdapter(ctx))
    return session

def api_call(url, query, logger):
    """ API call to redcap instance to fetch predefined report"""
    try:
        r = requests.post(url, data=query, verify=False)
    except Exception as e:
        logger.warning(f"Default RedCap API request Failed: {e}")
        logger.info("Trying legacy RedCap API request...")
        r = get_legacy_session().post(url, data=query)

    http_status = str(r.status_code)
    logger.info(f'HTTP Status: {http_status}')

    if http_status == "200":
        query_results = r.json()
        query_df = pd.DataFrame(query_results)

    else:
        logger.error(f"RedCap API request Failed with HTTP Status: {http_status}")

    return query_df

def run(global_configs, task, query_label="Q1", dash_bagel=True, logger=None):
    # load config
    DATASET_ROOT = global_configs["DATASET_ROOT"]
    log_dir = f"{DATASET_ROOT}/scratch/logs/"
    redcap_config_json = f"{DATASET_ROOT}/proc/.redcap.json"
    redcap_config = json.load(open(redcap_config_json))

    # Available imaging (BIDS) sessions
    study_sessions = global_configs["SESSIONS"]

    # logger
    if logger is None:
        log_file = f"{log_dir}/bids_conv.log"
        logger = my_logger.get_logger(log_file)

    # timestamp
    now = datetime.now() # current date and time
    timestamp = now.strftime("%Y%m%d")

    # redcap report
    # redcap report (backup)
    redcap_report_fpath = f"{DATASET_ROOT}/tabular/redcap_report.csv"
    redcap_report_bkup_dpath = f"{DATASET_ROOT}/tabular/.redcap_exports/"
    redcap_report_bkup_fpath = f"{redcap_report_bkup_dpath}/redcap_report_{timestamp}.csv"

    # read from backup (avoid frequent API calls)
    if query_label in ["backup", "bkup"]:
        logger.info(f"Reading redcap report from backup: {redcap_report_fpath}")
        query_df = pd.read_csv(redcap_report_fpath)

    else:
        # redcap config
        url = redcap_config["url"]
        query = redcap_config["queries"][query_label]

        # run query
        logger.info(f"Running query {query_label}...")
        query_df = api_call(url, query, logger=logger)

        Path(redcap_report_bkup_dpath).mkdir(parents=True, exist_ok=True)
        query_df.to_csv(redcap_report_fpath, index=False)
        query_df.to_csv(redcap_report_bkup_fpath, index=False)

        logger.info(f"Saving redcap report to {redcap_report_fpath}")

    logger.info("Query results:")
    logger.info(query_df.head())

    # get the list of participants
    redcap_participants = query_df["record_id"].unique()
    n_participants = len(redcap_participants)

    # get the list of visits
    redcap_visits = query_df["redcap_event_name"].unique()
    n_visits = len(redcap_visits)

    logger.info(f"Fetched {n_participants} participants and {n_visits} visits: {redcap_visits}")

    # generate manifest
    if task == "regenerate":
        logger.info("Generating manifest...")
        logger.info(f"Mapping visits to sessions: {VISIT_SESSION_MAP}")
        
        manifest_df = pd.DataFrame(columns=MANIFEST_COLUMNS)

        manifest_df["participant_id"] = query_df["record_id"].copy()
        manifest_df["visit"] = query_df["redcap_event_name"].copy()
        
        # populate session and datatype
        manifest_df["session"] = query_df["redcap_event_name"].copy()
        manifest_df["session"] = manifest_df["session"].replace(VISIT_SESSION_MAP)
        manifest_df["datatype"] = "[]"
        manifest_df.loc[manifest_df["session"].isin(study_sessions), "datatype"] = f"{DATATYPES}"

        # save manifest
        manifest_fpath = f"{DATASET_ROOT}/tabular/manifest.csv"
        manifest_bkup_dpath = f"{DATASET_ROOT}/tabular/.manifests"
        manifest_bkup_fpath = f"{manifest_bkup_dpath}/manifest_{timestamp}.csv"

        Path(manifest_bkup_dpath).mkdir(parents=True, exist_ok=True)

        manifest_df.to_csv(manifest_fpath, index=False)
        manifest_df.to_csv(manifest_bkup_fpath, index=False)

        logger.info(f"Current manifest saved to {manifest_fpath} and backuped to: {manifest_bkup_fpath}")
        logger.info(manifest_df.head())

    else:
        logger.info("Updating manifest...")
        # TODO: implement update manifest

    # generate bagel for dashboard
    if dash_bagel:
        logger.info("Generating bagel for dashboard...")

        dash_df = query_df.copy()
        dash_df = dash_df.rename(columns={"record_id": "participant_id", "redcap_event_name": "session"})
        dash_df["session"] = dash_df["session"].replace(VISIT_SESSION_MAP)

        logger.info(f"dash participants: {dash_df['participant_id'].value_counts()}")
        dash_df["bids_id"] = dash_df["participant_id"].apply(utils.participant_id_to_bids_id)
        # dash_df.loc[dash_df["session"].isin(study_sessions),"bids_id"] = dash_df["participant_id"].apply(utils.participant_id_to_bids_id)

        # melt
        dash_df_melt = dash_df.melt(id_vars=DASH_INDEX_COLUMNS, var_name=DASH_NAME_COL, value_name=DASH_VAL_COL)

        # save bagel
        dash_fpath = f"{DATASET_ROOT}/tabular/bagel.csv"
        dash_df_melt.to_csv(dash_fpath, index=False)
        logger.info(f"Bagel saved to {dash_fpath}")

if __name__ == '__main__':
    # argparse
    HELPTEXT = """
    Script to generate manifest and tabular data bagels for the dashboard
    """
    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument('--global_config', type=str, required=True, help='path to global config file for your dataset')
    parser.add_argument('--task', type=str, default="regenerate", help='specify either regenerate or update the manifest')
    parser.add_argument('--query_label', type=str, default="Q1", help='query label to run')
    parser.add_argument('--dash_bagel', action='store_true', help='generate bagel for dashboard')

    # args
    args = parser.parse_args()

    global_configs = json.load(open(args.global_config))
    task = args.task
    query_label = args.query_label
    dash_bagel = args.dash_bagel

    run(global_configs, task, query_label, dash_bagel)