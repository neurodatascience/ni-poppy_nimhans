#!/usr/bin/env python

import argparse
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from nipoppy.workflow.utils import (
    BIDS_SESSION_PREFIX,
    COL_BIDS_ID_MANIFEST,
    COL_CONV_STATUS,
    COL_DATATYPE_MANIFEST,
    COL_PARTICIPANT_DICOM_DIR,
    COL_DICOM_ID,
    COL_DOWNLOAD_STATUS,
    COL_ORG_STATUS,
    COL_SESSION_MANIFEST,
    COL_SUBJECT_MANIFEST,
    COLS_STATUS,
    DNAME_BACKUPS_STATUS, 
    FNAME_STATUS,
    FNAME_MANIFEST,
    load_manifest,
    participant_id_to_bids_id,
    participant_id_to_dicom_id,
    session_id_to_bids_session,
    save_backup,
)

DPATH_STATUS_RELATIVE = Path('scratch', 'raw_dicom')
FPATH_MANIFEST_RELATIVE = Path('tabular') / FNAME_MANIFEST

FLAG_EMPTY = '--empty'
FLAG_REGENERATE = '--regenerate' # TODO move this to common utils?

GLOBAL_CONFIG_DATASET_ROOT = 'DATASET_ROOT'
GLOBAL_CONFIG_SESSIONS = 'SESSIONS'

def run(global_config_file, regenerate=False, empty=False):
    
    # parse global config
    with open(global_config_file) as file:
        global_config = json.load(file)
    dpath_dataset = Path(global_config[GLOBAL_CONFIG_DATASET_ROOT])

    # generate DICOM/BIDS directory paths
    dpath_downloaded_dicom = dpath_dataset / 'scratch' / 'raw_dicom'
    dpath_organized_dicom = dpath_dataset / 'dicom'
    dpath_converted = dpath_dataset / 'bids'

    # get path to status file
    fpath_status_symlink = dpath_dataset / DPATH_STATUS_RELATIVE / FNAME_STATUS

    # load manifest
    fpath_manifest = dpath_dataset / FPATH_MANIFEST_RELATIVE
    df_manifest = load_manifest(fpath_manifest)

    # validate that sessions are all in global configs
    sessions_global_config = {
        session_id_to_bids_session(session)
        for session in global_config[GLOBAL_CONFIG_SESSIONS]
    }
    sessions_manifest = set(df_manifest.loc[~df_manifest[COL_SESSION_MANIFEST].isna(), COL_SESSION_MANIFEST].apply(session_id_to_bids_session))
    if not sessions_manifest.issubset(sessions_global_config):
        raise ValueError(
            f'Not all sessions in the manifest are in global config:'
            f'\n{sessions_manifest - sessions_global_config}'
        )

    # only participants with imaging data have non-empty session column
    df_status = df_manifest.loc[~df_manifest[COL_SESSION_MANIFEST].isna()].copy()

    # sanity check that everyone who has session_id also has non-empty datatype list
    has_datatypes = df_status.set_index(COL_SUBJECT_MANIFEST)[COL_DATATYPE_MANIFEST].apply(lambda datatypes: len(datatypes) > 0)
    participants_without_datatypes = has_datatypes.loc[~has_datatypes].index.values
    if len(participants_without_datatypes) > 0:
        raise ValueError(
            f'Some participants have a value in "{COL_SESSION_MANIFEST}" but nothing in "{COL_DATATYPE_MANIFEST}": {participants_without_datatypes}'
        )

    # look for existing status file
    if fpath_status_symlink.exists() and not empty:
        df_status_old = pd.read_csv(fpath_status_symlink, dtype=str)
    else:
        df_status_old = None
        
        if (not empty) and (not regenerate):
            raise ValueError(
                f'Did not find an existing {FNAME_STATUS} file'
                f'. Use {FLAG_EMPTY} to create an empty one'
                f' or {FLAG_REGENERATE} to create one based on current files'
                ' in the dataset (can be slow)'
            )
        
    # generate bids_id
    df_status.loc[:, COL_BIDS_ID_MANIFEST] = df_status[COL_SUBJECT_MANIFEST].apply(
        participant_id_to_bids_id
    )
    
    # initialize dicom dir (cannot be inferred directly from participant id)
    df_status.loc[:, COL_PARTICIPANT_DICOM_DIR] = np.nan

    # populate dicom_id
    df_status.loc[:, COL_DICOM_ID] = df_status[COL_SUBJECT_MANIFEST].apply(
        participant_id_to_dicom_id
    )

    # initialize all status columns
    for col in [COL_DOWNLOAD_STATUS, COL_ORG_STATUS, COL_CONV_STATUS]:
        df_status[col] = False

    if regenerate:

        try:
            from nipoppy.workflow.dicom_org.dicom_dir_func import participant_id_to_dicom_dir

        except ModuleNotFoundError:
            from nipoppy.workflow.dicom_org.sample_dicom_dir_func import participant_id_to_dicom_dir
            warnings.warn(
                'Could not find participant ID -> DICOM directory conversion function, '
                'using participant_id as dicom_dir. To use a custom function, make a new file called '
                f'"dicom_dir_func.py" in {Path(__file__).parent} that contains a '
                'function definition for participant_id_to_dicom_dir(). '
                'See sample_dicom_dir_func.py for an example.'
            )

        df_status[COL_PARTICIPANT_DICOM_DIR] = df_status.apply(
            lambda row: participant_id_to_dicom_dir(
                row[COL_SUBJECT_MANIFEST],
                str(row[COL_SESSION_MANIFEST]).removeprefix(BIDS_SESSION_PREFIX),
                global_config,
            ),
            axis='columns',
        )

        # look for raw DICOM: scratch/raw_dicom/session/dicom_dir
        df_status[COL_DOWNLOAD_STATUS] = check_status(
            df_status, dpath_downloaded_dicom, COL_PARTICIPANT_DICOM_DIR, session_first=True,
        )

        # look for organized DICOM
        df_status[COL_ORG_STATUS] = check_status(
            df_status, dpath_organized_dicom, COL_DICOM_ID, session_first=True,
        )

        # look for BIDS: bids/bids_id/session
        df_status[COL_CONV_STATUS] = check_status(
            df_status, dpath_converted, COL_BIDS_ID_MANIFEST, session_first=False,
        )

        # warn user if there are rows with a 'True' column after one or more 'False' columns
        has_lost_files = (
            (df_status[COL_CONV_STATUS] & ~(df_status[COL_ORG_STATUS] | df_status[COL_DOWNLOAD_STATUS])) |
            (df_status[COL_ORG_STATUS] & ~df_status[COL_DOWNLOAD_STATUS])
        )
        if has_lost_files.any():
            warnings.warn(
                'Some participants-session pairs seem to have lost files:'
                f'\n{df_status.loc[has_lost_files]}'
            )

    else:

        df_status = df_status.set_index([COL_SUBJECT_MANIFEST, COL_SESSION_MANIFEST])

        if df_status_old is not None:
            subject_session_pairs_old = pd.Index(zip(
                df_status_old[COL_SUBJECT_MANIFEST],
                df_status_old[COL_SESSION_MANIFEST],
            ))
            df_status_deleted_rows = df_status_old.loc[~subject_session_pairs_old.isin(df_status.index)]

            # error if new status file loses subject-session pairs
            if len(df_status_deleted_rows) > 0:
                raise RuntimeError(
                    'Some of the subject/session pairs in the old status file do not'
                    ' seem to exist anymore:'
                    f'\n{df_status_deleted_rows}'
                    f'\nUse {FLAG_REGENERATE} to fully regenerate the status file')
        else:
            subject_session_pairs_old = pd.Index([])

        df_status_new_rows = df_status.loc[~df_status.index.isin(subject_session_pairs_old)]
        df_status_new_rows = df_status_new_rows.reset_index()[COLS_STATUS]
        df_status = pd.concat([df_status_old, df_status_new_rows], axis='index')
        print(f'\nAdded {len(df_status_new_rows)} rows to existing status file')

    df_status = df_status[COLS_STATUS].drop_duplicates(ignore_index=True)
    df_status = df_status.sort_values([COL_SUBJECT_MANIFEST, COL_SESSION_MANIFEST], ignore_index=True)

    # do not write file if there are no changes from previous one
    if df_status_old is not None and df_status.equals(df_status_old):
        print(f'\nNo change from existing status file. Will not write new status file.')
        return

    # save backup and make symlink
    save_backup(df_status, fpath_status_symlink, DNAME_BACKUPS_STATUS)

def check_status(df: pd.DataFrame, dpath, col_dname, session_first=True):

    def check_dir(dpath):
        dpath = Path(dpath)
        if dpath.exists():
            return len(list(dpath.iterdir())) > 0
        return False
    
    dpath = Path(dpath)
    status = pd.Series(np.nan, index=df.index)
    for session in df[COL_SESSION_MANIFEST].drop_duplicates():
        if pd.isna(session):
            continue
        idx = (df[COL_SESSION_MANIFEST] == session)

        if session_first:
            check_func = lambda dname: check_dir(dpath / session / dname)
        else:
            check_func = lambda dname: check_dir(dpath / dname / session)

        status.loc[idx] = df[col_dname].apply(check_func)

    return status

if __name__ == '__main__':
    # argparse
    HELPTEXT = f"""
    Generate/update CSV file that tracks DICOM fetching/organization/conversion.
    """
    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument(
        '--global_config', type=str, required=True,
        help='path to global config file for your nipoppy dataset (required)')
    parser.add_argument(
        FLAG_REGENERATE, action='store_true',
        help=('regenerate entire status file'
              ' (default: only append rows for new subjects/sessions)'),
    )
    parser.add_argument(
        FLAG_EMPTY, action='store_true', 
        help='generate empty status file (without checking what\'s on the disk)')
    args = parser.parse_args()

    # parse
    global_config_file = args.global_config
    regenerate = getattr(args, FLAG_REGENERATE.lstrip('-'))
    empty = getattr(args, FLAG_EMPTY.lstrip('-'))

    run(global_config_file, regenerate=regenerate, empty=empty)
