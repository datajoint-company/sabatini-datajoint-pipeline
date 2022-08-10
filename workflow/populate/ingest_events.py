# Need to write custom ingestion for behavioral events
# Perform Behavioral Data ingestion from events.csv, block.csv, trial.csv
import numpy as np
from .ingest_sessions import session, subject
from workflow.pipeline import event, trial
import pandas as pd
from element_interface.utils import ingest_csv_to_table

def generate_behavior_csv(block_csv_path = "./user_data/block.csv",
    trial_csv_path = "./user_data/trial.csv",
    event_csv_path = ".user_data/events.csv"
    ):
    
    trial_d = {'subject': subject, 'session_datetime': session.fetch('session_datetime'), 'filepath': trial_csv_path}
    event_d = {'subject': subject, 'session_datetime': session.fetch('session_datetime'), 'filepath': event_csv_path}
    block_d = {'subject': subject, 'session_datetime': session.fetch('session_datetime'), 'filepath': block_csv_path}
    
    recording_csv_table = pd.DataFrame(trial_d, event_d, block_d)
    filepath = "./user_data/behavior_recordings.csv"
    recording_csv_table.to_csv(filepath)

def ingest_events(
    recording_csv_path = "./user_data/behavior_recordings.csv",
    block_csv_path = "./user_data/block.csv",
    trial_csv_path = "./user_data/trial.csv",
    event_csv_path = ".user_data/events.csv",
    skip_duplicates = True,
    verbose = True,
    ):
    """
    Inserts Data from example data .csv's into event and trial schemas
    """
    block_df = pd.read_csv(block_csv_path)
    trial_df = pd.read_csv(trial_csv_path)
    event_df = pd.read_csv(event_csv_path)
    
    # Add subject and session_datetime columns at starts of tables
    block_subj = [subject.fetch('subject') for i in range(len(block_df.rows))]
    block_sess = [session.fetch('session') for i in range(len(block_df.rows))] 
    block_df.insert(0, block_sess, 'session_datetime')
    block_df.insert(0, block_subj, 'subject')

    event_subj = [subject.fetch('subject') for i in range(len(event_df.rows))]
    event_sess = [session.fetch('session') for i in range(len(event_df.rows))] 
    event_df.insert(0, event_sess, 'session_datetime')
    event_df.insert(0, event_subj, 'subject')

    trial_subj = [subject.fetch('subject') for i in range(len(trial_df.rows))]
    trial_sess = [session.fetch('session') for i in range(len(trial_df.rows))] 
    trial_df.insert(0, trial_sess, 'session_datetime')
    trial_df.insert(0, trial_subj, 'subject')

    # Rewrite .csv files 
    block_df.to_csv(block_csv_path)
    trial_df.to_csv(trial_csv_path)
    event_df.to_csv(event_csv_path)

    csvs = [
        recording_csv_path,
        recording_csv_path,
        block_csv_path,
        block_csv_path,
        trial_csv_path,
        trial_csv_path,
        trial_csv_path,
        trial_csv_path,
        event_csv_path,
        event_csv_path,
        event_csv_path,
    ]
    tables = [
        event.BehaviorRecording(),
        event.BehaviorRecording.File(),
        trial.Block(),
        trial.Block.Attribute(),
        trial.TrialType(),
        trial.Trial(),
        trial.Trial.Attribute(),
        trial.BlockTrial(),
        event.EventType(),
        event.Event(),
        trial.TrialEvent(),
    ]

    # Allow direct insert required because element-event has Imported that should be Manual
    print(f"\n---- Insert events into event and trial schema tables----")
    ingest_csv_to_table(
        csvs,
        tables,
        skip_duplicates=skip_duplicates,
        verbose=verbose,
        allow_direct_insert=True,
    )






