import datajoint as dj
import pandas as pd
from element_event import event, trial 
from core import session, subject
from workflow import db_prefix
from workflow.pipeline import lab, session

__all__ = ['event', 'trial']

# ------------- Activate "event" schema -------------
Session = session.Session

if not event.schema.is_activated():
    event.activate(db_prefix + 'event', linking_module=__name__)

# ------------- Activate "trial" schema -------------
Event = event.Event

if not trial.schema.is_activated():
    trial.activate(db_prefix + 'trial', db_prefix + 'event',linking_module=__name__)


@event.schema
class BehaviorIngestion(dj.Imported):
    definition = """
    -> Session
    ---
    ingestion_time: datetime        # Stores the start time of behavioral data ingestion
    """

    def make(self, key):
        """
        Insert behavioral event, trial and block data into corresponding schema tables 
        """
        session_dir = pathlib.Path(ephys.get_session_directory(key))
        root_dir = ephys.get_ephys_root_data_dir()[0]
        
        session_dir = session_dir.relative_to(root_dir).as_posix()

        event_path = session_dir / 'events.csv'    
        block_path = session_dir / 'block.csv'   
        trial_path = session_dir / 'trial.csv'   

        assert event_path.exists()   
        assert block_path.exists()    
        assert trial_path.exists()       
        
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
            

        # session_key
        # retrieve rel path -> full path(to session dir)
        # Identify 3 behavior csv files assertions
        # throw error if not found (return)
        # Add in subject and session id 
        # Insert into event.BehaviorRecording()/ event.BehaviorRecording().File()
        # Only relative path in the File table
        # Insert into all remaining tables
        # trial.Block(),
        # trial.Block.Attribute(),
        # trial.TrialType(),
        # trial.Trial(),
        # trial.Trial.Attribute(),
        # trial.BlockTrial(),
        # event.EventType(),
        # event.Event(),
        # trial.TrialEvent()
        pass
        # Insertion into Part tables
        # Need to loop through all trials for trial attributes
        # Need to loop thorugh all blocks for block attributes
        # Add to large dictionary(in loop) and insert in 1 step


event.BehaviorIngestion = BehaviorIngestion
