from workflow import db_prefix
from workflow.pipeline import event, session
from workflow.utils.paths import get_raw_root_data_dir
from .file_manifest import FileManifest, support_db_prefix
import datajoint as dj

 global _wf_external_inbox_path, _wf_external_outbox_path
    wfs_full_name = "_".join(event.schema.database.split("_")[:2])
    _wf_external_inbox_path = f"{wfs_full_name}/inbox"
    _wf_external_outbox_path = f"{wfs_full_name}/outbox"

__all__ = ["event_support"]

schema = dj.schema(support_db_prefix + "event_support")

@schema
class PreBehaviorIngestion()
    defintion = """
    -> session.Session
    """
    class File(dj.Part):
        definition = """
        -> master
        -> FileManifest
        """
        
    @classmethod
    def clean_up(cls):
        _clean_up(cls, event.BehaviorIngestion)

    def make(self, key):
    """
    Download all availible behavioral data files 
        + event.csv
        + block.csv
        + trial.csv
    """
    session_dir = (session.SessionDirectory & key).fetch1("session_dir")
    session_full_dir = find_full_path(get_raw_root_data_dir(), session_dir)

    beh_files = ['events.csv','trial.csv','block.csv']



    file_keys, files = (
            FileManifest
            & f"remote_fullpath LIKE '%{session_full_dir}/Behavior/{f}%'"
            for f in beh_files
            & key
        ).fetch("KEY", "file")

    if file_keys:
        self.insert1(key)
        self.File.insert([{**key, **file_key}] for file_key in file_keys)

event_support = PreBehaviorIngestion

def _clean_up(prepare_table, populate_table):
    """
    Entries created in the "prepare" table but do not have a corresponding ones in the "populate" table
        represent unsuccessful `.populate()` - remove entries here and try again
    """
    unsuccessful_entries = prepare_table - populate_table
    with dj.config(safemode=False):
        (prepare_table & unsuccessful_entries.fetch("KEY")).delete()
