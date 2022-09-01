from workflow import db_prefix
from workflow.pipeline import event, session
from workflow.utils.paths import get_raw_root_data_dir
from element_interface.utils import find_full_path
from .file_manifest import FileManifest, support_db_prefix
import datajoint as dj

wfs_full_name = "_".join(event.schema.database.split("_")[:2])
_wf_external_inbox_path = f"{wfs_full_name}/inbox"
_wf_external_outbox_path = f"{wfs_full_name}/outbox"

schema = dj.schema(support_db_prefix + "event_support")
@schema
class PreBehaviorIngestion(dj.Imported):
    definition = """
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

    key_source = session.Session & session.SessionDirectory

    def make(self, key):
        """
        Download all availible behavioral data files 
            + events.csv
            + block.csv
            + trial.csv
        """
        session_dir = (session.SessionDirectory & key).fetch1("session_dir")

        file_keys, files = (
                FileManifest
                & [f"remote_fullpath LIKE '{_wf_external_inbox_path}%{session_dir}/Behavior/{f}'"
                for f in ('events.csv','trial.csv','block.csv')]
            ).fetch("KEY", "file")

        if file_keys:
            self.insert1(key)
            self.File.insert([{**key, **file_key} for file_key in file_keys])

def _clean_up(prepare_table, populate_table):
    """
    Entries created in the "prepare" table but do not have a corresponding ones in the "populate" table
        represent unsuccessful `.populate()` - remove entries here and try again
    """
    unsuccessful_entries = prepare_table - populate_table
    with dj.config(safemode=False):
        (prepare_table & unsuccessful_entries.fetch("KEY")).delete()
