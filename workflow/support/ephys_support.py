import os
import datajoint as dj

from workflow import db_prefix
from workflow.utils.paths import get_ephys_root_data_dir
from workflow.pipeline import session, ephys, probe

__all__ = ['ephys_support', 'FileManifest']

# ------------- Configure the "support-pipeline" -------------
from element_array_ephys_support import ephys_support

org_name, workflow_name, _ = db_prefix.split('_')
org_vm = dj.create_virtual_module('org_vm', f'{org_name}_admin_workflow')

dj.config["stores"] = {
    "data-root": dict(
        protocol="s3",
        endpoint="s3.amazonaws.com:9000",
        bucket='dj-sciops',
        location=f'{db_prefix[:-1]}',
        access_key=os.getenv("AWS_ACCESS_KEY", None),
        secret_key=os.getenv("AWS_ACCESS_SECRET", None),
        stage=get_ephys_root_data_dir().parent,
    ),
}

FileManifest = org_vm.FileManifest
ephys.Session = session.Session

if not ephys_support.schema.is_activated():
    ephys_support.activate(f'{org_name}_support_{workflow_name}_' + "ephys_support", linking_module=__name__)

# rework key_source

ephys.EphysRecording.key_source = ephys.EphysRecording.key_source & ephys_support.PreEphysRecording
ephys.LFP.key_source = ephys.LFP.key_source & ephys_support.PreLFP
ephys.Clustering.key_source = ephys.Clustering.key_source & ephys_support.PreClustering
ephys.CuratedClustering.key_source = ephys.CuratedClustering.key_source & ephys_support.PreCuratedClustering
ephys.WaveformSet.key_source = ephys.WaveformSet.key_source & ephys_support.PreWaveformSet
