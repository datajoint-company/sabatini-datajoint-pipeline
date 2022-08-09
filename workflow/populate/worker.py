import datajoint as dj
from datajoint_utilities.dj_worker import DataJointWorker, WorkerLog, ErrorLog
from workflow import db_prefix
from workflow.pipeline import session, ephys, scan, imaging, model as dlc_model, train as dlc_train
<<<<<<< HEAD
from workflow.support import ephys_support, imaging_support, facemap_support, dlc_model_support
from .ingest_tasks import generate_processing_task
=======
>>>>>>> upstream-sciops-dev

logger = dj.logger

__all__ = ["standard_worker", "dlc_worker", "calcium_imaging_worker", \
<<<<<<< HEAD
     "spike_sorting_worker", "WorkerLog",  "ErrorLog"]
=======
     "spike_sorting_worker", "WorkerLog", "ErrorLog"]
>>>>>>> upstream-sciops-dev

def auto_generate_probe_insertions():
    for skey in (session.Session - ephys.ProbeInsertion).fetch('KEY'):
        try:
            logger.debug(f"Making {skey} -> {ephys.ProbeInsertion.full_table_name}")
            ephys.ProbeInsertion.auto_generate_entries(skey)
            logger.debug(f"Success making {skey} -> {ephys.ProbeInsertion.full_table_name}")
        except Exception as error:
            logger.debug(f"Error making {skey} -> {ephys.ProbeInsertion.full_table_name} - {str(error)}")
            ErrorLog.log_exception(skey, ephys.ProbeInsertion.auto_generate_entries, error)

def auto_generate_clustering_tasks():
    for rkey in (ephys.EphysRecording - ephys.ClusteringTask).fetch('KEY'):
        try:
            ephys.ClusteringTask.auto_generate_entries(rkey, paramset_idx=0)
        except Exception as error:
            logger.error(str(error))
            ErrorLog.log_exception(rkey, ephys.ClusteringTask.auto_generate_entries, error)

<<<<<<< HEAD
def auto_generate_processing_tasks():
    for scan_key in (scan.ScanInfo - imaging.ProcessingTask).fetch("KEY"):
        try:
            logger.debug(
                f"Making {scan_key} -> {imaging.ProcessingTask.full_table_name}"
            )
            generate_processing_task(scan_key)
        except Exception as error:
            logger.debug(
                f"Error making {scan_key} -> {imaging.ProcessingTask.full_table_name} - {str(error)}"
            )
            ErrorLog.log_exception(scan_key, generate_processing_task, error)
        else:
            logger.debug(
                f"Success making {scan_key} -> {imaging.ProcessingTask.full_table_name}"
            )


=======
>>>>>>> upstream-sciops-dev
# -------- Define process(s) --------
org_name, workflow_name, _ = db_prefix.split("_")

worker_db_prefix = f"{org_name}_support_{workflow_name}_"
worker_schema_name = worker_db_prefix + "workerlog"
autoclear_error_patterns = ["%FileNotFound%"]

# standard process for non-GPU jobs 
standard_worker = DataJointWorker('standard_worker',
                                  worker_schema_name,
                                  db_prefix=[db_prefix, worker_db_prefix],
                                  run_duration=-1,
                                  sleep_duration=30,
                                  autoclear_error_patterns=autoclear_error_patterns)

<<<<<<< HEAD
standard_worker(ephys_support.PreProbeInsertion)
standard_worker(auto_generate_probe_insertions)
standard_worker(ephys_support.PreProbeInsertion.clean_up)

standard_worker(ephys_support.PreEphysRecording)
standard_worker(ephys.EphysRecording, max_calls=10)
standard_worker(ephys_support.PreEphysRecording.clean_up)

standard_worker(auto_generate_clustering_tasks)

standard_worker(ephys_support.PreCuratedClustering)
standard_worker(ephys.CuratedClustering, max_calls=10)
standard_worker(ephys_support.PreCuratedClustering.clean_up)

standard_worker(ephys_support.PreWaveformSet)
standard_worker(ephys.WaveformSet, max_calls=1)
standard_worker(ephys_support.PreWaveformSet.clean_up)

standard_worker(ephys_support.PreLFP)
standard_worker(ephys.LFP, max_calls=1)
standard_worker(ephys_support.PreLFP.clean_up)
=======
standard_worker(auto_generate_probe_insertions)

standard_worker(ephys.EphysRecording, max_calls=5)

standard_worker(auto_generate_clustering_tasks)

standard_worker(ephys.CuratedClustering, max_calls=5)

standard_worker(ephys.WaveformSet, max_calls=5)

standard_worker(ephys.LFP, max_calls=5)

>>>>>>> upstream-sciops-dev

# spike_sorting process for GPU required jobs
spike_sorting_worker = DataJointWorker('spike_sorting_worker',
                                       worker_schema_name,
                                       db_prefix=[db_prefix, worker_db_prefix],
                                       run_duration=-1,
                                       sleep_duration=30,
                                       autoclear_error_patterns=autoclear_error_patterns)

spike_sorting_worker(ephys.Clustering, max_calls=6)

<<<<<<< HEAD
# Imaging analyses run by standard worker

standard_worker(imaging_support.PreScanInfo, max_calls=1)
standard_worker(scan.ScanInfo, max_calls=5)
standard_worker(imaging_support.PreScanInfo.clean_up)

standard_worker(auto_generate_processing_tasks)

standard_worker(imaging_support.PreMotionCorrection, max_calls=2)
standard_worker(imaging.MotionCorrection, max_calls=5)
standard_worker(imaging_support.PreMotionCorrection.clean_up)

standard_worker(imaging_support.PreSegmentation, max_calls=2)
standard_worker(imaging.Segmentation, max_calls=5)
standard_worker(imaging_support.PreSegmentation.clean_up)

standard_worker(imaging_support.PreFluorescence, max_calls=2)
standard_worker(imaging.Fluorescence, max_calls=5)
standard_worker(imaging_support.PreFluorescence.clean_up)

standard_worker(imaging_support.PreActivity, max_calls=3)
standard_worker(imaging.Activity, max_calls=5)
standard_worker(imaging_support.PreActivity.clean_up)


# calcum imaging worker
=======
# imaging
standard_worker(scan.ScanInfo, max_calls=5)

standard_worker(imaging.MotionCorrection, max_calls=5)

standard_worker(imaging.Segmentation, max_calls=5)

standard_worker(imaging.Fluorescence, max_calls=5)

standard_worker(imaging.Activity, max_calls=5)

# analysis worker
>>>>>>> upstream-sciops-dev
calcium_imaging_worker = DataJointWorker(
    "calcium_imaging_worker",
    worker_schema_name,
    db_prefix=[db_prefix, worker_db_prefix],
    run_duration=-1,
    sleep_duration=30,
    autoclear_error_patterns=autoclear_error_patterns,
)

calcium_imaging_worker(imaging.Processing, max_calls=5)

# --- Deeplabcut ---

# GPU worker for DLC
dlc_worker = DataJointWorker(
    "dlc_worker",
    worker_schema_name,
    db_prefix=[db_prefix, worker_db_prefix],
    run_duration=-1,
    sleep_duration=30,
    autoclear_error_patterns=autoclear_error_patterns,
)
dlc_worker(dlc_model.RecordingInfo, max_calls=5)
dlc_worker(dlc_model.PoseEstimation, max_calls=5)
