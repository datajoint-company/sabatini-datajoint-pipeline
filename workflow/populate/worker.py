import datajoint as dj
from datajoint_utilities.dj_worker import DataJointWorker, WorkerLog, ErrorLog
from workflow import db_prefix
from workflow.pipeline import session, ephys, scan, event, imaging, model as dlc_model, train as dlc_train
from workflow.support import ephys_support, imaging_support, dlc_model_support

logger = dj.logger

__all__ = ["standard_worker", "dlc_worker", "calcium_imaging_worker", \
     "spike_sorting_worker", "WorkerLog",  "ErrorLog"]

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

standard_worker(event.BehaviorIngestion, max_calls=5)

standard_worker(ephys_support.PreProbeInsertion)
standard_worker(ephys_support.PreProbeInsertion.clean_up)

standard_worker(ephys_support.PreEphysRecording)
standard_worker(ephys.EphysRecording, max_calls=10)
standard_worker(ephys_support.PreEphysRecording.clean_up)


standard_worker(ephys_support.PreCuratedClustering)
standard_worker(ephys.CuratedClustering, max_calls=10)
standard_worker(ephys_support.PreCuratedClustering.clean_up)

standard_worker(ephys_support.PreWaveformSet)
standard_worker(ephys.WaveformSet, max_calls=1)
standard_worker(ephys_support.PreWaveformSet.clean_up)

standard_worker(ephys_support.PreLFP)
standard_worker(ephys.LFP, max_calls=1)
standard_worker(ephys_support.PreLFP.clean_up)

# spike_sorting process for GPU required jobs
spike_sorting_worker = DataJointWorker('spike_sorting_worker',
                                       worker_schema_name,
                                       db_prefix=[db_prefix, worker_db_prefix],
                                       run_duration=-1,
                                       sleep_duration=30,
                                       autoclear_error_patterns=autoclear_error_patterns)

spike_sorting_worker(ephys.Clustering, max_calls=6)

# Imaging analyses run by standard worker

standard_worker(imaging_support.PreScanInfo, max_calls=1)
standard_worker(scan.ScanInfo, max_calls=5)
standard_worker(imaging_support.PreScanInfo.clean_up)


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

dlc_worker(dlc_model_support.PreRecordingInfo, max_calls=5)
dlc_worker(dlc_model.RecordingInfo, max_calls=5)
dlc_worker(dlc_model_support.PreRecordingInfo.clean_up)

dlc_worker(dlc_model_support.PrePoseEstimation, max_calls=5)
dlc_worker(dlc_model.PoseEstimation, max_calls=5)
dlc_worker(dlc_model_support.PostPoseEstimation, max_calls=5)
dlc_worker(dlc_model_support.PrePoseEstimation.clean_up)
