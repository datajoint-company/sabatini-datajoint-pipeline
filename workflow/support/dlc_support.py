from workflow import db_prefix
from workflow.pipeline import dlc

from .file_manifest import FileManifest, support_db_prefix

__all__ = ["dlc_model_support"]

# ---- support pipeline for deeplabcut ----
from element_deeplabcut_support import dlc_model_support

# rework key_source
model = dlc.model
dlc_model_support.activate(
    support_db_prefix + "dlc_model_support", linking_module=__name__
)

dlc.model.RecordingInfo.key_source = (
    dlc.model.RecordingInfo.key_source & dlc_model_support.PreRecordingInfo
)
dlc.model.PoseEstimation.key_source = (
    dlc.model.PoseEstimation.key_source & dlc_model_support.PrePoseEstimation
)
