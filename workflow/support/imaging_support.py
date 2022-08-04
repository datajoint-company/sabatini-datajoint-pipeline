from workflow.pipeline import scan, imaging

from .file_manifest import FileManifest, support_db_prefix

__all__ = ['imaging_support']

# ---- support pipeline for calcium-imaging ----
from element_calcium_imaging_support import imaging_support

imaging_support.activate(support_db_prefix + "imaging_support", linking_module=__name__)

# rework key_source
scan.ScanInfo.key_source = scan.ScanInfo.key_source & imaging_support.PreScanInfo
imaging.Processing.key_source = imaging.Processing.key_source & imaging_support.PreProcessing
imaging.MotionCorrection.key_source = (
    imaging.MotionCorrection.key_source & imaging_support.PreMotionCorrection
)
imaging.Segmentation.key_source = (
    imaging.Segmentation.key_source & imaging_support.PreSegmentation
)
imaging.Fluorescence.key_source = (
    imaging.Fluorescence.key_source & imaging_support.PreFluorescence
)
imaging.Activity.key_source = imaging.Activity.key_source & imaging_support.PreActivity
