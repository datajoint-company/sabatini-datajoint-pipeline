import datajoint as dj
import numpy as np
from element_calcium_imaging import scan, imaging_no_curation as imaging
from workflow import db_prefix
from .core import lab, session

__all__ = ["scan", "imaging"]

# ------------- Activate "imaging" schema -------------
Session = session.Session
Equipment = lab.Equipment

imaging.activate(db_prefix + "imaging", db_prefix + "scan", linking_module=__name__)