import os
import datajoint as dj

from workflow import db_prefix
from workflow.utils.paths import get_imaging_root_data_dir

__all__ = ['FileManifest', 'support_db_prefix']

# ------------- Configure the "support-pipeline" -------------
org_name, workflow_name, _ = db_prefix.split('_')
org_vm = dj.create_virtual_module("org_vm", f"{org_name}_admin_workflow")
support_db_prefix = f'{org_name}_support_{workflow_name}_'

dj.config["stores"] = {
    "data-root": dict(
        protocol="s3",
        endpoint="s3.amazonaws.com:9000",
        bucket="dj-sciops",
        location=f"{db_prefix[:-1]}",
        access_key=os.getenv("AWS_ACCESS_KEY", None),
        secret_key=os.getenv("AWS_ACCESS_SECRET", None),
        stage=get_imaging_root_data_dir().parent,
    ),
}

FileManifest = org_vm.FileManifest
