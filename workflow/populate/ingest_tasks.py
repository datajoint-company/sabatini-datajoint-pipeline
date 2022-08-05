import datajoint as dj
import re
import yaml
import numpy as np
from pathlib import Path
from .ingest_sessions import load_sessions_df
from workflow.pipeline import scan, imaging, fbe, dlc
from workflow.support import FileManifest
from workflow import db_prefix


_sessions_csv_fp = Path.cwd() / db_prefix[:-1] / "userdata/sessions.csv"
_sessions_df = load_sessions_df(_sessions_csv_fp)

# ---------- ProcessingParamSet & ProcessingTask -----------

_wf_external_inbox_path = f"{db_prefix[:-1]}/inbox"
_wf_external_outbox_path = f"{db_prefix[:-1]}/outbox"


def generate_processing_task(scan_key):
    # --- ProcessingParamSet & ProcessingTask ---
    # df['params'] is the relative path of the ops.npy file
    # When loading user analyzed data, ops.npy is overwritten with more information.
    # So make a reduced list of the initial parameters
    nd2_path = Path((scan.ScanInfo.ScanFile & scan_key).fetch("file_path", limit=1)[0])
    params_path = (nd2_path.parent / "suite2p/plane0/ops.npy").as_posix()

    params_file_query = FileManifest & {
        "remote_fullpath": f"{_wf_external_inbox_path}/{params_path}"
    }

    if len(params_file_query) == 1:
        params_file = params_file_query.fetch1("file")
    else:
        try:
            # Remove the raw data and raise an error
            imaging.find_full_path(
                imaging.get_imaging_root_data_dir(), nd2_path
            ).unlink()
        except FileNotFoundError:
            pass
        raise FileNotFoundError(f"Params file not found: {params_path}")

    params = np.load(params_file, allow_pickle=True).item()
    dict_filter = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
    initial_suite2p_keys = [
        "nplanes",
        "nchannels",
        "functional_chan",
        "tau",
        "fs",
        "do_bidiphase",
        "multiplane_parallel",
        "ignore_flyback",
        "preclassify",
        "save_mat",
        "save_NWB",
        "combined",
        "reg_tif",
        "reg_tif_chan2",
        "aspect",
        "delete_bin",
        "move_bine",
        "do_registration",
        "align_by_chan",
        "nimg_init",
        "batch_size",
        "smooth_sigma",
        "maxregshift",
        "th_badframes",
        "keep_movie_raw",
        "two_step_registration",
        "nonrigid",
        "block_size",
        "snr_thresh",
        "maxregshiftNR",
        "1Preg",
        "spatial_hp_reg",
        "pre_smooth",
        "spatial_taper",
        "roidetect",
        "denoise",
        "anatomical_only",
        "diameter",
        "spatial_scale",
        "threshold_scaling",
        "max_overlap",
        "max_iterations",
        "high_pass",
        "neuropil_extract",
        "allow_overlap",
        "inner_neuropil_radius",
        "min_neuropil_pixels",
        "soma_crop",
        "spikedetect",
        "win_baseline",
        "sig_baseline",
        "neucoeff",
        "input_format",
        "look_one_level_down",
    ]
    reduced_params = dict_filter(params, initial_suite2p_keys)

    try:
        paramset_idx = (
            dj.U().aggr(imaging.ProcessingParamSet, n="max(paramset_idx)").fetch1("n")
            or 0
        ) + 1
        imaging.ProcessingParamSet.insert_new_params(
            paramset_idx=paramset_idx,
            processing_method="suite2p",
            params=reduced_params,
            paramset_desc="",
        )
    except dj.DataJointError as e:
        emsg = str(e)
        assert "The specified param-set already exists" in emsg
        paramset_idx = int(emsg.split(": ")[-1])

    processing_output_dir = nd2_path.parent
    imaging.ProcessingTask.insert1(
        {
            **scan_key,
            "paramset_idx": paramset_idx,
            "processing_output_dir": processing_output_dir,
            "task_mode": "load",
        },
        skip_duplicates=True,
    )


def ingest_model():
    # This is by using FileManifest
    dlc_paths = (FileManifest & 'remote_fullpath LIKE "%dlc_projects%"').fetch(
        "remote_fullpath"
    )

    project_paths = np.unique(
        ["/".join(dlc_path.split("/")[:4]) for dlc_path in dlc_paths]
    )

    for project_path in project_paths:
        # Fetch config.yaml
        config_file_path = f"{project_path}/config.yaml"
        file = (FileManifest & f'remote_fullpath="{config_file_path}"').fetch1("file")

        train_files = (
            FileManifest & f'remote_fullpath LIKE "%{project_path}%/train/%"'
        ).fetch("file")

        with open(file, "rb") as f:
            dlc_config = yaml.safe_load(f)

        # Modify the project path and save it to the config.yaml file
        dlc_config["project_path"] = (
            dlc.get_dlc_root_data_dir() / "dlc_projects" / file.split("/")[-2]
        ).as_posix()

        with open(file, "w") as f:
            yaml.dump(dlc_config, f)

        # Determine the shuffle & trainingsetindex from the file path
        # There can be multiple iterations
        sample_paths = (
            FileManifest
            & f'remote_fullpath LIKE "%{project_path}/dlc-models%trainset%shuffle%"'
        ).fetch("remote_fullpath")
        iterations = [x for x in sample_paths[0].split("/") if "iteration" in x]

        for iteration in iterations:
            sample_path = next(x for x in sample_paths if iteration in x)
            trainsetshuffle_piece = next(
                x for x in sample_path.split("/") if "trainset" in x
            )

            trainset, shuffle = re.search(
                r"trainset(\d+)shuffle(\d+)", trainsetshuffle_piece
            ).groups()

            model_name = project_path.split("/")[-1] + f"_{iteration}"
            model_description = ", ".join(
                "/".join(x.replace("\\", "/").split("/")[-2:])
                for x in dlc_config["video_sets"].keys()
            )
            dlc.model.Model.insert_new_model(
                model_name=model_name,
                dlc_config=dlc_config,
                shuffle=int(shuffle),
                trainingsetindex=np.argmin(
                    np.abs(
                        np.array(dlc_config["TrainingFraction"]) - float(trainset) / 100
                    )
                ),
                prompt=False,
                model_description=model_description,
            )


def generate_poseestimation_task(video_key):
    video_file = (
        (dlc.model.VideoRecording.File & video_key).fetch1("file_path")
    ).split("/")[-1]

    model_key = (dlc.model.Model & f'model_description LIKE "%{video_file}%"').fetch(
        "KEY", limit=1
    )[0]

    (FileManifest & f'remote_fullpath LIKE "%{video_file}%"').fetch(
        "file"
    )  # Fetch the file to infer the output path

    dlc.model.PoseEstimationTask.insert_estimation_task({**model_key, **video_key})
