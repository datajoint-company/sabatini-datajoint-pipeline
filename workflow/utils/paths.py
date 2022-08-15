import datajoint as dj
import pathlib


def get_ephys_root_data_dir():
    data_dir = dj.config.get('custom', {}).get('ephys_root_data_dir', None)
    return pathlib.Path(data_dir) if data_dir else None


def get_ephys_processed_root_data_dir():
    data_dir = dj.config.get('custom', {}).get('ephys_processed_data_dir', None)
    return pathlib.Path(data_dir) if data_dir else None


def get_ephys_session_directory(session_key: dict) -> str:
    data_dir = get_ephys_root_data_dir()

    from workflow.pipeline import session
    if not (session.SessionDirectory & session_key):
        raise FileNotFoundError(f'No session data directory defined for {session_key}')

    sess_dir = data_dir / (session.SessionDirectory & session_key).fetch1('session_dir')

    return sess_dir.as_posix()

def get_imaging_root_data_dir():
    data_dir = dj.config.get("custom", {}).get("imaging_root_data_dir")
    return pathlib.Path(data_dir) if data_dir else None


def get_imaging_processed_root_data_dir():
    data_dir = dj.config.get("custom", {}).get("imaging_processed_data_dir", None)
    return pathlib.Path(data_dir) if data_dir else None


def get_scan_image_files(scan_key):
    # Folder structure: root / subject / session / .tif (raw)
    data_dir = get_imaging_root_data_dir()

    from workflow.pipeline import session

    sess_dir = data_dir / (session.SessionDirectory & scan_key).fetch1("session_dir")

    if not sess_dir.exists():
        raise FileNotFoundError(f"Session directory not found ({sess_dir})")

    tiff_filepaths = [fp.as_posix() for fp in sess_dir.glob("*.tif")]
    if tiff_filepaths:
        return tiff_filepaths
    else:
        raise FileNotFoundError(f"No tiff file found in {sess_dir}")


def get_scan_box_files(scan_key):
    raise NotImplementedError


def get_nd2_files(scan_key):
    # Folder structure: root / subject / session_id / scan_no /.nd2
    data_dir = get_imaging_root_data_dir()

    from workflow.pipeline import session

    sess_dir = data_dir / (session.SessionDirectory & scan_key).fetch1("session_dir")

    if not sess_dir.exists():
        raise FileNotFoundError(f"Session directory not found ({sess_dir})")

    # In this workflow, there is always one scan per session: "scan_0"
    nd2_filepaths = [fp.as_posix() for fp in (sess_dir / "scan_0").glob("*.nd2")]
    if nd2_filepaths:
        return nd2_filepaths
    else:
        raise FileNotFoundError(f"No .nd2 file found in {sess_dir}")


def get_dlc_root_data_dir():
    return get_imaging_root_data_dir()

def get_dlc_processed_data_dir():
    return get_imaging_processed_root_data_dir()