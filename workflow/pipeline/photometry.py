from __future__ import annotations

import datajoint as dj
import pandas as pd
import numpy as np
from pathlib import Path
import tomli
import tdt
from copy import deepcopy
import scipy.io as spio
from enum import Enum
from element_interface.utils import find_full_path
from workflow import db_prefix
from workflow.pipeline import session, subject, lab, reference
from workflow.utils.paths import get_raw_root_data_dir
import workflow.utils.photometry_preprocessing as pp
from workflow.utils import demodulation


logger = dj.logger
schema = dj.schema(db_prefix + "photometry")


@schema
class SensorProtein(dj.Lookup):
    definition = """            
    sensor_protein_name : varchar(16)  # (e.g., GCaMP, dLight, etc)
    """


@schema
class LightSource(dj.Lookup):
    definition = """
    light_source_name   : varchar(16)
    """
    contents = zip(["Plexon LED", "Laser", ""])


@schema
class ExcitationWavelength(dj.Lookup):
    definition = """
    excitation_wavelength   : smallint  # (nm)
    """


@schema
class EmissionColor(dj.Lookup):
    definition = """
    emission_color     : varchar(10) 
    ---
    wavelength=null    : smallint  # (nm)
    """


@schema
class FiberPhotometry(dj.Imported):
    definition = """
    -> session.Session
    ---
    data_format             : varchar(8)    # format of the raw data to import from (e.g., tdt, mat)
    -> [nullable] LightSource
    raw_sample_rate=null    : float         # sample rate of the raw data (in Hz) 
    beh_synch_signal=null   : longblob      # signal for behavioral synchronization from raw data
    """

    class Fiber(dj.Part):
        definition = """ 
        -> master
        fiber_id            : tinyint unsigned
        -> reference.Hemisphere
        ---
        notes=''             : varchar(1000)  
        """

    class DemodulatedTrace(dj.Part):
        definition = """ # demodulated photometry traces
        -> master.Fiber
        trace_name          : varchar(16)  # (e.g., raw, detrend)
        -> EmissionColor
        ---
        -> [nullable] SensorProtein          
        -> [nullable] ExcitationWavelength
        demod_sample_rate   : float       # sample rate of the demodulated data (in Hz) 
        trace               : longblob    # demodulated photometry traces
        """

    def make(self, key):

        logger.info(
            f'Start ingesting <FiberPhotometry> table on subject {key["subject"]} - session {key["session_id"]}'
        )

        # Find data dir
        session_dir = (session.SessionDirectory & key).fetch1("session_dir")
        session_full_dir: Path = find_full_path(get_raw_root_data_dir(), session_dir)
        photometry_dir = session_full_dir / "Photometry"

        # Read from the meta_info.toml in the photometry folder if exists
        meta_info = {}
        try:
            with open(list(photometry_dir.glob("*.toml"))[0].as_posix()) as f:
                meta_info = tomli.loads(f.read())
        except (FileNotFoundError, IndexError):
            logger.info("meta info is missing")
        light_source_name = meta_info.get("Fiber", {}).get("light_source", "")

        # Store data in this list for ingestion
        fiber_list: list[dict] = []
        demodulated_trace_list: list[dict] = []

        # Read raw data sourced from tdt
        data: tdt.StructType = tdt.read_block(photometry_dir)

        if data:

            synch_signal_names = ["toBehSys", "fromBehSys"]
            demod_sample_rate = 600

            # Demodulate & downsample raw photometry data
            # Also returns raw sample rate and list of fibers used
            data_format = "tdt"
            photometry_df, fibers, raw_sample_rate = demodulation.offline_demodulation(
                data, z=True, tau=0.05, downsample_fs=demod_sample_rate, bandpass_bw=20
            )
            del data

            # Get trace names e.g., ["detrend_grnR", "raw_grnR"]
            trace_names: list[str] = photometry_df.columns.drop(
                synch_signal_names
            ).tolist()
            trace_names = set([name[:-1] for name in trace_names])

            # Populate FiberPhotometry
            beh_synch_signal = photometry_df[synch_signal_names].to_dict("list")
            beh_synch_signal = {k: np.array(v) for k, v in beh_synch_signal.items()}

        else:
            data: list[dict] = spio.loadmat(
                next(photometry_dir.glob("*timeseries_2.mat")), simplify_cells=True
            )["timeSeries"]

            raw_sample_rate = None
            beh_synch_signal = None
            demod_sample_rate = 1 / data[0]["dt"]
            photometry_df = pd.DataFrame(data)

            data_format = "mat"
            del data

            photometry_df[["emission_color", "hemisphere"]] = photometry_df[
                "channel_name"
            ].str.split("_", expand=True)

            fibers = list(range(1, len(photometry_df["hemisphere"].unique()) + 1))

            trace_names = [
                "raw_" + s for s in photometry_df["emission_color"].unique()
            ]  # ["raw_green", "raw_red"]

        # Get photometry traces for each fiber
        for fiber_id in fibers:

            hemisphere = _Side(fiber_id).name

            fiber_notes = (
                meta_info.get("Fiber", {})
                .get("implantation", {})
                .get(hemisphere, {})
                .get("notes", "")
            )

            fiber_list.append(
                {
                    **key,
                    "fiber_id": fiber_id,
                    "hemisphere": hemisphere,
                    "notes": fiber_notes,
                }
            )

            # Populate FiberPhotometry
            for trace_name in trace_names:

                if data_format == "tdt":
                    photometry_trace = photometry_df[
                        trace_name + hemisphere[0].upper()
                    ].values
                elif data_format == "mat":
                    photometry_trace = photometry_df.query(
                        f"hemisphere == '{hemisphere}' & emission_color == '{trace_name.split('_')[1]}'"
                    )["raw_data"].values[0]

                # Populate EmissionColor if present
                emission_color = _Color(trace_name.split("_")[1][0]).name

                emission_wavelength = (
                    meta_info.get("Fiber", {})
                    .get("emission_wavelength", {})
                    .get(emission_color, None)
                )

                EmissionColor.insert1(
                    {
                        "emission_color": emission_color,
                        "wavelength": emission_wavelength,
                    },
                    skip_duplicates=True,
                )

                # Populate SensorProtein if present
                sensor_protein = (
                    meta_info.get("VirusInjection", {})
                    .get(hemisphere, {})
                    .get("sensor_protein", None)
                )

                if sensor_protein:
                    SensorProtein.insert1(
                        {"sensor_protein_name": sensor_protein}, skip_duplicates=True
                    )

                # Populate ExcitationWavelength if present
                excitation_wavelength = (
                    meta_info.get("Fiber", {})
                    .get("excitation_wavelength", {})
                    .get(emission_color, None)
                )

                if excitation_wavelength:
                    ExcitationWavelength.insert1(
                        {"excitation_wavelength": excitation_wavelength},
                        skip_duplicates=True,
                    )

                demodulated_trace_list.append(
                    {
                        **key,
                        "fiber_id": fiber_id,
                        "hemisphere": hemisphere,
                        "trace_name": trace_name.split("_")[0],
                        "emission_color": emission_color,
                        "sensor_protein_name": sensor_protein,
                        "excitation_wavelength": excitation_wavelength,
                        "demod_sample_rate": demod_sample_rate,
                        "trace": photometry_trace,
                    }
                )

        # Populate FiberPhotometry
        logger.info(f"Populate {__name__}.FiberPhotometry")
        self.insert1(
            {
                **key,
                "data_format": data_format,
                "light_source_name": light_source_name,
                "raw_sample_rate": raw_sample_rate,
                "beh_synch_signal": beh_synch_signal,
            }
        )

        # Populate FiberPhotometry.Fiber
        logger.info(f"Populate {__name__}.FiberPhotometry.Fiber")
        self.Fiber.insert(fiber_list)

        # Populate FiberPhotometry.DemodulatedTrace
        logger.info(f"Populate {__name__}.FiberPhotometry.DemodulatedTrace")
        self.DemodulatedTrace.insert(demodulated_trace_list)


@schema
class FiberPhotometrySynced(dj.Imported):
    definition = """
    -> FiberPhotometry
    ---
    timestamps   : longblob
    time_offset  : float     # time offset to synchronize the photometry traces to the master clock (in second)  
    sample_rate  : float     # target downsample rate of synced data (in Hz) 
    """

    class SyncedTrace(dj.Part):
        definition = """ # demodulated photometry traces
        -> master
        -> FiberPhotometry.Fiber
        trace_name          : varchar(16)  # (e.g., raw, detrend)
        -> EmissionColor
        ---
        trace      : longblob  
        """

    def make(self, key):

        logger.info(
            f'Start ingesting <FiberPhotometrySynced> table on subject {key["subject"]} - session {key["session_id"]}'
        )

        # Find data dir
        subject_id, session_dir = (session.SessionDirectory & key).fetch1(
            "subject", "session_dir"
        )
        session_full_dir: Path = find_full_path(get_raw_root_data_dir(), session_dir)
        behavior_dir = session_full_dir / "Behavior"
        photometry_dir = session_full_dir / "Photometry"

        # Parameters
        get_fiber_id = (
            lambda side: 1 if side.lower().startswith("r") else 2
        )  # map hemisphere to fiber id
        color_mapping = {"green": "grn"}

        data_format = (FiberPhotometry & key).fetch1("data_format")

        if data_format == "tdt":

            synch_signal_names = ["toBehSys", "fromBehSys"]
            behavior_sample_rate = 200  # original behavioral sampling freq (Hz)
            target_downsample_rate = 50  # (Hz)
            downsample_factor = behavior_sample_rate / target_downsample_rate

            # Fetch demodulated photometry traces from FiberPhotometry table
            query = (FiberPhotometry.Fiber * FiberPhotometry.DemodulatedTrace) & key

            photometry_dict = {}

            for row in query:
                trace_name = (
                    "_".join([row["trace_name"], color_mapping[row["emission_color"]]])
                    + row["hemisphere"][0].upper()
                )
                trace = row["trace"]
                photometry_dict[trace_name] = trace

            photometry_df = pd.DataFrame(
                (FiberPhotometry & key).fetch1("beh_synch_signal") | photometry_dict
            )
            # Get trace names e.g., ["detrend_grnR", "raw_grnR"]
            trace_names: list[str] = photometry_df.columns.drop(
                synch_signal_names
            ).tolist()

            # Update df to start with first trial pulse from behavior system
            photometry_df = pp.handshake_behav_recording_sys(photometry_df)

            analog_df: pd.DataFrame = pd.read_csv(
                behavior_dir / f"{subject_id}_analog_filled.csv", index_col=0
            )
            analog_df["session_clock"] = analog_df.index * 0.005

            # Resample the photometry data and align to 200 Hz state transition behavioral data (analog_df)
            behavior_df: pd.DataFrame = pd.read_csv(
                behavior_dir / f"{subject_id}_behavior_df_full.csv", index_col=0
            )

            aligned_behav_photo_df, time_offset = pp.resample_and_align(
                analog_df, photometry_df, channels=trace_names
            )
            del analog_df

            # One more rolling z-score over the window length (60s * sampling freq (200Hz))
            win = round(60 * 200)

            for channel in trace_names:
                if "detrend" in channel:
                    aligned_behav_photo_df[
                        f'z_{channel.split("_")[-1]}'
                    ] = demodulation.rolling_z(aligned_behav_photo_df[channel], wn=win)
            aligned_behav_photo_df = aligned_behav_photo_df.iloc[win:-win].reset_index(
                drop=True
            )  # drop edges that now contain NaNs from rolling window

            # Drop unnecessary columns that we don't need to save
            photo_columns = trace_names + [
                f'z_{channel.split("_")[-1]}' for channel in trace_names[::3]
            ]  # trace_names[::((len(trace_names)//2)+1)]]]

            cols_to_keep = [
                "nTrial",
                "iBlock",
                "Cue",
                "ENL",
                "Select",
                "Consumption",
                "iSpout",
                "stateConsumption",
                "ENLP",
                "CueP",
                "nENL",
                "nCue",
                "session_clock",
            ]
            cols_to_keep.extend(photo_columns)

            timeseries_task_states_df: pd.DataFrame = deepcopy(
                aligned_behav_photo_df[cols_to_keep]
            )
            timeseries_task_states_df["trial_clock"] = (
                timeseries_task_states_df.groupby("nTrial").cumcount() * 5 / 1000
            )

            # This has to happen AFTER alignment between photometry and behavior because first ENL triggers sync pulse
            _split_penalty_states(
                timeseries_task_states_df, behavior_df, penalty="ENLP"
            )
            _split_penalty_states(
                timeseries_task_states_df, behavior_df, penalty="CueP"
            )

            n_bins, remainder = divmod(
                len(timeseries_task_states_df), downsample_factor
            )  # get number of bins to downsample into
            bin_ids = [
                j for i in range(int(n_bins)) for j in np.repeat(i, downsample_factor)
            ]  # define ids of bins at downsampling rate [1,1,1,1,2,2,2,2,...]
            bin_ids.extend(
                np.repeat(bin_ids[-1] + 1, remainder)
            )  # tag on incomplete bin at end
            timeseries_task_states_df[
                "bin_ids"
            ] = bin_ids  # new column to label new bin_ids

            downsampled_states_df: pd.DataFrame = deepcopy(timeseries_task_states_df)

            # Apply aggregate function to each column
            col_fcns = {
                col: np.max
                for col in downsampled_states_df.columns
                if col not in photo_columns
            }
            [col_fcns.update({col: np.mean}) for col in photo_columns]

            # Handle penalties. Label preceding states as different from those without penalties
            downsampled_states_df = downsampled_states_df.groupby("bin_ids").agg(
                col_fcns
            )
            downsampled_states_df = downsampled_states_df.reset_index(drop=True)
            downsampled_states_df = downsampled_states_df.drop(columns=["bin_ids"])

            timestamps = downsampled_states_df["session_clock"].values

            trace_names = list(downsampled_states_df.columns[-6:])

        elif data_format == "mat":

            data: list[dict] = spio.loadmat(
                next(photometry_dir.glob("*timeseries_2.mat")), simplify_cells=True
            )["timeSeries"]

            time_offset = data[0]["time_offset"]
            target_downsample_rate = 1 / data[0]["dt"]
            timestamps = (
                np.arange(0, len(data[0]["processed_data"])) * data[0]["dt"]
                + time_offset
            )

            photometry_df = pd.DataFrame(data)

            photometry_df[["emission_color", "hemisphere"]] = photometry_df[
                "channel_name"
            ].str.split("_", expand=True)

            trace_names = [
                "processed_" + c + s.upper()[0]
                for c, s in zip(
                    photometry_df["emission_color"].unique(),
                    photometry_df["hemisphere"].unique(),
                )
            ]  # ["raw_greenL", "raw_redR"]

            del data

        # Populate FiberPhotometrySynced
        logger.info(f"Populate {__name__}.FiberPhotometrySynced")
        self.insert1(
            {
                **key,
                "timestamps": timestamps,
                "time_offset": time_offset,
                "sample_rate": target_downsample_rate,
            }
        )

        # Populate FiberPhotometry
        synced_trace_list: list[dict] = []

        for trace_name in trace_names:

            hemisphere = {"R": "right", "L": "left"}[trace_name[-1]]
            emission_color = _Color(trace_name.split("_")[1][0].lower()).name

            if data_format == "tdt":
                photometry_trace = downsampled_states_df[trace_name].values
            elif data_format == "mat":
                photometry_trace = photometry_df.query(
                    f"hemisphere == '{hemisphere}' & emission_color == '{emission_color}'"
                )["processed_data"].values[0]

            synced_trace_list.append(
                {
                    **key,
                    "fiber_id": get_fiber_id(trace_name[-1]),
                    "hemisphere": hemisphere,
                    "trace_name": trace_name.split("_")[0],
                    "emission_color": emission_color,
                    "trace": photometry_trace,
                }
            )

        logger.info(f"Populate {__name__}.FiberPhotometrySynced.SyncedTrace")
        self.SyncedTrace.insert(synced_trace_list)


def _split_penalty_states(
    df: pd.DataFrame, behavior_df: pd.DataFrame, penalty: str = "ENLP"
) -> None:
    """Handle penalties. Label preceding states as different from those without penalties"""
    penalty_trials = df.loc[df[penalty] == 1].nTrial.unique()

    if len(penalty_trials) > 1:
        penalty_groups = df.loc[df.nTrial.isin(penalty_trials)].groupby(
            "nTrial", as_index=False
        )

        mask = penalty_groups.apply(
            lambda x: x[f"n{penalty[:-1]}"]
            < behavior_df.loc[behavior_df.nTrial == x.nTrial.iloc[0].squeeze()][
                f"n_{penalty[:-1]}"
            ].squeeze()
        )

    else:
        mask = (
            df.loc[df.nTrial.isin(penalty_trials), f"n{penalty[:-1]}"]
            < behavior_df.loc[behavior_df.nTrial.isin(penalty_trials)][
                f"n_{penalty[:-1]}"
            ].squeeze()
        )

    # Label pre-penalty states as penalties
    df[f"state_{penalty}"] = 0
    df.loc[df.nTrial.isin(penalty_trials), f"state_{penalty}"] = (
        mask.values * df.loc[df.nTrial.isin(penalty_trials), f"{penalty[:-1]}"]
    )

    # Remove pre-penalty states from true states
    df.loc[df.nTrial.isin(penalty_trials), f"{penalty[:-1]}"] = (
        1 - mask.values
    ) * df.loc[df.nTrial.isin(penalty_trials), f"{penalty[:-1]}"]


class _Side(Enum):
    right = 1
    left = 2


class _Color(Enum):
    green = "g"
    red = "r"
    blue = "b"
