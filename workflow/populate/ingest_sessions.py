import pandas as pd
from pathlib import Path
from datetime import datetime
from workflow.pipeline import lab, subject, session, scan, model


### IMPORTANT NOTE
### Rows of the sessions.csv should not be shuffled.
### Any new entry should be added from the bottom to NOT alter some of the primary key ids (eg. VideoRecording.File.file_id)


def ingest_subjects(file="subjects.csv"):
    df = pd.read_csv(file, parse_dates=["subject_birth_date"])
    df["subject_birth_date"] = df["subject_birth_date"].dt.strftime("%Y-%m-%d")

    print(f"\n---- Insert subjects into subject.Subject ----")
    subject.Subject.insert(df, skip_duplicates=True)

    print("\n---- Successfully completed ingest_subjects ----")


def load_sessions_df(file="sessions.csv"):
    # Read the table and fix the datetime values
    df = pd.read_csv(file, parse_dates=["session_datetime"])
    df["session_datetime"] = df["session_datetime"].apply(
        lambda x: datetime.strftime(x, "%Y-%m-%d")
    )

    # Remove unwanted rows
    if "remove" in df.columns:
        df = df[df.remove != 1].drop("remove", axis=1).reset_index(drop=True)

    # Create session_id by target labeling aws_nd2_path
    # This should ensure consistency when new data is added
    df = df.sort_values(["subject", "session_datetime"]).reset_index(drop=True)
    # root_dir/subject/session/scan_no/x.nd2 - > root_dir/subject/session

    # --- SessionDirectory --
    # Construct the session_dir
    df["session_dir"] = df["aws_nd2_path"].apply(
        lambda x: Path(x).relative_to("sciops-dev_sabatini/inbox").parent.parent.as_posix()
    )

    # --- Scan ---
    # Create the parameters
    df["scan_id"] = df.groupby("session_dir")["aws_nd2_path"].cumcount().astype(str)

    return df


def ingest_sessions_and_scans(file="sessions.csv"):
    # Read the table and fix the datetime values
    df = load_sessions_df(file)

    # --- Session ---
    session.Session.insert(
        df[["subject", "session_id", "session_datetime"]], skip_duplicates=True
    )

    session.SessionDirectory.insert(
        df[["subject", "session_id", "session_dir"]], skip_duplicates=True
    )

    # --- Scan ---
    # Create the parameters
    df["scanner"] = lab.Equipment.fetch1("scanner")
    df["acq_software"] = "NIS"  # By default for Indiana

    scan.Scan.insert(
        df[["subject", "session_id", "scan_id", "scanner", "acq_software"]],
        skip_duplicates=True,
    )


def ingest_behavior_videos(file="sessions.csv"):
    # Read the table and fix the datetime values
    df = load_sessions_df(file)

    # --- Videos ---
    # Drop the rows with no aws_behavior path
    vid_recs = []
    vid_recs_files = []
    for _, data in df.iterrows():
        if data["aws_behavior_path"]:
            vid_recs.append(
                dict(
                    subject=data["subject"],
                    session_id=data["session_id"],
                    recording_id=0,
                    device_id=1,
                )
            )

            vid_recs_files.append(
                dict(
                    subject=data["subject"],
                    session_id=data["session_id"],
                    recording_id=0,
                    file_id=0,
                    file_path="/".join(data["aws_behavior_path"].split("/")[2:]),
                )
            )

    model.VideoRecording.insert(vid_recs, skip_duplicates=True)
    model.VideoRecording.File.insert(vid_recs_files, skip_duplicates=True)
