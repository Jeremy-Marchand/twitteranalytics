import pandas as pd
import gcsfs

from cloud_function_cleaning.word_transformation import (
    num_remove,
    punct_remove,
    stop_remove,
)


class GcsFile:
    def __init__(self):
        self.connector = gcsfs.GCSFileSystem(project="wagon-bootcamp-802")


def df_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Overall cleansing of the Dataframe
    """
    # Removing RT mentions
    df["text"] = df["text"].str.replace(r"RT @\S* ", "")
    df["text"] = df["text"].str.replace(r"@\S* ", "")
    df["text"] = df["text"].str.replace(r"http\S*", "")
    # Removing ponctuation
    df["text"] = df["text"].apply(punct_remove)
    df["text"] = df["text"].apply(lambda row: row.lower())
    # Removing numerical values
    df["text"] = df["text"].apply(num_remove)
    # removing stop words
    df["text"] = df["text"].apply(stop_remove)
    df["text"] = df["text"].str.replace(r" f ", " f1 ")
    return df


def cleaning_file() -> None:
    """
    Initiating a GCS conector,
    """

    gcs = GcsFile()
    with gcs.connector.open("raw_data.csv") as f:
        raw_tweets = pd.read_csv(f)
    df = df_cleaning(raw_tweets)
    df.shape
