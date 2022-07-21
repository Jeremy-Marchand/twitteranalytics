import pandas as pd
import gcsfs
from google.cloud import storage

from cloud_function_cleaning.word_transformation import (
    num_remove,
    punct_remove,
    stop_remove,
)


class Gcs:
    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.get_bucket("wagon-data-802-marchand")
        self.connector = gcsfs.GCSFileSystem(project="wagon-bootcamp-802")

    def upload_data(self, df: pd.DataFrame, name: str) -> None:
        """
        Loads data into a csv file in gcs

        Args :
            df : Dataframe to push
            name : name that will be the name of the file.csv
        """

        self.bucket.blob(f"twitter_data/{name}.csv").upload_from_string(
            df.to_csv(index=False), "text/csv"
        )


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
    Initiating a GCS conector and cleaning file
    """

    gcs = Gcs()
    with gcs.connector.open("raw_data.csv") as f:
        raw_tweets = pd.read_csv(f)
    clean_df = df_cleaning(raw_tweets)
    gcs.upload_data(clean_df, "clean_data")
