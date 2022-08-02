import pandas as pd
import gcsfs
from google.cloud import storage

from word_transformation import (
    num_remove,
    punct_remove,
    stop_remove,
)


class Gcs:
    def __init__(self) -> None:
        self.client = storage.Client()
        self.connector = gcsfs.GCSFileSystem(project="wagon-bootcamp-802")

    def upload_data(self, bucket_name: str, df: pd.DataFrame, name: str) -> None:
        """
        Loads data into a csv file in gcs

        Args :
            df : Dataframe to push
            name : name that will be the name of the file.csv
        """
        self.bucket = self.client.get_bucket(bucket_name)
        self.bucket.blob(f"twitter_data/{name}.csv").upload_from_string(
            df.to_csv(index=False), "text/csv"
        )

    def download_data(self, bucket_name: str) -> pd.DataFrame:
        with self.connector.open(f"gs://{bucket_name}/twitter_data/raw_data.csv") as f:
            raw_df = pd.read_csv(f)
        return raw_df


def df_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Overall cleansing of the Dataframe
    """
    # Removing RT and mentions
    df["clean_text"] = df["text"]
    df["clean_text"] = df["clean_text"].str.replace(r"RT @\S* ", "")
    df["clean_text"] = df["clean_text"].str.replace(r"http\S*", "")
    # Removing ponctuation
    df["clean_text"] = df["clean_text"].apply(punct_remove)
    df["clean_text"] = df["clean_text"].apply(lambda row: row.lower())
    # Removing numerical values
    df["clean_text"] = df["clean_text"].apply(num_remove)
    # removing stop words
    df["clean_text"] = df["clean_text"].apply(stop_remove)
    df["clean_text"] = df["clean_text"].str.replace(r" f ", " f1 ")
    return df


drivers_list = {
    "Hamilton": ["hamilton", "lewis"],
    "Russel": ["russel", "georges"],
    "Perez": ["perez", "checo", "sergio"],
    "Verstappen": ["verstappen", "max"],
    "Sainz": ["sainz", "carlos"],
    "Leclerc": ["leclerc", "charles"],
    "Ricciardo": ["ricciardo", "daniel"],
    "Norris": ["norris", "lando"],
    "Alonso": ["alonso", "fernando"],
    "Ocon": ["ocon", "esteban"],
    "Schumacher": ["schumacher", "mick"],
    "Magnussen": ["magnussen", "kevin"],
    "Bottas": ["bottas", "valtteri"],
    "Zhou": ["zhou", "guanyu"],
    "Gasly": ["gasly", "pierre"],
    "Tsunoda": ["tsunoda", "yuki"],
    "Stroll": ["stroll", "lance"],
    "Hulkenberg": ["hulkenberg", "nico"],
    "Albon": ["albon", "alexander"],
    "Latifi": ["latifi", "nicholas"],
}


def cleaning_file(event, context) -> None:
    """
    Initiating a GCS conector and cleaning file
    """
    # Storing clean tweets for prediction
    gcs = Gcs()
    raw_df = gcs.download_data("raw_data_twitter_bucket")
    clean_df = df_cleaning(raw_df)
    gcs.upload_data("clean_data_twitter_bucket", clean_df, "clean_data")

    # Creating the n-n table for tweet_id / driver_name
    normalized_df = pd.DataFrame({"id": [], "driver_name": []})
    for driver, names in drivers_list.items():
        driver_mask = clean_df["clean_text"].str.contains(f"{names[0]}", na=False)
        if len(names) > 1:
            for name in names[1:]:
                driver_mask = driver_mask | clean_df["clean_text"].str.contains(
                    f"{name}", na=False
                )
        temp_df = clean_df[["id"]][driver_mask]
        temp_df["driver_name"] = driver
        normalized_df = pd.concat([normalized_df, temp_df])
    normalized_df.reset_index(drop=True, inplace=True)
    normalized_df.rename(columns={"id": "tweet_id"}, inplace=True)
    normalized_df["tweet_id"] = normalized_df["tweet_id"].astype(int)

    table_normal_id = "wagon-bootcamp-802.my_dataset.normalized_table_tweet_driver"
    normalized_df.to_gbq(table_normal_id, if_exists="append")
