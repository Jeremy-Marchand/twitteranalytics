import pandas as pd
import gcsfs
from google.cloud import storage
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


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
        with self.connector.open(
            f"gs://{bucket_name}/twitter_data/clean_data.csv"
        ) as f:
            df = pd.read_csv(f)
        return df


def sentiment_vader(row: pd.Series) -> pd.Series:

    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()

    sentiment_dict = sid_obj.polarity_scores(row["clean_text"])
    negative = sentiment_dict["neg"]
    neutral = sentiment_dict["neu"]
    positive = sentiment_dict["pos"]
    compound = sentiment_dict["compound"]

    if sentiment_dict["compound"] >= 0.2:
        overall_sentiment = "Positive"

    elif sentiment_dict["compound"] <= -0.2:
        overall_sentiment = "Negative"

    else:
        overall_sentiment = "Neutral"

    return row.append(
        pd.Series(
            [negative, neutral, positive, compound, overall_sentiment],
            index=["negative", "neutral", "positive", "compound", "overall_sentiment"],
        )
    )


def predicting_tweeter_sentiment(event, context) -> None:
    """
    Initiating a GCS conector and cleaning file
    """

    gcs = Gcs()
    clean_df = gcs.download_data("clean_data_twitter_bucket")
    predicted_df = clean_df.apply(sentiment_vader, axis=1)
    table_id = "wagon-bootcamp-802.my_dataset.tweet_table_normalized"
    predicted_df.to_gbq(table_id, if_exists="append")
