import os
from typing import Dict
import requests
import json

# from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import pandas as pd
import logging
from typing import TypedDict, Optional, Union
from typing_extensions import NotRequired
from google.cloud import firestore, storage
from jsonschema import validate

search_url = "https://api.twitter.com/2/tweets/search/recent"
# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields


def get_token():
    token = os.environ.get("BEARER_TOKEN", "Key missing in env settings")
    return token


class FirestoreLastDate:
    def __init__(self) -> None:
        self.db = firestore.Client(project="wagon-bootcamp-802")
        self.doc_ref = self.db.collection("twitter_dates").document("last_date")

    def update_last_date(self, last_date_db: str) -> None:
        self.doc_ref.set({"last_date_db": last_date_db})  # type: ignore

    def read_last_date(self):
        last_date = self.doc_ref.get().to_dict()  # type: ignore
        if last_date:
            return last_date.get("last_date_db")
        else:
            logging.error("Unable to retrieve last date from firestore")


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    bearer_token = get_token()
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


class Tweet(TypedDict):
    lang: str
    created_at: str
    id: str
    text: str


class TweetMeta(TypedDict):
    newest_id: str
    oldest_id: str
    result_count: int
    next_token: NotRequired[str]


class TwitterApiResponse(TypedDict):
    data: list[Tweet]
    meta: TweetMeta


def connect_to_endpoint(url: str, params: dict) -> TwitterApiResponse:
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        logging.error("Failed to connect, response different from 200")
    else:
        logging.info("Successfully connected to twitter API")
    return response.json()


def query_twitter(
    start_time: Optional[str] = None, next_token: Optional[str] = None
) -> TwitterApiResponse:
    query_params = {
        "query": "#F1",
        "tweet.fields": "created_at,lang",
        "start_time": start_time,
        "max_results": "100",
        "next_token": next_token,
    }
    json_response = connect_to_endpoint(search_url, query_params)
    return json_response


def fetching_tweets(response: TwitterApiResponse) -> pd.DataFrame:
    if response["meta"].get("next_token", None):
        df = pd.DataFrame(response["data"])[["text", "created_at", "id", "lang"]]
    else:
        df = pd.DataFrame(response["data"])[["text", "created_at", "id", "lang"]].iloc[
            :-1
        ]
    df = df[df["lang"] == "en"]
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df


class PusherToGcs:
    def __init__(self, bucket_name: str) -> None:
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)
        self.error_bucket = self.client.get_bucket("twitter_production_error_bucket")

    def upload_data(self, df: pd.DataFrame, name: str) -> None:
        """
        Loads data into a csv file in gcs

        Args :
            df : Dataframe to push
            name : name that will be the name of the file.csv
        """

        self.bucket.blob(f"twitter_data/{name}.csv").upload_from_string(
            df.to_csv(index=False, encoding="utf-8-sig"), "text/csv"
        )

    def upload_error_data(
        self, json_object: Union[TwitterApiResponse, list], name: str
    ) -> None:
        """
        Loads data into a json file in gcs

        Args :
            json_object : json to push
            name : name that will be the name.csv
        """

        self.error_bucket.blob(f"twitter_data/{name}.json").upload_from_string(
            data=json.dumps(json_object), content_type="application/json"
        )


twitter_json_schema = {
    "type": "object",
    "properties": {
        "data": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "lang": {"type": "string"},
                    "created_at": {"type": "string"},
                    "text": {"type": "string"},
                    "id": {"type": "string"},
                },
                "additionalProperties": False,
                "required": ["lang", "created_at", "text", "id"],
            },
        },
        "meta": {
            "type": "object",
            "properties": {
                "newest_id": {"type": "string"},
                "oldest_id": {"type": "string"},
                "result_count": {"type": "number"},
                "next_token": {"type": "string"},
            },
            "additionalProperties": False,
            "required": ["newest_id", "oldest_id", "result_count"],
        },
    },
}


def twitter_update(event, context) -> None:
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    most_recent_firestore = FirestoreLastDate()
    pusher_to_gcs = PusherToGcs("raw_data_twitter_bucket")
    most_recent_dt = most_recent_firestore.read_last_date()
    response = query_twitter(most_recent_dt)

    # Validating twitter API response schema to handle exceptions and investigate
    try:
        validate(instance=response, schema=twitter_json_schema)
    except:
        pusher_to_gcs.upload_error_data(
            response, f"raw_error_twitter_format_{most_recent_dt}"
        )
    data = pd.DataFrame()
    while response["meta"].get("next_token", None):
        data = data.append(fetching_tweets(response), ignore_index=True)
        response = query_twitter(
            most_recent_dt, response["meta"].get("next_token", None)
        )
    data = data.append(fetching_tweets(response), ignore_index=True)
    table_id = "wagon-bootcamp-802.my_dataset.twitter_table"
    try:
        data.to_gbq(table_id, if_exists="append")
    except:
        data_error = data.copy(deep=True)
        data_error["created_at"] = data_error["created_at"].astype(str)
        json_error = data_error.to_dict(orient="records")
        pusher_to_gcs.upload_error_data(json_error, f"raw_error_gbq_{most_recent_dt}")
    new_last_date = data.iloc[0]["created_at"].tz_localize(None).isoformat() + "Z"
    logging.info("Tweets successfully merged into the table")
    most_recent_firestore.update_last_date(new_last_date)
    logging.info("New date updated to firestore")
    pusher_to_gcs.upload_data(data, "raw_data")
    logging.info("New data updated to gcs")
