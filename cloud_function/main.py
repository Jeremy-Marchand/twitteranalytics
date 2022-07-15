import os
from typing import Dict
import requests

# from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import pandas as pd
import logging
from typing import TypedDict, Optional
from typing_extensions import NotRequired

search_url = "https://api.twitter.com/2/tweets/search/recent"
# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields


def get_token():
    token = os.environ.get("BEARER_TOKEN", "Key missing in env settings")
    return token

def last_date_db() -> str:
    """
    Method to retrieve the last date in the DB
    """
    # Construct a BigQuery client object.

    query = """
        SELECT created_at
        FROM `wagon-bootcamp-802.my_dataset.twitter_table`
        ORDER BY created_at DESC
        LIMIT 1
    """
    df = pd.read_gbq(query, dialect="standard")
    return df.iloc[0]["created_at"].tz_localize(None).isoformat() + "Z"


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
        logging.error('Failed to connect, response different from 200')
    else:
        logging.info('Successfully connected to twitter API')
    return response.json()


def query_twitter(start_time: Optional[str]=None, next_token: Optional[str]=None) -> TwitterApiResponse:
    query_params = {'query': '#F1',
                    'tweet.fields':'created_at,lang',
                    'start_time':start_time,
                    'max_results' : '100',
                    'next_token': next_token
                   }
    json_response = connect_to_endpoint(search_url, query_params)
    return json_response

def fetching_tweets(response: TwitterApiResponse) -> pd.DataFrame:
    if response['meta'].get('next_token', None):
        df = pd.DataFrame(response['data'])[['text', 'created_at', 'id', 'lang']]
    else:
        df = pd.DataFrame(response["data"])[["text", "created_at", "id", "lang"]].iloc[
            :-1
        ]
    df = df[df["lang"] == "en"]
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df

def main() -> None:
    """
    Pushing results to GBQ
    """
    most_recent_dt = last_date_db()
    response = query_twitter(most_recent_dt)
    data = pd.DataFrame()
    while response["meta"].get("next_token", False):
        data = data.append(fetching_tweets(response), ignore_index=True)
        response = query_twitter(most_recent_dt,response['meta'].get('next_token',None))
    data = data.append(fetching_tweets(response), ignore_index=True)
    table_id = 'wagon-bootcamp-802.my_dataset.twitter_table'
    data.to_gbq(table_id, if_exists='append')
    logging.info('Tweets successfully merged into the table')
    return None

def twitter_update(event, context) -> None:
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    main()
