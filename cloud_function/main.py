import os
import requests
# from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import pandas as pd

search_url = "https://api.twitter.com/2/tweets/search/recent"
# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields

def get_token():
    token = os.environ.get('BEARER_TOKEN','Key missing in env settings')
    return token

def last_date_db():
    """
    Method to retrieve the last date in the DB
    """
    # Construct a BigQuery client object.

    query = """
        SELECT created_at
        FROM `wagon-bootcamp-802.my_dataset.new_table`
        ORDER BY created_at DESC
        LIMIT 1
    """
    df = pd.read_gbq(query, dialect='standard')
    return df.iloc[0]['created_at'].tz_localize(None).isoformat()+'Z'


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    bearer_token = get_token()
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def query_twitter(start_time=None):
    query_params = {'query': '#F1','tweet.fields':'created_at,lang', 'start_time':start_time, 'max_results' : '100'}
    json_response = connect_to_endpoint(search_url, query_params)
    return json_response

def main():
    '''
    Pushing results to GBQ
    '''
    most_recent_dt = last_date_db()
    df = pd.DataFrame(query_twitter(most_recent_dt)['data']).iloc[:-1]
    df = df[df['lang'] == 'en']
    df['created_at'] = pd.to_datetime(df['created_at'])
    table_id = 'wagon-bootcamp-802.my_dataset.new_table'
    df.to_gbq(table_id, if_exists='append')

def twitter_update(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    main()
