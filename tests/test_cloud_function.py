from typing_extensions import NotRequired
import pandas as pd
from cloud_function.main import last_date_db, get_token, query_twitter, fetching_tweets
from typing import TypedDict
from typing_extensions import NotRequired



def get_token_side_effect(token_name, default=None):
    if token_name == "BEARER_TOKEN":
        return "TOKEN_OK"
    else:
        return "Key missing in env settings"


def test_get_token(mocker):
<<<<<<< HEAD
    mocker.patch(
        "cloud_function.main.os.environ.get", side_effect=get_token_side_effect
    )
    assert get_token() == "TOKEN_OK"


def test_last_date_db(mocker):
    df_test_last_date = pd.DataFrame(
        {"created_at": ["2022-06-10 17:57:21 UTC"]}, index=[0]
    )
    df_test_last_date["created_at"] = pd.to_datetime(df_test_last_date["created_at"])
    mocker.patch("cloud_function.main.pd.read_gbq", return_value=df_test_last_date)
    assert last_date_db() == "2022-06-10T17:57:21Z"
=======
    mocker.patch('cloud_function.main.os.environ.get',
                side_effect=get_token_side_effect)
    assert get_token() == 'TOKEN_OK'


def test_last_date_db(mocker):
    df_test_last_date = pd.DataFrame({'created_at': ['2022-06-10 17:57:21 UTC']}, index = [0])
    df_test_last_date['created_at'] = pd.to_datetime(df_test_last_date['created_at'])
    mocker.patch('cloud_function.main.pd.read_gbq',
                return_value = df_test_last_date
                )
    assert last_date_db() == '2022-06-10T17:57:21Z'


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

FAKE_TWITTER_API_RESPONSE: TwitterApiResponse
FAKE_TWITTER_API_RESPONSE = {'data': [{
                'lang': 'en',
                'created_at': '2022-07-06T13:53:07.000Z',
                'id': '1544680846883528706',
                'text': '“This weekend won’t be smooth sailing” says Championship Leader Max Verstappen #Formula1 #F1 #F1News #Motorsport https://t.co/ncnzaoqvcd'
                    }],
            'meta': {'newest_id': '1544680846883528706',
                'oldest_id': '1544679599849623552',
                'result_count': 100,
                'next_token': 'b26v89c19zqg8o3fpz2m15nlsnmmd4ev3codr28uvwzgd'
                    }
            }

FAKE_TWITTER_API_RESPONSE_WITH_NO_NEXT: TwitterApiResponse

FAKE_TWITTER_API_RESPONSE_WITH_NO_NEXT = {'data': [{
                'lang': 'en',
                'created_at': '2022-07-06T13:53:07.000Z',
                'id': '1544680846883528706',
                'text': '“This weekend won’t be smooth sailing” says Championship Leader Max Verstappen #Formula1 #F1 #F1News #Motorsport https://t.co/ncnzaoqvcd'
                    },{
                'lang': 'en',
                'created_at': '2022-07-06T13:53:07.000Z',
                'id': '1544680846883528706',
                'text': '“This weekend won’t be smooth sailing” says Championship Leader Max Verstappen #Formula1 #F1 #F1News #Motorsport https://t.co/ncnzaoqvcd'
                    }],
            'meta': {'newest_id': '1544680846883528706',
                'oldest_id': '1544679599849623552',
                'result_count': 100
                    }
            }

def test_query_twitter_type(mocker):
    mocker.patch('cloud_function.main.connect_to_endpoint',
                return_value = FAKE_TWITTER_API_RESPONSE
                )
    assert query_twitter()

def test_query_twitter_get_data(mocker):
    mocker.patch('cloud_function.main.connect_to_endpoint',
                return_value = FAKE_TWITTER_API_RESPONSE
                )
    assert query_twitter().get('data',False) != False

def test_fetching_tweets_no_next():
    df_test_from_json = fetching_tweets(FAKE_TWITTER_API_RESPONSE)
    df_test_comparison = pd.DataFrame({
                'text': '“This weekend won’t be smooth sailing” says Championship Leader Max Verstappen #Formula1 #F1 #F1News #Motorsport https://t.co/ncnzaoqvcd',
                'created_at': '2022-07-06T13:53:07.000Z',
                'id': '1544680846883528706',
                'lang': 'en'
                    },index = [0])
    df_test_comparison['created_at'] = pd.to_datetime(df_test_comparison['created_at'])
    pd.testing.assert_frame_equal(df_test_from_json,df_test_comparison)

def test_fetching_tweets_next():
    df_test_from_json = fetching_tweets(FAKE_TWITTER_API_RESPONSE_WITH_NO_NEXT)
    df_test_comparison = pd.DataFrame({
                'text': '“This weekend won’t be smooth sailing” says Championship Leader Max Verstappen #Formula1 #F1 #F1News #Motorsport https://t.co/ncnzaoqvcd',
                'created_at': '2022-07-06T13:53:07.000Z',
                'id': '1544680846883528706',
                'lang': 'en'
                    },index = [0])
    df_test_comparison['created_at'] = pd.to_datetime(df_test_comparison['created_at'])
    pd.testing.assert_frame_equal(
        df_test_from_json,
        df_test_comparison)
<<<<<<< HEAD
>>>>>>> adding tests for CF
=======
>>>>>>> Solving typing missing
