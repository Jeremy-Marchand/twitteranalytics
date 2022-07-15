import pandas as pd
from cloud_function.main import last_date_db, get_token


def get_token_side_effect(token_name, default=None):
    if token_name == "BEARER_TOKEN":
        return "TOKEN_OK"
    else:
        return "Key missing in env settings"


def test_get_token(mocker):
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
