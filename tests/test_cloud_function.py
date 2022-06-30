import pandas as pd
from cloud_function.main import last_date_db

def test_last_date_db(mocker):
    df_test = pd.DataFrame({'created_at': ['2022-06-10 17:57:21 UTC']}, index = [0])
    df_test['created_at'] = pd.to_datetime(df_test['created_at'])
    mocker.patch('cloud_function.main.pd.read_gbq',
                return_value = df_test
                )
    date = last_date_db()
    assert date == '2022-06-10T17:57:21Z'