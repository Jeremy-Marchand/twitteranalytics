import pandas as pd


from cloud_function_cleaning.word_transformation import (
    num_remove,
    punct_remove,
    stop_remove,
)


def cleaning():
    # local test request : http://127.0.0.1:8000/drivers?date_start=2022-05-20%2009:00:00%2B00:00&date_end=2022-05-22%2009:00:00%2B00:00
    df = pd.DataFrame()
    df_clean = df.copy()

    # Removing RT mentions
    df_clean["text"] = df_clean["text"].str.replace(r"RT @\S* ", "")
    df_clean["text"] = df_clean["text"].str.replace(r"@\S* ", "")
    df_clean["text"] = df_clean["text"].str.replace(r"http\S*", "")

    # Removing ponctuation
    df_clean["text"] = df_clean["text"].apply(punct_remove)
    df_clean["text"] = df_clean["text"].apply(lambda row: row.lower())

    # Removing numerical values
    df_clean["text"] = df_clean["text"].apply(num_remove)

    # removing stop words
    df_clean["text"] = df_clean["text"].apply(stop_remove)
    df_clean["text"] = df_clean["text"].str.replace(r" f ", " f1 ")

    # fetching final results using the drivers list
    return df_clean
