import pandas as pd


from cloud_function_cleaning.word_transformation import (
    num_remove,
    punct_remove,
    stop_remove,
)


def drivers(date_start, date_end):
    # local test request : http://127.0.0.1:8000/drivers?date_start=2022-05-20%2009:00:00%2B00:00&date_end=2022-05-22%2009:00:00%2B00:00
    query = """
    SELECT *
    FROM `wagon-bootcamp-802.my_dataset.twitter_table`
    WHERE created_at > "{}" AND created_at < "{}"
    ORDER BY created_at DESC
    """
    final_query = query.format(date_start, date_end)
    project_id = "wagon-bootcamp-802"
    # Uncomment to run in a container locally
    #     credentials = service_account.Credentials.from_service_account_file(
    #     'gcp_key/wagon-bootcamp-802-bd537eeb2bd3.json',
    # )
    # Uncomment final part of the following to run in a local container
    df = pd.read_gbq(
        final_query, project_id=project_id, dialect="standard"
    )  # , credentials=credentials)
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
    nb_tweets = {}
    for driver, names in drivers_list.items():
        mask = df_clean["text"].str.contains(f"{names[0]}", na=False)
        if len(names) > 1:
            for name in names[1:]:
                mask = mask | df_clean["text"].str.contains(f"{name}", na=False)
        nb_tweets[driver] = len(df_clean[mask])

    return nb_tweets
