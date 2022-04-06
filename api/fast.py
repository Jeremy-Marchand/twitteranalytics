from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
# from pydantic import BaseModel
import string
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
@app.get("/")
def test():
    return {'message' : 'Yo les 802!'}


# Get drivers API :

# Defining the names to find in tweets
drivers_list = {'Hamilton' : ['hamilton','lewis'],
           'Russel' : ['russel','georges'],
           'Perez' : ['perez','checo','sergio'],
           'Verstappen' : ['verstappen','max'],
           'Sainz' : ['sainz','carlos'],
           'Leclerc' : ['leclerc','charles'],
           'Ricciardo' : ['ricciardo', 'daniel'],
           'Norris' : ['norris','lando'],
           'Alonso' : ['alonso','fernando'],
           'Ocon' : ['ocon','esteban'],
           'Schumacher' : ['schumacher','mick'],
           'Magnussen' : ['magnussen','kevin'],
           'Bottas' : ['bottas','valtteri'],
           'Zhou' : ['zhou','guanyu'],
           'Gasly' : ['gasly','pierre'],
           'Tsunoda' : ['tsunoda','yuki'],
           'Stroll' : ['stroll','lance'],
           'Hulkenberg' : ['hulkenberg','nico'],
           'Albon' : ['albon','alexander'],
           'Latifi' : ['latifi','nicholas']}

@app.get("/drivers")
def drivers(date_start, date_end):
    # local test request : http://127.0.0.1:8000/drivers?date_start=2022-04-05%2015:25:49%2B00:00&date_end=2022-04-05%2015:24:59%2B00:00
    query = """
    SELECT *
    FROM `wagon-bootcamp-802.my_dataset.new_table`
    WHERE created_at > "{}" AND created_at < "{}"
    ORDER BY created_at DESC
    """
    final_query = query.format(date_start, date_end)
    project_id = 'wagon-bootcamp-802'
    
    df = pd.read_gbq(final_query, project_id=project_id, dialect='standard')
    df_clean = df.copy()

    #Removing RT mentions
    df_clean['text'] = df_clean['text'].str.replace(r'RT @\S* ', '')
    df_clean['text'] = df_clean['text'].str.replace(r'@\S* ', '')
    df_clean['text'] = df_clean['text'].str.replace(r'http\S*', '')
    
    #Removing ponctuation
    def punct_remove(text):
        for punctuation in string.punctuation:
            text = text.replace(punctuation, '') 
        return text
    df_clean['text'] = df_clean['text'].apply(punct_remove)
    df_clean['text'] = df_clean['text'].apply(lambda row : row.lower())
    
    #Removing numerical values
    def num_remove(text):
        text_rwkd = ''
        for car in text:
            text_rwkd += car if not car.isdigit() else ''
        return text_rwkd
    df_clean['text'] = df_clean['text'].apply(num_remove)
    
    #removing stop words
    def stop_remove(text):
        stop_words = set(stopwords.words('english')) 
        word_tokens = word_tokenize(text)
        return " ".join([w for w in word_tokens if not w in stop_words])

    df_clean['text'] = df_clean['text'].apply(stop_remove)
    df_clean['text'] = df_clean['text'].str.replace(r' f ', ' f1 ')
    
    #fetching final results using the drivers list
    nb_tweets = {}
    for driver, names in drivers_list.items():
        mask = df_clean['text'].str.contains(f"{names[0]}", na=False)
        if len(names) > 1:
            for name in names[1:]:
                mask = mask | df_clean['text'].str.contains(f"{name}", na=False)
        nb_tweets[driver] = len(df_clean[mask])
    
    return nb_tweets

#Template for post method
# class Item(BaseModel):
#     uuid : object
#     account_amount_added_12_24m : int
#     account_days_in_dc_12_24m : float
#     account_days_in_rem_12_24m : float
#     account_days_in_term_12_24m : float
#     account_incoming_debt_vs_paid_0_24m : float
#     account_status : float
#     account_worst_status_0_3m : float
#     account_worst_status_12_24m : float
#     account_worst_status_3_6m : float
#     account_worst_status_6_12m : float
#     age : int
#     avg_payment_span_0_12m : float
#     avg_payment_span_0_3m : float
#     merchant_category : object
#     merchant_group : object
#     has_paid : bool
#     max_paid_inv_0_12m : float
#     max_paid_inv_0_24m : float
#     name_in_email : object
#     num_active_div_by_paid_inv_0_12m : float
#     num_active_inv : int
#     num_arch_dc_0_12m : int
#     num_arch_dc_12_24m : int
#     num_arch_ok_0_12m : int
#     num_arch_ok_12_24m : int
#     num_arch_rem_0_12m : int
#     num_arch_written_off_0_12m : float
#     num_arch_written_off_12_24m : float
#     num_unpaid_bills : int
#     status_last_archived_0_24m : int
#     status_2nd_last_archived_0_24m : int
#     status_3rd_last_archived_0_24m : int
#     status_max_archived_0_6_months : int
#     status_max_archived_0_12_months : int
#     status_max_archived_0_24_months : int
#     recovery_debt : int
#     sum_capital_paid_account_0_12m : int
#     sum_capital_paid_account_12_24m : int
#     sum_paid_inv_0_12m : int
#     time_hours : float
#     worst_status_active_inv : float


# @app.post("/multipred")
# async def create_item(item: Item):
#     return item
