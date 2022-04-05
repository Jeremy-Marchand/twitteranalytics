from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pickle
from pydantic import BaseModel


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

@app.get("/predict")
def predict(account_amount_added_12_24m,
            age,
            avg_payment_span_0_12m,
            num_unpaid_bills,
            status_last_archived_0_24m,
            recovery_debt,
            sum_capital_paid_account_0_12m,
            sum_paid_inv_0_12m,
            merchant_group):
    df = pd.DataFrame({'account_amount_added_12_24m':[account_amount_added_12_24m],
    'account_days_in_dc_12_24m':[1],
    'account_days_in_rem_12_24m':[1],
    'account_days_in_term_12_24m':[1],
    'account_incoming_debt_vs_paid_0_24m':[1],
    'account_status':[1],
    'account_worst_status_0_3m':[1],
    'account_worst_status_12_24m':[1],
    'account_worst_status_3_6m':[1],
    'account_worst_status_6_12m':[1],
    'age':[age],
    'avg_payment_span_0_12m':[avg_payment_span_0_12m],
    'avg_payment_span_0_3m':[1],
    'merchant_category':['1'],
    'merchant_group':[merchant_group],
    'has_paid':[1],
    'max_paid_inv_0_12m':[1],
    'max_paid_inv_0_24m':[1],
    'name_in_email':[1],
    'num_active_div_by_paid_inv_0_12m':[1],
    'num_active_inv':[1],
    'num_arch_dc_0_12m':[1],
    'num_arch_dc_12_24m':[1],
    'num_arch_ok_0_12m':[1],
    'num_arch_ok_12_24m':[1],
    'num_arch_rem_0_12m':[1],
    'num_arch_written_off_0_12m':[1],
    'num_arch_written_off_12_24m':[1],
    'num_unpaid_bills':[num_unpaid_bills],
    'status_last_archived_0_24m':[status_last_archived_0_24m],
    'status_2nd_last_archived_0_24m':[1],
    'status_3rd_last_archived_0_24m':[1],
    'status_max_archived_0_6_months':[1],
    'status_max_archived_0_12_months':[1],
    'status_max_archived_0_24_months':[1],
    'recovery_debt':[recovery_debt],
    'sum_capital_paid_account_0_12m':[sum_capital_paid_account_0_12m],
    'sum_capital_paid_account_12_24m':[1],
    'sum_paid_inv_0_12m':[sum_paid_inv_0_12m],
    'time_hours':[1],
    'worst_status_active_inv':[1]})
    # Load pipeline from pickle file
    pipeline = pickle.load(open("pipeline.pkl","rb"))
    return {'prediction' : pipeline.predict_proba(df)[0,1]}

class Item(BaseModel):
    uuid : object
    account_amount_added_12_24m : int
    account_days_in_dc_12_24m : float
    account_days_in_rem_12_24m : float
    account_days_in_term_12_24m : float
    account_incoming_debt_vs_paid_0_24m : float
    account_status : float
    account_worst_status_0_3m : float
    account_worst_status_12_24m : float
    account_worst_status_3_6m : float
    account_worst_status_6_12m : float
    age : int
    avg_payment_span_0_12m : float
    avg_payment_span_0_3m : float
    merchant_category : object
    merchant_group : object
    has_paid : bool
    max_paid_inv_0_12m : float
    max_paid_inv_0_24m : float
    name_in_email : object
    num_active_div_by_paid_inv_0_12m : float
    num_active_inv : int
    num_arch_dc_0_12m : int
    num_arch_dc_12_24m : int
    num_arch_ok_0_12m : int
    num_arch_ok_12_24m : int
    num_arch_rem_0_12m : int
    num_arch_written_off_0_12m : float
    num_arch_written_off_12_24m : float
    num_unpaid_bills : int
    status_last_archived_0_24m : int
    status_2nd_last_archived_0_24m : int
    status_3rd_last_archived_0_24m : int
    status_max_archived_0_6_months : int
    status_max_archived_0_12_months : int
    status_max_archived_0_24_months : int
    recovery_debt : int
    sum_capital_paid_account_0_12m : int
    sum_capital_paid_account_12_24m : int
    sum_paid_inv_0_12m : int
    time_hours : float
    worst_status_active_inv : float


@app.post("/multipred")
async def create_item(item: Item):
    return item
