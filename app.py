import os
import pymongo
import pandas as pd
from uuid import uuid4
import streamlit as st
from pprint import pprint as pp


st.set_page_config(page_title="Menu creator", layout="wide")
MONGO_CLUSTER = os.environ["MONGO_URI"]
PRODUCTION_MONGO_USERNAME = os.getenv("PRODUCTION_MONGO_USERNAME")
PRODUCTION_MONGO_PASSWORD = os.getenv("PRODUCTION_MONGO_PASSWORD")
PRODUCTION_DB_NAME = os.getenv("PRODUCTION_DB_NAME")
STAGING_MONGO_USERNAME = os.getenv("STAGING_MONGO_USERNAME")
STAGING_MONGO_PASSWORD = os.getenv("STAGING_MONGO_PASSWORD")
STAGING_DB_NAME = os.getenv("STAGING_DB_NAME")


production_connection_string = f"mongodb+srv://{PRODUCTION_MONGO_USERNAME}:{PRODUCTION_MONGO_PASSWORD}@{MONGO_CLUSTER}/?retryWrites=true&ssl=true&ssl_cert_reqs=CERT_NONE&w=majority"
staging_connection_string = f"mongodb+srv://{STAGING_MONGO_USERNAME}:{STAGING_MONGO_PASSWORD}@{MONGO_CLUSTER}/?retryWrites=true&ssl=true&ssl_cert_reqs=CERT_NONE&w=majority"

db_client = pymongo.MongoClient(production_connection_string)
db_client = db_client.get_database(PRODUCTION_DB_NAME)

merchant_details = pd.DataFrame(list(db_client.merchants_details.find({}, {'_id': 0})))
merchant_details = merchant_details.rename(columns={'phone_number': 'merchant_phone_number'})
restuarant_names = list(merchant_details["restaurant_name"].unique())
restaurant_name = st.selectbox("Select restaurant", options=restuarant_names)

orders = pd.DataFrame(list(db_client.orders.find({}, {'_id': 0})))
orders = orders.rename(columns={'created_on': 'order_created_on'})
pp(orders.columns)

required_columns = [
    "restaurant_name", "merchant_phone_number",
    "customer_name", "order_id", "order_created_on", 
    "order_type", "table_number", "order_created_by",
    "amount_after_taxes", "order_status"
]
orders = orders.merge(merchant_details, on='merchant_id', how='left')
customer_details = pd.DataFrame(list(db_client.customer_details.find({}, {'_id': 0})))
customer_details = customer_details.rename(columns={'user_name': 'customer_name'})
pp(customer_details.columns)
orders = orders.merge(customer_details, on='customer_id', how='left')
restaurant_orders = orders[orders["restaurant_name"]==restaurant_name][required_columns]
restaurant_orders["table_number"] = restaurant_orders["table_number"].fillna("NA")
st.write(restaurant_orders)

total_order_amount = restaurant_orders["amount_after_taxes"].sum()
total_dine_order_amount = restaurant_orders[restaurant_orders["order_type"]=="Dine-in"]["amount_after_taxes"].sum()
total_takeaway_order_amount = restaurant_orders[restaurant_orders["order_type"]=="Take away"]["amount_after_taxes"].sum()
total_delivery_order_amount = restaurant_orders[restaurant_orders["order_type"]=="Delivery"]["amount_after_taxes"].sum()

st.write(f"Total order amount: {total_order_amount}")
st.write(f"Total dine in order amount: {total_dine_order_amount}")
st.write(f"Total takeaway order amount: {total_takeaway_order_amount}")
st.write(f"Total delivery order amount: {total_delivery_order_amount}")