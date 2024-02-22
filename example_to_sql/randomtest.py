# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 15:08:50 2023

@author: Dime
"""

import warnings
import os
import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from example_pipelines.healthcare.healthcare_utils import MyW2VTransformer, MyKerasClassifier,         create_model
from mlinspect.utils import get_project_root, store_timestamp
import time
from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector

#HIER IST DIE ECHTE TESTDB MIT TPCH DATEN
POSTGRES_USER = "postgres"
POSTGRES_PW = "123456"
POSTGRES_DB = "db1"
POSTGRES_PORT = 25432
POSTGRES_HOST = "localhost"

dbms_connector_p = PostgresqlConnector(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PW, port=POSTGRES_PORT, host=POSTGRES_HOST)

def udf_disc_price(extended, discount):
    return (extended*(1-discount))

def udf_charge(extended, discount, tax):
    return extended*((1-discount)*(1+tax))
    #np.multiply(extended, np.multiply(np.subtract(1, discount), np.add(1, tax)))
           
def load_table(table, sf=1):
    parse_dates=[]
    if table=='orders':
        parse_dates=['o_orderdate']
    elif table=='lineitem':
        parse_dates=['l_shipdate', 'l_commitdate', 'l_receiptdate']

    df = pd.read_sql_query('SELECT * FROM pg2_sf'+str(sf)+'_'+table,
                         dbms_connector_p.connection, parse_dates=parse_dates)
    dbms_connector_p.run(f"END;")
    
    cols = df.select_dtypes(object).columns
    df[cols] = df[cols].apply(lambda x: x.astype(str).str.strip())
    return df
    
    
#Eventuell muss ich hier connection NOCHMAL zur database aufbauen, um die Daten aus der Datenbank aufzurufen.
#import sqlite3 
#connection = sqlite3.connect('db1.db') 
#table1_Customer = pd.read_sql_query('Select * from pg1_sf1_customer;', dbms_connector_p.connection)
#pg1_sf1_customer_test = pd.read_csv(os.path.join( str(get_project_root()), "example_pipelines", "healthcare", "patients.csv"), na_values='?')
#ergebnis = pg1_sf1_customer_test.groupby('c_mktsegment').agg(mean_accountbalance=('c_acctbal','mean'))

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
#SO WIRD DAS IN TPCH-QUERY GEMACHT! Nutze dazu doch noch die dazugehörige methode load_table (bzw nutze einfach den connectorx und schau was passiert)
#nation = load_table(read_method, 'nation', sf)

#lineitem = load_table('lineitem')
t0 = time.time()

parse_dates=['l_shipdate', 'l_commitdate', 'l_receiptdate']
lineitem= pd.read_sql_query('SELECT * FROM pg2_sf1_lineitem', dbms_connector_p.connection, parse_dates=parse_dates)

df = lineitem[["l_shipdate", "l_returnflag", "l_linestatus", "l_quantity",
                "l_extendedprice", "l_discount", "l_tax"]][(lineitem['l_shipdate'] <= '1998-09-01')]
df['disc_price'] = udf_disc_price(
    df['l_extendedprice'], df['l_discount'])
df['charge'] = udf_charge(df['l_extendedprice'],
                            df['l_discount'], df['l_tax'])    
res = df.groupby(['l_returnflag', 'l_linestatus'])        .agg({'l_quantity': 'sum', 'l_extendedprice': 'sum', 'disc_price': 'sum', 'charge': 'sum',
            'l_quantity': 'mean', 'l_extendedprice': 'mean', 'l_discount': 'mean', 'l_shipdate': 'count'})

t1 = time.time()
print("\nTime spend with original (pandas): " + str(t1 - t0))
#del lineitem
#return loadt, exect, res
print("df jjjjjjjjjjjjjjjjjjjjjjjjjjjj", df)
print("res jjjjjjjjjjjjjjjjjjjjjjjjjj", res)