#!/usr/bin/env python

import pickle
import os
import requests
import pandas
import time
from tabula import read_pdf
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

URL = "https://www.perth.wa.gov.au/develop/planning-and-building-applications/building-and-development-applications"
DATABASE = "data.sqlite"
DATA_TABLE = "data"
PROCESSED_FILES_TABLE = "files_processed"
PROCESSED_FILES_COLUMN = "NAME"

#engine = create_engine('sqlite:///' + DATABASE, echo=False)
#pandas.DataFrame(columns=[PROCESSED_FILES_COLUMN]).to_sql(PROCESSED_FILES_TABLE, con=engine, if_exists="append")
#
#soup = BeautifulSoup(requests.get(URL).content)
#for link in soup.find_all('a', download=True):
#    title = link.get('title')
#
#    if len(engine.execute(f"SELECT 1 from {PROCESSED_FILES_TABLE} where NAME=\"{title}\"").fetchall()) > 0:
#        continue
#
#    df = read_pdf(link.get('href'), lattice=True)
#
#    resultTable = pandas.DataFrame()
#    if "Application Lodged" in title:
#        resultTable['date_received'] = df.LODGED
#        resultTable['address'] = df.ADDRESS.map(lambda x: x.replace("\r", " "))
#        resultTable['description'] = "Application Lodged " + df.DESCRIPTION + ", Value: " + df.VALUE.map(str)
#        resultTable['council_reference'] = dfa['APPLICATION\rNUMBER']
#    elif "Building Permits" in title or  "DA Approved" in title or "Demolition Licenses Approved" in title:
#        resultTable['date_received'] = df['Decision Date']
#        resultTable['address'] = df['Primary Property Address'].map(lambda x: x.replace("\r", " "))
#        resultTable['description'] = df['Application Description'] + ", Value: " + df['Est Value'] + ", Descision: " + df.Descision
#        resultTable['council_reference'] = dfa['App Year/Number']
#    else:
#        print(f"ignoring unkown pdf {title}")
#
#    resultTable['info_url'] = URL
#    resultTable['comment_url'] = URL
#
#    resultTable.to_sql(TABLE_NAME, con=engine, if_exists='append')
#
#    pandas.DataFrame([title], columns=[PROCESSED_FILES_COLUMN]).to_sql(PROCESSED_FILES_TABLE, con=engine, if_exists="append")
