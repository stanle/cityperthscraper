import os
import re
import stat
from datetime import datetime
from functools import partial
from shutil import which
from typing import List


import numpy as np
import pandas as pd
from selenium.webdriver import ChromeOptions
import tabula

from splinter import Browser
from sqlalchemy import create_engine
from tabula import read_pdf

# Note: work-around because the morph early_release image doesn't have java installed,
# and the tabula _run() function has the java path hard-coded
if which("java") is None:
    print("Java not found. Installing JRE.")
    import jdk
    import tabula_custom
    jre_dir = jdk.install('11', jre=True, path='/tmp/.jre')
    tabula.io._run = partial(tabula_custom._run, java_path=jre_dir + '/bin/java')

URL = "https://www.perth.wa.gov.au/develop/planning-and-building-applications/building-and-development-applications"
DATABASE = "data.sqlite"
DATA_TABLE = "data"
PROCESSED_FILES_TABLE = "files_processed"
PROCESSED_FILES_COLUMN = "name"

engine = create_engine(f'sqlite:///{DATABASE}', echo=False)
pd.DataFrame(columns=[PROCESSED_FILES_COLUMN]).to_sql(PROCESSED_FILES_TABLE, con=engine, if_exists="append")


def make_first_row_header(df: pd.DataFrame) -> pd.DataFrame:
    df, df.columns = df[1:], df.iloc[0]
    return df

def clean_received_date(date: str) -> datetime:
    d, m, y = date.split("/")
    if len(y) == 2:
        y = f"20{y}"
    return datetime(int(y), int(m), int(d))

def clean_address(address: str) -> str:
    """
    :param address: as extracted from PDF, containing line breaks, e.g. 89 Fairway\rCRAWLEY WA  6009
    :return: cleaned address, optimised for address parsing, e.g. 89 Fairway, CRAWLEY, WA, 6009
    """
    return re.sub(r'\sWA\s+(6\d{3})$', r', WA, \1', address.replace("\r", ", "))


def clean_description(description: str) -> str:
    return description.replace("\r", " ")

# can not use simple request to get the page content. Need headless browser
options = ChromeOptions()
options.headless = True
options.add_argument('--no-sandbox')
options.add_argument('--disable-extensions')

with Browser('chrome', headless=True, options=options) as browser:
    browser.visit(URL)
    links = browser.find_by_css(".list-item > a")
    for link in links:
        title = link.html
        pdf_url = link["href"]

        if len(engine.execute(f"SELECT 1 FROM {PROCESSED_FILES_TABLE} WHERE name=:title", dict(title=title)).fetchall()) > 0:
            print(f"==== read file {title} already")
            continue

        print(f"Downloading PDF for '{title}' - {pdf_url}")
        dfs: List[pd.DataFrame] = read_pdf(pdf_url,
                                           lattice=True,
                                           pages="all",
                                           user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64)')

        final_df = pd.DataFrame()
        last_df = None
        for df in dfs:
            if df.empty:
                continue

            # drop empty columns
            df.dropna(axis=1, how='all', inplace=True)
            df.fillna('', inplace=True)

            # check if the table is a continuation from the previous page with the same columns and
            #  first rows having been read as header
            if (
                not final_df.empty and len(df.columns) == len(final_df.columns)
                and not df.empty and '' not in df.columns
                and re.match('\d+/\d+/\d+', df.columns[0])
            ):
                print(f"Table continuation detected. Converting header into record: {df.columns.values}")
                df.loc[-1] = df.columns  # adding a row
                df.index = df.index + 1  # shifting index
                df = df.sort_index()  # sorting by index
                df.columns = final_df.columns.values

            # keep replacing heading for first row until we have column headers
            while (
                not {"Decision Date", "Lodged", "Decision", "Decision Date", "DESCRIPTION"}.intersection(df.columns)
                and not df.empty
            ):
                df = make_first_row_header(df)

            # sometimes tables are too wide and are split in the middle of a column and flow onto the following page
            if last_df is not None:
                print("Wide table split detected. Merging with table on previous page.")
                df = last_df.merge(df, left_index=True, right_index=True)
                if '' in df.columns:
                    empty_col = df.columns.get_loc('')
                    df[df.columns[empty_col - 1]] = df[df.columns[empty_col - 1]] + df[
                        df.columns[empty_col]]
                    df.drop(columns=[''], inplace=True)

            # left-shift header and drop last column if header parsing failed, which often results
            # in all headers getting concatenated into first header and an empty columns getting added
            left_header = df.columns[0].lower()
            if (
                sum([1 for w in ["decision", "lodged", "decision", "description", "address"] if w in left_header]) > 2
                and df[df.columns[-1]].replace('', np.nan).isnull().all()
            ):
                print("Re-aligning Columns")
                df.columns = list(df.columns[1:]) + ['dummy']
                df.drop(columns=df.columns[-1], inplace=True)

            if len(df.columns) >= 6:
                final_df = final_df.append(df)
                last_df = None
            else:
                last_df = df

        df = final_df

        # header cleanup
        df.columns = df.columns.map(lambda x: x.replace("\r", " "))
        df.rename(columns={
            "App Year/Number": "Application Number",
            "LODGEMENT PROCESSED / RENEWED": "LODGED"}, inplace=True)

        # drop rows with empty required fields
        for non_empty_col in ['Application Description', 'DESCRIPTION', 'Primary Property Address', 'ADDRESS', 'Decision Date', 'LODGED']:
            if non_empty_col in df.columns:
                df[non_empty_col].replace('', np.nan, inplace=True)
                df.dropna(subset=[non_empty_col], inplace=True)

        print(title)
        print(df.head(1))
        print(df.columns.values)

        try:
            resultTable = pd.DataFrame()
            resultTable['date_scraped'] = datetime.today()
            if "Applications Lodged" in title and "Decision" not in df.columns:
                resultTable['date_received'] = df['LODGED'].map(clean_received_date)
                resultTable['address'] = df['ADDRESS'].map(clean_address)
                resultTable['description'] = "Application Lodged " \
                                             + df['DESCRIPTION'].map(clean_description) \
                                             + ", Value: " + df['VALUE'].map(str)
                resultTable['council_reference'] = df['APPLICATION NUMBER']
            elif (
                "Building Permits" in title
                or "DA Approved" in title
                or ("Applications Lodged" in title and "Decision" in df.columns)
                or "Demolition Licenses Approved" in title
            ):
                resultTable['date_received'] = df['Decision Date'].map(clean_received_date)
                resultTable['address'] = df['Primary Property Address'].map(clean_address)
                resultTable['description'] = df['Application Description'].map(clean_description) \
                                             + ", Value: " + df['Est Value'] \
                                             + ", Decision: " + df.Decision
                resultTable['council_reference'] = df['Application Number']
            else:
                print(f"==== ignoring unkown pdf {title}")

            resultTable['info_url'] = pdf_url
            resultTable.to_sql(DATA_TABLE, con=engine, if_exists='append', index=False)
            print(f"Saved {len(resultTable)} records")
        except Exception as e:
            print(f"failed to process {title} - {str(e)}")
            print(df)
            raise e

        pd.DataFrame([title], columns=[PROCESSED_FILES_COLUMN]).to_sql(PROCESSED_FILES_TABLE, con=engine, if_exists="append")
