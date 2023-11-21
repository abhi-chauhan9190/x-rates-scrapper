import pandas as pd
import os
import json
import selenium.common
import xrate_currency_conversion as xcc
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
import warnings
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from datetime import datetime
from selenium.webdriver.support.ui import Select
import time
from datetime import date
from dateutil.relativedelta import relativedelta
import shutil
import requests
# import pandas as pd
from bs4 import BeautifulSoup

import collections

collections.Callable = collections.abc.Callable

def get_global_config():
    """
        this function fetches the local configuration

        Returns:
            dict: configuration in dictionary format
    """
    user_dir: str = os.path.expanduser("~")
    global_config_path: str = os.path.join(
        user_dir,
        "Project Configurations",
        "xrates_config.json")

    with open(global_config_path, "r") as f:
        global_config: dict = json.load(f)
        f.close()

    return global_config


def get_data_path(global_config: dict):
    """
        this function returns the path of the given configuration

        Args:
            global_config (dict): configuration in dictionary format

        Returns:
            Path: path of the given configuration
    """
    return Path(global_config['data_path'])


def get_input_df(data_path, global_config):
    """
        this function creates a dataframe from a given Excel file

        Args:
            data_path (Path): path to the folder where file is stored
            global_config (dict): configuration in dictionary format

        Returns:
            pandas.DataFrame: dataframe created from the given Excel file
    """

    input_wb_path = os.path.join(data_path, global_config["input_file_name"])
    input_df = pd.read_excel(input_wb_path)
    return input_df


def init_driver(
        headless: bool = False,
        user_data_dir: str = None,
        browser: str = "firefox",
        log: bool = False
) -> webdriver.Firefox:
    """
        this function automatically open the web-browser

        Args:
            headless (bool): decides whether to show UI Window or not
            user_data_dir (str):
            browser (str): browser to be used
            log (bool): decides whether to log or not

        Returns:
            WebDriver: an API that automates web browsing
    """

    if browser == "firefox":
        options = FirefoxOptions()
    else:
        raise ValueError("Invalid Browser: {}".format(browser))

    if headless:
        options.add_argument("--headless")

    if not log:
        options.add_argument("--disable-logging")

    driver_path = os.path.join(os.getcwd(), "geckodriver.exe")
    driver = webdriver.Firefox(service=FirefoxService(driver_path, log_output=os.devnull),
                               options=options)
    driver.maximize_window()

    return driver


def make_df_copy(df: pd.DataFrame):
    """
        this function makes a copy of the data frame

        Args:
            df (pandas.DataFrame): the data frame to be copied

        Returns:
            pandas.DataFrame: the copied data frame
    """
    assert isinstance(df, pd.DataFrame)
    return df.copy()


def create_mapped_column(col_name: str, dfs: [pd.DataFrame]):
    """
        this function creates a new mapped column in the data frame

        Args:
            col_name (str): the name of the column to be mapped
            dfs (pandas.DataFrame): the given data frame
    """
    df = dfs["add column to this df"]
    mapped_df = dfs["mapped_df"]
    mapping_df = dfs["mapping_df"]

    mapping_dict = mapping_df[col_name].to_dict()

    modified_col_name = col_name.replace("Current", "Initial")
    df[modified_col_name] = mapped_df.index.map(mapping_dict)


def add_informative_columns(df: pd.DataFrame, input_df: pd.DataFrame):
    """
        this function adds new columns to the given data frame

        Args:
            df (pandas.DataFrame): the data frame to which new columns are to be added
            input_df (pandas.DataFrame): the data frame which will be used to create IDs and new columns

        Returns:
            pandas.DataFrame: the new data frame containing the new columns
    """
    input_df_copy = make_df_copy(input_df)
    df_copy = make_df_copy(df)

    mapping_related_dfs = {
        "add column to this df": df,
        "mapped_df": df_copy,
        "mapping_df": input_df_copy,
    }

    create_mapped_column("Location", dfs=mapping_related_dfs)
    create_mapped_column("Current Currency", dfs=mapping_related_dfs)
    create_mapped_column("Final Currency", dfs=mapping_related_dfs)
    create_mapped_column("Current Quantity", dfs=mapping_related_dfs)
    create_mapped_column("Current Unit", dfs=mapping_related_dfs)
    create_mapped_column("Final Quantity", dfs=mapping_related_dfs)
    create_mapped_column("Final Unit", dfs=mapping_related_dfs)
    create_mapped_column("Density", dfs=mapping_related_dfs)
    create_mapped_column("Upload on PR", dfs=mapping_related_dfs)

    return df


def touch_excel(
        df: pd.DataFrame,
        file_path: str,
        sheet_name: str = "Sheet1",
        add_df: pd.DataFrame = None):
    """
        this function concatenates two data frames (if given), and converts them into an Excel file at the given location

        Args:
            df (pandas.DataFrame): the data frame to be written into the Excel file
            file_path (str): the path to the Excel file
            sheet_name (str): the name of the sheet in the Excel file
            add_df (pandas.DataFrame): the additional data frame to be concatenated
    """
    if add_df is not None:
        df = pd.concat([df, add_df], ignore_index=True)

    try:
        if not os.path.exists(file_path):
            df.to_excel(file_path, sheet_name=sheet_name, index=False)
        else:
            with pd.ExcelWriter(file_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    except PermissionError:
        raise Exception("File might be open. Close it.")


def get_local_config(config_filename: str):
    """
        this function fetches the local configuration

        Args:
            config_filename (str): filename containing the local configuration

        Returns:
            dict: configuration in dictionary format
    """
    local_config_path: str = os.path.join(
        os.getcwd(),
        config_filename)

    with open(local_config_path, "r") as f:
        local_config: dict = json.load(f)
        f.close()

    return local_config


def convert_price_for_uom(df: pd.DataFrame):
    """
        This function converts the prices in a DataFrame from one unit of measurement to another.

        Args:
            df: A Pandas DataFrame containing the prices to be converted.

        Returns:
            A Pandas DataFrame with the converted prices.
        """
    uom_config = get_local_config("uom_config.json")
    unique_uom_combos = df[["Initial Unit", "Final Unit"]].drop_duplicates()
    temp_df = df.copy()
    for index, row in unique_uom_combos.iterrows():
        initial_unit = row["Initial Unit"]
        final_unit = row["Final Unit"]

        uom_combo_df = df.loc[
            (df["Initial Unit"] == initial_unit) &
            (df["Final Unit"] == final_unit)
            ]

        to_SI_unit_rate = uom_config["mass_conversion_rates"][initial_unit.lower()]
        temp_df["UOM Converted Price"] = uom_combo_df["Initial Price"] * to_SI_unit_rate  # converting to SI unit
        to_final_unit_rate = uom_config["mass_conversion_rates"][final_unit.lower()]
        temp_df["UOM Converted Price"] = temp_df["UOM Converted Price"] / to_final_unit_rate  # converting to final unit
        df.loc[
            (df["Initial Unit"] == initial_unit) &
            (df["Final Unit"] == final_unit),
            "UOM Converted Price"] = temp_df.loc[
            temp_df["UOM Converted Price"] != NameError]  # feeding it into data frame

    return df


def convert_price_for_currency(df: pd.DataFrame):
    """
        this function converts the initially given currency into the desired currency

        Args:
            df (pandas.DataFrame): the data frame for which currency is to be converted

        Returns:
            pandas.DataFrame: data frame with converted currencies
    """
    unique_currency_combos = df[["Initial Currency", "Final Currency", "Date"]].drop_duplicates()

    for index, row in unique_currency_combos.iterrows():
        initial_currency = row["Initial Currency"]
        final_currency = row["Final Currency"]
        date = row["Date"]
        year = date.year
        month = date.month

        currency_conversion_rate = xcc.get_conversion_rate(from_=initial_currency, to_=final_currency, year_=year,
                                                           month_=month)

        df.loc[
            (df["Initial Currency"] == initial_currency) &
            (df["Final Currency"] == final_currency) &
            (df["Date"] == date),
            "Conversion Rate"] = currency_conversion_rate

        df["Price"] = df["Conversion Rate"] * df["UOM Converted Price"]

    df = df.rename(columns={"Conversion Rate": "Currency Conversion Rate"})
    return df


def extract_date(month,year):
    """
        This function extracts the date from a string in the format "Month Year".

        Args:
            month_year: A string in the format "Month Year".

        Returns:
            A date object representing the extracted date.
        """

    month_mapping = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12,
    }
    month = month_mapping[month]
    year = int(year)
    first_date = date(year, month, 1)
    return first_date




def writing_all_file(df,product):
    """
    This function reads the Data Frame and write it in the output file . If the output file is
    already present it will concat the historical data and the new dataframe and writes it in the
    output file . If the output file is not present it will make one and writes the new df to it.
    Args:
        df: The main Data Frame to write in the output file

    Returns:

    """
    flag=0
    global_config: dict = get_global_config()
    data_path = get_data_path(global_config)
    sorted_df = df.copy()
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_year=int(current_year)
    current_month=int(current_month)
    try:
        path_to_hist_file = os.path.join(data_path, "All-Xrates-Data.xlsx")
        existing_df = pd.read_excel(path_to_hist_file,sheet_name=f'{product}')
        index_to_delete=-1

        for index,row in existing_df.iterrows():
            try:
                if int(row['Month']) == current_month and int(row['Year'])==current_year:
                    index_to_delete=index
                    break

            except Exception as e:
                print(e)
                pass
        # index_to_delete = existing_df[(existing_df['Month'] == current_month) & (existing_df['Year'] == current_year)].index

        if index_to_delete!=-1:
            print('Updated')
            existing_df.drop(index=index_to_delete, inplace=True)

        print(f'Wrting for {product}')
        hist_df = pd.concat([existing_df, sorted_df], ignore_index=True)

        hist_df = hist_df.drop_duplicates(subset=['Year','Month'])
        hist_df = hist_df.sort_values(by=['Year','Month'], ascending=False)
        touch_excel(hist_df, os.path.join(data_path, "All-Xrates-Data.xlsx"),sheet_name=f'{product}')
        backup_path = os.path.join(data_path, 'Backups')
        month = max(df['Month'])
        year = max(df['Year'])
        touch_excel(existing_df, os.path.join(backup_path, f'{month} {year} All-Xrates-Data.xlsx'),
                    sheet_name=f'{product}')
    except:

        existing_df = sorted_df.sort_values(by=['Year','Month'], ascending=False)
        touch_excel(existing_df, os.path.join(data_path, "All-Xrates-Data.xlsx"),sheet_name=f'{product}')
        backup_path = os.path.join(data_path, 'Backups')
        month=max(df['Month'])
        year=max(df['Year'])
        touch_excel(existing_df, os.path.join(backup_path, f'{month} {year} All-Xrates-Data.xlsx'),sheet_name=f'{product}')

def update_input_file(input_df, months ,years):
    global_config: dict = get_global_config()
    data_path = get_data_path(global_config)
    for index, row in input_df.iterrows():
        product = row['Initial Currency']

        try:
            fetch_date = years[product]
            input_df.at[index, 'Fetched Year'] = fetch_date
        except:
            pass

    touch_excel(input_df, os.path.join(data_path, "Inputs.xlsx"), sheet_name="All Currencies")


def main():
    global_config: dict = get_global_config()
    data_path = get_data_path(global_config)
    input_df = get_input_df(data_path, global_config)
    os.makedirs(os.path.join(data_path, "Backups"), exist_ok=True)
    max_year=datetime.now().year
    print(input_df)
    max_years={}
    max_month={}

    for index , row in input_df.iterrows():
        data = []
        curr=row['Initial Currency']
        year=row['Fetched Year']
        year=int(year)

        print(curr)

        if 'USD'==curr:
            print('Skipping')
            continue
        current_year = datetime.now().year
        while year<=current_year:
            retry=5
            flag=1
            while retry:
                try:
                    url = f"https://www.x-rates.com/average/?from={curr}&to=USD&amount=1&year={year}"
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'}
                    html = requests.get(url, headers=headers).text
                    soup = BeautifulSoup(html, "lxml")
                    select = soup.find('select', {"id": "to"})
                    options = select.find_all('option')
                    ul = soup.find("ul", class_='OutputLinksAvg')
                    lists = ul.text.split("\n")
                    flag=0
                    break
                except Exception as e:
                    print(e)
                    retry-=1
                    time.sleep(1)
                    if retry==0:
                        print(f'skipping for {curr}')

            if flag:
                year += 1
                continue
            print(lists)
            for lis in lists:

                lis_text = lis.split(" ")
                if len(lis_text)<=1:
                    continue
                print(lis_text)
                month=lis_text[0]
                crate=lis_text[1]
                crate=float(crate)
                date=extract_date(month,year)
                fmonth=date.month
                fyear=date.year
                fmonth=int(fmonth)

                try:
                    fcrate=1/crate
                except:
                    fcrate=0
                data.append({
                    'Currency 1':curr,
                    "Currency 2":"USD",
                    'Year': year,
                    'Month': fmonth,
                    f'{curr} to USD':crate,
                    f'USD to {curr}':fcrate

                })
            year+=1
        print(data)
        df = pd.DataFrame(data)

        if df.empty:
            continue
        max_month[curr] = max(df['Month'])
        max_years[curr] = max(df['Year'])
        sname=f'{curr}'
        writing_all_file(df,sname)
    if len(max_years)==0 or len(max_month)==0:
        return
    update_input_file(input_df,max_month,max_years)



if __name__ == "__main__":
    main()