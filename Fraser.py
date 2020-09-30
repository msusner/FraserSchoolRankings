import csv
import sys
import datetime
from selenium import webdriver
import time
import os
import pymysql
import csv
import pandas as pd
import numpy as np
import lxml.html as lh
import cryptography
import requests


def create_sql_table(path_to_csv, table_name, database_name):
    conn = pymysql.connect(host='34.89.97.3', user='u831388458_covid19', password='Password@123', db='u831388458_covid19stats')
    cursor = conn.cursor()
    print("Database opened successfully")
    create_query = "CREATE TABLE IF NOT EXISTS `" + database_name + "`.`" + table_name + "` ("
    with open(path_to_csv, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        column_names = next(reader)
        print('These are the default column names:')
        print(column_names)
        print("\n")
    df = pd.read_csv(path_to_csv)

    for x in range(0,len(column_names)):
        column_data = df[column_names[x]].tolist()
        longest = -1
        for s in column_data:
            if longest < len(str(s)):
                longest = len(str(s))
        create_query += "`" + column_names[x] + "` VARCHAR(" + str(longest + 1) + ") NULL,"
    create_query = create_query[:-1]
    create_query += ")"
    print(create_query)
    cursor.execute(create_query)
    conn.commit()
    print("Table " + table_name + " has been created successfully with " + str(len(column_names)) +
          " columns")


def upload_to_sql(path_to_csv, table_name):
    with open(path_to_csv, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        num_cols = len(next(reader))
    f = open(path_to_csv, "r")
    fString = f.read()
    fList = []
    for line in fString.split('\n'):
        fList.append(line.split(','))
    row_part = "('{}'"
    for c in range(0, num_cols - 1):
        row_part += ",'{}'"
    row_part += ")"
    conn = pymysql.connect(host='34.89.97.3', user='u831388458_covid19', password='Password@123',
                           db='u831388458_covid19stats')
    print("Opened database successfully")
    cursor = conn.cursor()
    query = "DELETE FROM " + table_name
    cursor.execute(query)
    conn.commit()
    rows = ""
    for i in range(1, len(fList) - 1):
        param_list = []
        for j in range(0, num_cols):
            param_list.append(str(fList[i][j]))
        rows += row_part.format(*param_list)
        if i != len(fList) - 2:
            rows += ','
    queryInsert = "INSERT INTO " + table_name + " VALUES " + rows
    cursor.execute(queryInsert)
    conn.commit()
    print("Uploaded")
    conn.close()

def update_notif_sheet(path_to_sheet):
    file_path = sys.argv[0].split('/')
    file_name = file_path[len(file_path) - 1]
    with open(path_to_sheet, 'r') as read_obj:
        csv_reader = list(csv.reader(read_obj))
        for row in csv_reader:
            if len(row) != 0 and row[0] == file_name:
                row[len(row) - 1] = datetime.datetime.now()
                break
    os.remove(path_to_sheet)
    with open(path_to_sheet, 'w', newline='\n') as write_obj:
        csv_writer = csv.writer(write_obj)
        csv_writer.writerows(csv_reader)


# # Chrome Path on local computer
# chrome_path = r"C:\webdrivers\chromedriver.exe"

# # Chrome path for the Database, will not change
# chrome_path = r"\usr\bin\chromedriver"

# Set where you want to download the CSV
preferences = {"download.default_directory": r"C:\50Hands\50HandsSprint3\OECD"}

#  Add Chrome options
options = webdriver.ChromeOptions()
options.EnsureCleanSession = True
options.add_argument("--incognito")
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shn-usage")
options.add_argument("--disable-notifications")
options.add_experimental_option("detach", True)
options.add_experimental_option("prefs", preferences)
browser = webdriver.Chrome(executable_path='C:\webdrivers\chromedriver.exe', options=options)


# This script is to get data for ALBERTA schools
browser = webdriver.Chrome("C:\webdrivers\chromedriver.exe")
# browser.maximize_window()
browser.get("https://www.compareschoolrankings.org/")
time.sleep(10)
# Click on the dropdown arrow
browser.find_element_by_xpath("/html/body/div/div[10]/div/div/form/div[1]").click()
time.sleep(2)
# Click on AB option in the dropdown
browser.find_element_by_xpath("/html/body/div/div[11]/div/div/div[1]").click()
time.sleep(5)
# Click on the List view
browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[1]/form/div/div[5]/div[4]/div/div[4]/button").click()
time.sleep(5)
#Show all checkbox
browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[1]/div[1]/div[1]/input").click()
time.sleep(5)

# Get table data from the first page for BC schools
tb = browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[2]/div/div/table").get_attribute('outerHTML')
tb_ab = pd.read_html(tb)
df_a1 = tb_ab[0].drop(['Add to compare', 'Notify mehelp'], axis=1)
df_a1['School Namekeyboard_arrow_down'].replace('', np.nan, inplace=True)
df_a1.dropna(subset=['School Namekeyboard_arrow_down'], inplace=True)
df_a1.insert(0, "Province", "Alberta", allow_duplicates=False)
df_a1.insert(2, "Score 2017-2018", " ", allow_duplicates=False)
df_a1.insert(3, "Rank 2017-2018", " ", allow_duplicates=False)

df_ab1 = df_a1.rename(columns = {'School Namekeyboard_arrow_down': 'School Name',
                                 'Score 2018 - 19keyboard_arrow_downhelp': 'Score 2018-2019',
                                 'Rank 2018 - 19keyboard_arrow_down': 'Rank 2018-2019',
                                 'Citykeyboard_arrow_down': 'City'}, inplace = False)
df_ab1['School Name'] = df_ab1['School Name'].str.replace(r"[\"\',]", '')
df_ab1['City'] = df_ab1['City'].str.replace(r"[\"\',]", '')

# For loop to get scrape tables on all pages
df_ab = []
for x in range(0, 17):
    browser.find_element_by_class_name("next").click()
    tb_ab = browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[2]/div/div/table").get_attribute('outerHTML')
    tb_ab2 = pd.read_html(tb_ab)
    df_a2 = tb_ab2[0].drop(['Add to compare', 'Notify mehelp'], axis=1)
    df_a2['School Namekeyboard_arrow_down'].replace('', np.nan, inplace=True)
    df_a2.dropna(subset=['School Namekeyboard_arrow_down'], inplace=True)
    df_a2.insert(0, "Province", "Alberta", allow_duplicates=False)
    df_a2.insert(2, "Score 2017-2018", " ", allow_duplicates=False)
    df_a2.insert(3, "Rank 2017-2018", " ", allow_duplicates=False)

    df_ab2 = df_a2.rename(columns={'School Namekeyboard_arrow_down': 'School Name',
                                   'Score 2018 - 19keyboard_arrow_downhelp': 'Score 2018-2019',
                                   'Rank 2018 - 19keyboard_arrow_down': 'Rank 2018-2019',
                                   'Citykeyboard_arrow_down': 'City'}, inplace=False)

    df_ab2['School Name'] = df_ab2['School Name'].str.replace(r"[\"\',]", '')
    df_ab2['City'] = df_ab2['City'].str.replace(r"[\"\',]", '')

    df_ab.append(df_ab2)
    df_abAll = df_ab1.append(df_ab)

browser.close()


# This script is to get data for BRITISH COLUMBIA schools
browser = webdriver.Chrome("C:\webdrivers\chromedriver.exe")
# browser.maximize_window()
browser.get("https://www.compareschoolrankings.org/")
time.sleep(10)
# browser.maximize_window()
# Click on the dropdown arrow
browser.find_element_by_xpath("/html/body/div/div[10]/div/div/form/div[1]").click()
time.sleep(2)
# Click on BC option in the dropdown
browser.find_element_by_xpath("/html/body/div/div[11]/div/div/div[2]").click()
time.sleep(5)
# Click on the List view
browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[1]/form/div/div[5]/div[4]/div/div[4]/button").click()
time.sleep(5)
#Show all checkbox
browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[1]/div[1]/div[1]/input").click()
time.sleep(5)

# Get table data from the first page for BC schools
tb = browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[2]/div/div/table").get_attribute('outerHTML')
tb_bc = pd.read_html(tb)
df_b1 = tb_bc[0].drop(['Add to compare', 'Notify mehelp'], axis=1)
df_b1['School Namekeyboard_arrow_down'].replace('', np.nan, inplace=True)
df_b1.dropna(subset=['School Namekeyboard_arrow_down'], inplace=True)
df_b1.insert(0, "Province", "British Columbia", allow_duplicates=False)
df_b1.insert(2, "Score 2017-2018", " ", allow_duplicates=False)
df_b1.insert(3, "Rank 2017-2018", " ", allow_duplicates=False)

df_bc1 = df_b1.rename(columns={'School Namekeyboard_arrow_down': 'School Name',
                               'Score 2017 - 18keyboard_arrow_downhelp': 'Score 2018-2019',
                                 'Rank 2017 - 18keyboard_arrow_down': 'Rank 2018-2019',
                               'Citykeyboard_arrow_down': 'City'}, inplace=False)

df_bc1['School Name'] = df_bc1['School Name'].str.replace(r"[\"\',]", '')
df_bc1['City'] = df_bc1['City'].str.replace(r"[\"\',]", '')

# For loop to get scrape tables on all pages
df_bc = []
for x in range(0, 19):
    browser.find_element_by_class_name("next").click()
    tb_bc = browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[2]/div/div/table").get_attribute('outerHTML')
    tb_bc2 = pd.read_html(tb_bc)
    df_b2 = tb_bc2[0].drop(['Add to compare', 'Notify mehelp'], axis=1)
    df_b2['School Namekeyboard_arrow_down'].replace('', np.nan, inplace=True)
    df_b2.dropna(subset=['School Namekeyboard_arrow_down'], inplace=True)
    df_b2.insert(0, "Province", "British Columbia", allow_duplicates=False)
    df_b2.insert(2, "Score 2017-2018", " ", allow_duplicates=False)
    df_b2.insert(3, "Rank 2017-2018", " ", allow_duplicates=False)

    df_bc2 = df_b2.rename(columns={'School Namekeyboard_arrow_down': 'School Name',
                                   'Score 2017 - 18keyboard_arrow_downhelp': 'Score 2018-2019',
                                   'Rank 2017 - 18keyboard_arrow_down': 'Rank 2018-2019',
                                   'Citykeyboard_arrow_down': 'City'}, inplace=False)

    df_bc2['School Name'] = df_bc2['School Name'].str.replace(r"[\"\',]", '')
    df_bc2['City'] = df_bc2['City'].str.replace(r"[\"\',]", '')
    df_bc.append(df_bc2)
    df_bcAll = df_bc1.append(df_bc)

browser.close()

# Append the AB and BC Tables together
df_ab_bc = df_abAll.append(df_bcAll)


# This script is to get data for ONTARIO schools
browser = webdriver.Chrome("C:\webdrivers\chromedriver.exe")
# browser.maximize_window()
browser.get("https://www.compareschoolrankings.org/")
time.sleep(5)
# Click on the dropdown arrow
browser.find_element_by_xpath("/html/body/div/div[10]/div/div/form/div[1]").click()
time.sleep(2)
# Click on Ontario option in the dropdown
browser.find_element_by_xpath("/html/body/div/div[11]/div/div/div[3]").click()
time.sleep(2)
# Click on the List view
browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[1]/form/div/div[5]/div[4]/div/div[4]/button").click()
time.sleep(2)
#Show all checkbox
browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[1]/div[1]/div[1]/input").click()
time.sleep(2)

# Get table data from the first page for BC schools
tb = browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[2]/div/div/table").get_attribute('outerHTML')
tb_on = pd.read_html(tb)
df_o1 = tb_on[0].drop(['Add to compare', 'Notify mehelp'], axis=1)
df_o1['School Namekeyboard_arrow_down'].replace('', np.nan, inplace=True)
df_o1.dropna(subset=['School Namekeyboard_arrow_down'], inplace=True)
df_o1.insert(0, "Province", "Ontario", allow_duplicates=False)
df_o1.insert(2, "Score 2017-2018", " ", allow_duplicates=False)
df_o1.insert(3, "Rank 2017-2018", " ", allow_duplicates=False)

df_on1 = df_o1.rename(columns={'School Namekeyboard_arrow_down': 'School Name',
                               'Score 2018 - 19keyboard_arrow_downhelp': 'Score 2018-2019',
                                'Rank 2018 - 19keyboard_arrow_down': 'Rank 2018-2019',
                               'Citykeyboard_arrow_down': 'City'}, inplace=False)

df_on1['School Name'] = df_on1['School Name'].str.replace(r"[\"\',]", '')
df_on1['City'] = df_on1['City'].str.replace(r"[\"\',]", '')

# For loop to get scrape tables on all pages
df_on = []
for x in range(0, 60):
    browser.find_element_by_class_name("next").click()
    tb_on = browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[2]/div/div/table").get_attribute('outerHTML')
    tb_on2 = pd.read_html(tb_on)
    df_o2 = tb_on2[0].drop(['Add to compare', 'Notify mehelp'], axis=1)
    df_o2['School Namekeyboard_arrow_down'].replace('', np.nan, inplace=True)
    df_o2.dropna(subset=['School Namekeyboard_arrow_down'], inplace=True)
    df_o2.insert(0, "Province", "Ontario", allow_duplicates=False)
    df_o2.insert(2, "Score 2017-2018", " ", allow_duplicates=False)
    df_o2.insert(3, "Rank 2017-2018", " ", allow_duplicates=False)

    df_on2 = df_o2.rename(columns={'School Namekeyboard_arrow_down': 'School Name',
                                   'Score 2018 - 19keyboard_arrow_downhelp': 'Score 2018-2019',
                                   'Rank 2018 - 19keyboard_arrow_down': 'Rank 2018-2019',
                                   'Citykeyboard_arrow_down': 'City'}, inplace=False)

    df_on2['School Name'] = df_on2['School Name'].str.replace(r"[\"\',]", '')
    df_on2['City'] = df_on2['City'].str.replace(r"[\"\',]", '')

    df_on.append(df_on2)
    df_onAll = df_on1.append(df_on)

browser.close()

# Append the AB, BC and ON Tables together
df_ab_bc_on = df_ab_bc.append(df_onAll)


# This script is to get data for Quebec schools
browser = webdriver.Chrome("C:\webdrivers\chromedriver.exe")
# browser.maximize_window()
browser.get("https://www.compareschoolrankings.org/")
time.sleep(5)
# Click on the dropdown arrow
browser.find_element_by_xpath("/html/body/div/div[10]/div/div/form/div[1]").click()
time.sleep(2)
# Click on Quebec option in the dropdown
browser.find_element_by_xpath("/html/body/div/div[11]/div/div/div[4]").click()
time.sleep(2)
# Click on the List view
browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[1]/form/div/div[5]/div[4]/div/div[4]/button").click()
time.sleep(2)
#Show all checkbox
browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[1]/div[1]/div[1]/input").click()
time.sleep(2)

# Get table data from the first page for BC schools
tb = browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[2]/div/div/table").get_attribute('outerHTML')
tb_qc = pd.read_html(tb)
df_q1 = tb_qc[0].drop(['Add to compare', 'Notify mehelp'], axis=1)
df_q1['School Namekeyboard_arrow_down'].replace('', np.nan, inplace=True)
df_q1.dropna(subset=['School Namekeyboard_arrow_down'], inplace=True)
df_q1.insert(0, "Province", "Quebec", allow_duplicates=False)
df_q1.insert(4, "Score 2018-2019", " ", allow_duplicates=False)
df_q1.insert(5, "Rank 2018-2019", " ", allow_duplicates=False)

df_qc1 = df_q1.rename(columns={'School Namekeyboard_arrow_down': 'School Name',
                               'Score 2017 - 18keyboard_arrow_downhelp': 'Score 2017-2018',
                               'Rank 2017 - 18keyboard_arrow_down': 'Rank 2017-2018',
                               'Citykeyboard_arrow_down': 'City'}, inplace = False)

df_qc1['School Name'] = df_qc1['School Name'].str.replace(r"[\"\',]", '')
df_qc1['City'] = df_qc1['City'].str.replace(r"[\"\',]", '')

# For loop to get scrape tables on all pages
df_qc = []
for x in range(0, 9):
    browser.find_element_by_class_name("next").click()
    tb_qc = browser.find_element_by_xpath("/html/body/div/div[14]/main/div[2]/div/div[3]/div/div/div[3]/div[2]/div/div/table").get_attribute('outerHTML')
    tb_qc2 = pd.read_html(tb_qc)
    df_q2 = tb_qc2[0].drop(['Add to compare', 'Notify mehelp'], axis=1)
    df_q2['School Namekeyboard_arrow_down'].replace('', np.nan, inplace=True)
    df_q2.dropna(subset=['School Namekeyboard_arrow_down'], inplace=True)
    df_q2.insert(0, "Province", "Quebec", allow_duplicates=False)
    df_q2.insert(4, "Score 2018-2019", " ", allow_duplicates=False)
    df_q2.insert(5, "Rank 2018-2019", " ", allow_duplicates=False)

    df_qc2 = df_q2.rename(columns={'School Namekeyboard_arrow_down': 'School Name',
                                   'Score 2017 - 18keyboard_arrow_downhelp': 'Score 2017-2018',
                                   'Rank 2017 - 18keyboard_arrow_down': 'Rank 2017-2018',
                                   'Citykeyboard_arrow_down': 'City'}, inplace=False)

    df_qc2['School Name'] = df_qc2['School Name'].str.replace(r"[\"\',]", '')
    df_qc2['City'] = df_qc2['City'].str.replace(r"[\"\',]", '')

    df_qc.append(df_qc2)
    df_qcAll = df_qc1.append(df_qc)

browser.close()


# Append dataframes for all 4 provinces together
df_all = df_ab_bc_on.append(df_qcAll)

# Convert the dataframe to CSV File
df_all.to_csv("S3_ca_fraser_school_ranking.csv", index=False)


# Call function to create and upload the table and to notify of changes
create_sql_table("S3_ca_fraser_school_ranking.csv", "S3_ca_fraser_school_ranking", "u831388458_covid19stats")
upload_to_sql("S3_ca_fraser_school_ranking.csv", "S3_ca_fraser_school_ranking")
# update_notif_sheet(path_to_sheet)