# import our libraries
import re
import requests
import source
import unicodedata
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from itertools import takewhile

######### CONTROL VARIABLES #########

error_table = pd.DataFrame(columns=['Ticker', 'Year', 'Reason', 'Explenation', 'Link'])

######### IMPORT AND FORMAT DATA #########

linkData = pd.read_csv('/Users/tombeecken/Desktop/Dissertation/Diss_Code/10-Ks/allLinks.csv')
links = linkData['link'].tolist()
name = linkData['CompName'].tolist()
tick = linkData['CompTick'].tolist()
accnum = linkData['accessionNumber'].tolist()
date = linkData['reportDate'].tolist()

########################
# Delete for full loop #
########################
links = links[10:40]
tick = tick[10:40]
accnum = accnum[10:40]
date = date[10:40]
########################

master_filings = pd.DataFrame({'ticker':tick, 'date':date, 'accessionNumber':accnum, 'md&a':np.nan})

count = 0

################### START LOOP ###################

for link in links:

    print('-'*80)
    print(f"Grabbing code for {tick[count]} {date[count][0:4]}")

    ################### GET CODE ###################

    try:
        doc_code, doc_text = source.get_code(link)
        print(f"Got code for {tick[count]} {date[count][0:4]}")
    except:
        print(f"Error 1 : Couldn't scrape code")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 1', 'Explanation':'Couldn\'t scrape code', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue
    
    ################### SPLIT DOCUMENT ###################
    try:
        pages = source.split_doc(doc_text)
        print(f"Split pages for {tick[count]} {date[count][0:4]}, we have {len(pages)} Pages!")
    except:
        print(f"Error 2 : Couldn't split pages")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 2', 'Explanation':'Couldn\'t split pages', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue

    ################### CLEAN + NORMALIZE DOCUMENT ###################

    try:
        normalized_text, repaired_pages = source.clean(pages)
        print(f"Normalized pages for {tick[count]} {date[count][0:4]}")
    except:
        print(f"Error 3 : Couldn't normalized pages")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 3', 'Explanation':'Couldn\'t normalized pages', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue

    ################### FIX PAGE NUMBERS + EXTRACT INDEX ###################

    try:
        normalized_text, repaired_pages, index, page_diff = source.fix_page_numbers(normalized_text, repaired_pages)
        print(f"Fixed page numbers for {tick[count]} {date[count][0:4]}, adjusted keys by {page_diff}")
    except:
        print(f"Error 4 : Couldn't fix page numbers")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 4', 'Explanation':'Couldn\'t fix page numbers', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue

    ################### FIND PAGES OF MD&A ###################

    try:
        beg, end = source.find_pages(index)
        print((f"MD&A is between page {beg} and {end} for {tick[count]} {date[count][0:4]}"))
    except:
        print(f"Error 5 : Couldn't find page numbers")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 5', 'Explanation':'Couldn\'t find page numbers', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue

    ################### FIND, EXTRACT AND COMBINE MD&A SECTION ###################

    try:
        md_and_a = source.find_mda(repaired_pages, beg, end)
        print(f'MD & A section for {tick[count]} {date[count][0:4]} sucessfully obtained!')
    except:
        print(f"Error 6 : Extracting MD & A failed")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 6', 'Explanation':'Extracting MD & A failed', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue
    
    ################### Insert section into DataFrame ###################

    master_filings.loc[master_filings['accessionNumber'] == accnum[count], 'md&a'] = md_and_a

    count += 1

################### EXPORT ###################

master_filings.to_excel('master.xlsx')
error_table.to_excel('errors.xlsx')