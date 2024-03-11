# import our libraries
import re
import requests
import source
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

######### CONTROL VARIABLES #########

error_table = pd.DataFrame(columns=['Ticker', 'Year', 'Reason', 'Explenation', 'Link'])
linkPath = "../input/allLinks.csv"
docidtofirmPath = "../results/docidtofirm.csv"
errorsPath= "../results/errors.csv"
documentsPath = "../results/documents.txt"
documentIdsPath= "../results/document_ids.txt"
######### IMPORT AND FORMAT DATA #########

linkData = pd.read_csv(linkPath)
links = linkData['link'].tolist()
name = linkData['CompName'].tolist()
tick = linkData['CompTick'].tolist()
accnum = linkData['accessionNumber'].tolist()
date = linkData['reportDate'].tolist()

########################
# Delete for full loop #
########################
links = links[10:20]
tick = tick[10:20]
accnum = accnum[10:20]
date = date[10:20]
year = [x[0:4] for x in date]

docidtofirm = pd.DataFrame({'document_id':accnum, 'firm_id':tick, 'date':year})

documents = open(documentsPath, "w")
document_ids = open(documentIdsPath, "w")

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
    except Exception as e:
        print(f"Error 2 : Couldn't split pages: {e}")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 2', 'Explanation':'Couldn\'t split pages', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue

    ################### CLEAN + NORMALIZE DOCUMENT ###################

    #try:
    normalized_text, repaired_pages = source.clean(pages)
    print(f"Normalized pages for {tick[count]} {date[count][0:4]}")
    # except:
    #     print(f"Error 3 : Couldn't normalized pages:")
    #     error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 3', 'Explanation':'Couldn\'t normalized pages', 'Link':links[count]}
    #     new_row = pd.DataFrame(data = error, index = [0])
    #     error_table = pd.concat([error_table, new_row], ignore_index = True)
    #     count += 1
    #     continue

    ################### GRAB INDEX NUMBERS ###################

    try:
        start_index, end_index = source.fix_page_numbers(normalized_text, repaired_pages)
        print(f"Found Index : Beginning = {start_index}, End = {end_index}")
    except:
        print(f"Error 4 : Couldn't find Index for MD&A")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 4', 'Explanation':'Couldn\'t fix page numbers', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue

    ################### FIND, EXTRACT AND COMBINE MD&A SECTION ###################

    try:
        md_and_a = source.find_mda(repaired_pages, start_index, end_index)
        print(f'MD & A section for {tick[count]} {date[count][0:4]} sucessfully obtained!')
    except:
        print(f"Error 6 : Extracting MD & A failed")
        error = {'Ticker':tick[count], 'Year':date[count][0:4], 'Reason':'Error 6', 'Explanation':'Extracting MD & A failed', 'Link':links[count]}
        new_row = pd.DataFrame(data = error, index = [0])
        error_table = pd.concat([error_table, new_row], ignore_index = True)
        count += 1
        continue
    
    ################### Insert section into DataFrame and text_file ###################

    if md_and_a != "":
        documents.write(md_and_a + '\n')
        document_ids.write(accnum[count] + '\n')

    count += 1

################### EXPORT ###################

documents.close()
docidtofirm.to_csv(docidtofirmPath)
error_table.to_csv(errorsPath)