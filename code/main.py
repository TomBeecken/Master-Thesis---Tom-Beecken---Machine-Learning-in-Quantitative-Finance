import json
import threading
import pandas as pd
import source
from tqdm import tqdm

######### CONTROL VARIABLES #########

linkPath = "../input/allLinks.csv"
docidtofirmPath = "../results/docidtofirm.csv"
errorsPath= "../results/errors.json"
documentsPath = "../results/documents.txt"
documentIdsPath= "../results/document_ids.txt"
######### IMPORT AND FORMAT DATA #########

linkData = pd.read_csv(linkPath)
linkData = linkData.head(20)
links = linkData['link'].tolist()
tick = linkData['CompTick'].tolist()
accnum = linkData['accessionNumber'].tolist()
date = linkData['reportDate'].tolist()
year = [str(x)[0:4] for x in date]


docidtofirm = pd.DataFrame({'document_id': accnum, 'firm_id': tick, 'date': year})
documents = open(documentsPath, "w")
document_ids = open(documentIdsPath, "w")

maxCount = len(links)

lock = threading.Lock()

def write_error(link, ticker, year, reason, explanation):
    error = {
        'Link': link,
        'Ticker': ticker,
        'Year': year,
        'Reason': reason,
        'Explanation': explanation
    }
    with lock:
        try:
            with open(errorsPath, 'r') as file:
                errors = json.load(file)
        except FileNotFoundError:
            errors = []
        errors.append(error)
        with open(errorsPath, 'w') as file:
            json.dump(errors, file, indent=4)

def process_document(link, count, pbar):
    global documents, document_ids

    try:
        doc_code, doc_text = source.get_code(link)
        pbar.set_description(f"Grabbing code for {tick[count]} {date[count][0:4]}")
    except Exception as e:
        print(f"Error 1 : Couldn't scrape code: {e}")
        write_error(link, tick[count], date[count][0:4], 'Error 1', 'Couldn\'t scrape code')
        return
    
    try:
        pages = source.split_doc(doc_text)
        pbar.set_description(f"Split pages for {tick[count]} {date[count][0:4]}, we have {len(pages)} Pages!")
    except Exception as e:
        print(f"Error 2 : Couldn't split pages: {e}")
        write_error(link, tick[count], date[count][0:4], 'Error 2', 'Couldn\'t split pages')
        return

    try:
        normalized_text, repaired_pages = source.clean(pages)
        pbar.set_description(f"Normalized pages for {tick[count]} {date[count][0:4]}")
    except Exception as e:
        print(f"Error 3 : Couldn't normalized pages: {e}")
        write_error(link, tick[count], date[count][0:4], 'Error 3', 'Couldn\'t normalized pages')
        return

    try:
        start_index, end_index = source.fix_page_numbers(normalized_text, repaired_pages)
        pbar.set_description(f"Found Index : Beginning = {start_index}, End = {end_index}")
    except Exception as e:
        print(f"Error 4 : Couldn't find Index for MD&A: {e}")
        write_error(link, tick[count], date[count][0:4], 'Error 4', 'Couldn\'t fix page numbers')
        return

    try:
        if start_index < end_index:
            md_and_a = source.find_mda(repaired_pages, start_index, end_index)
            pbar.set_description(f'MD & A section for {tick[count]} {date[count][0:4]} successfully obtained!')
    except Exception as e:
        print(f"Error 6 : Extracting MD & A failed: {e}")
        write_error(link, tick[count], date[count][0:4], 'Error 6', 'Extracting MD & A failed')
        return
    
    if md_and_a != "":
        with lock:
            documents.write(md_and_a + '\n')
            document_ids.write(accnum[count] + '\n')

    pbar.update(1)

with tqdm(total=maxCount) as pbar:
    threads = []
    for i, link in enumerate(links):
        thread = threading.Thread(target=process_document, args=(link, i, pbar))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

documents.close()
docidtofirm.to_csv(docidtofirmPath, index=False)
