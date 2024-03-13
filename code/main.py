import json  # Importing the json module for handling JSON data
import pandas as pd  # Importing pandas for data manipulation
import source  # Importing the source module, assumed to contain necessary functions
from multiprocessing import Pool, cpu_count  # Importing multiprocessing for parallel processing
from tqdm import tqdm  # Importing tqdm for displaying progress bars
import traceback  # Importing traceback module to print full error trace

######### CONTROL VARIABLES #########

# Define paths to input and output files
linkPath = "../input/allLinks.csv"
docidtofirmPath = "../results/docidtofirm.csv"
errorsPath = "../results/errors.json"
documentsPath = "../results/documents.txt"
documentIdsPath = "../results/document_ids.txt"

######### IMPORT AND FORMAT DATA #########

# Read input data from CSV file
linkData = pd.read_csv(linkPath)
linkData = linkData.head(20)  # Limiting data to first 20 rows for demonstration
links = linkData['link'].tolist()  # Extracting 'link' column and converting to list

try:
    #remove done links
    accnums = linkData['accessionNumber'].tolist()
    accslink = dict(zip(accnums, links))
    acnnmus_done = [str(x)[:-1] for x in open(documentIdsPath).readlines()]
    for accnum in acnnmus_done:
        if accnum in accslink:
            del accslink[accnum]
    links = list(accslink.values())
    #remove error links
    with open(errorsPath, 'r') as file:
        data = file.read()
        errors = json.loads(data)
        error_links = [error['Link'] for error in errors]
        links = [link for link in links if link not in error_links]
except:
    pass

# Creating DataFrame for document to firm mapping
docidtofirm = pd.DataFrame({
    'document_id': linkData['accessionNumber'],  # Extracting 'accessionNumber' column
    'firm_id': linkData['CompTick'],  # Extracting 'CompTick' column
    'date': [str(x)[0:4] for x in linkData['reportDate']]  # Extracting year from 'reportDate' column
})
docidtofirm.to_csv(docidtofirmPath, index=False)  # Writing docidtofirm DataFrame to CSV file

def write_error(link, tick, accnum, date, year, reason, explanation):
    """
    Function to write error information to errors.json file.

    Args:
    - link: Link associated with the error
    - tick: Ticker associated with the error
    - accnum: Accession number associated with the error
    - date: Date associated with the error
    - year: Year associated with the error
    - reason: Reason for the error
    - explanation: Explanation of the error
    """
    error = {
        'Link': link,
        'Ticker': tick,
        'Accnum': accnum,
        'Year': year,
        'Reason': reason,
        'Explanation': explanation
    }
    try:
        with open(errorsPath, 'r') as file:
            data = file.read()
            errors = json.loads(data) if data.strip() else []
    except FileNotFoundError:
        errors = []
    
    errors.append(error)
    
    try:
        with open(errorsPath, 'w') as file:
            json.dump(errors, file, indent=4)
    except:
        pass

def count_errors():
    try:
        with open(errorsPath, 'r') as file:
            data = file.read()
            errors = json.loads(data) if data.strip() else []
    except FileNotFoundError:
        return 0
    return len(errors)

def process_document(link):
    """
    Function to process document for a given link.

    Args:
    - link: Link of the document to be processed
    """
    try:
        # Extracting relevant information from linkData based on the link
        tick = linkData.loc[linkData['link'] == link, 'CompTick'].iloc[0]
        accnum = linkData.loc[linkData['link'] == link, 'accessionNumber'].iloc[0]
        date = str(linkData.loc[linkData['link'] == link, 'reportDate'].iloc[0])[0:4]
        year = str(linkData.loc[linkData['link'] == link, 'reportDate'].iloc[0])[0:4]

        # Check if accnum already exists in document_ids
        try:
            if str(accnum) in [str(x)[:-1] for x in open(documentIdsPath).readlines()]:
                return
        except:
            pass

        doc_code, doc_text = source.get_code(link)

        # Similar try-except blocks for subsequent steps of document processing
        pages = source.split_doc(doc_text)
        normalized_text, repaired_pages = source.clean(pages)
        start_index, end_index = source.fix_page_numbers(normalized_text, repaired_pages)
        md_and_a = source.find_mda(repaired_pages, start_index, end_index)
        
        if md_and_a != "":
            with open(documentsPath, "a") as documents, open(documentIdsPath, "a") as document_ids:
                documents.write(md_and_a + '\n')
                document_ids.write(str(accnum) + '\n')
    
    except Exception as e:
        traceback.print_exc()  # Print full traceback
        write_error(link, tick, accnum, date, year, 'Error', str(e))

# Create progress bar
maxCount = len(links)  # Maximum count for progress bar
if __name__ == '__main__':
    with tqdm(total=maxCount, desc="Processing Documents", unit="docs", position=0, leave=True, dynamic_ncols=True, ascii=True, mininterval=0.5, maxinterval=5.0, miniters=1, unit_scale=True, unit_divisor=1000, smoothing=0.3, bar_format="{l_bar}{bar}| ETA: {remaining} ") as pbar:
        with Pool(cpu_count()) as pool:
            for _ in tqdm(pool.imap_unordered(process_document, links), total=maxCount):
                pbar.update(1)
print(count_errors())