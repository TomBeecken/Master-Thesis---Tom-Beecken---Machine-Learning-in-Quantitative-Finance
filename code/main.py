import json  # Importing the json module for handling JSON data
import threading  # Importing the threading module for creating and managing threads
import pandas as pd  # Importing pandas for data manipulation
import source  # Importing the source module, assumed to contain necessary functions
from tqdm import tqdm  # Importing tqdm for displaying progress bars

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
#linkData = linkData.head(20) #Limiting data to first 20 rows for testing  
links = linkData['link'].tolist()  # Extracting 'link' column and converting to list

# Creating DataFrame for document to firm mapping
docidtofirm = pd.DataFrame({
    'document_id': linkData['accessionNumber'],  # Extracting 'accessionNumber' column
    'firm_id': linkData['CompTick'],  # Extracting 'CompTick' column
    'date': [str(x)[0:4] for x in linkData['reportDate']]  # Extracting year from 'reportDate' column
})
docidtofirm.to_csv(docidtofirmPath, index=False)  # Writing docidtofirm DataFrame to CSV file

# Opening files for writing documents and document IDs
documents = open(documentsPath, "w")
document_ids = open(documentIdsPath, "w")

maxCount = len(links)  # Maximum count for progress bar

lock = threading.Lock()  # Lock for thread-safe writing to errors.json

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
    with lock:
        try:
            with open(errorsPath, 'r') as file:
                errors = json.load(file)
        except FileNotFoundError:
            errors = []
        errors.append(error)
        with open(errorsPath, 'w') as file:
            json.dump(errors, file, indent=4)

def process_document(link, pbar):
    """
    Function to process document for a given link.

    Args:
    - link: Link of the document to be processed
    - pbar: Progress bar instance to track progress
    """
    global documents, document_ids

    # Extracting relevant information from linkData based on the link
    tick = linkData.loc[linkData['link'] == link, 'CompTick'].iloc[0]
    accnum = linkData.loc[linkData['link'] == link, 'accessionNumber'].iloc[0]
    date = str(linkData.loc[linkData['link'] == link, 'reportDate'].iloc[0])[0:4]
    year = str(linkData.loc[linkData['link'] == link, 'reportDate'].iloc[0])[0:4]

    try:
        doc_code, doc_text = source.get_code(link)
        pbar.set_description(f"Grabbing code for {tick} {date}")
    except Exception as e:
        print(f"Error 1 : Couldn't scrape code: {e}")
        write_error(link, tick, accnum, date, year, 'Error 1', 'Couldn\'t scrape code')
        return
    
    # Similar try-except blocks for subsequent steps of document processing
    try:
        pages = source.split_doc(doc_text)
        pbar.set_description(f"Split pages for {tick} {date}, we have {len(pages)} Pages!")
    except Exception as e:
        print(f"Error 2 : Couldn't split pages: {e}")
        write_error(link, tick, accnum, date, year, 'Error 2', 'Couldn\'t split pages')
        return

    try:
        normalized_text, repaired_pages = source.clean(pages)
        pbar.set_description(f"Normalized pages for {tick} {date}")
    except Exception as e:
        print(f"Error 3 : Couldn't normalized pages: {e}")
        write_error(link, tick, accnum, date, year, 'Error 3', 'Couldn\'t normalized pages')
        return

    try:
        start_index, end_index = source.fix_page_numbers(normalized_text, repaired_pages)
        pbar.set_description(f"Found Index : Beginning = {start_index}, End = {end_index}")
    except Exception as e:
        print(f"Error 4 : Couldn't find Index for MD&A: {e}")
        write_error(link, tick, accnum, date, year, 'Error 4', 'Couldn\'t fix page numbers')
        return

    try:
        if start_index < end_index:
            md_and_a = source.find_mda(repaired_pages, start_index, end_index)
            pbar.set_description(f'MD & A section for {tick} {date} successfully obtained!')
    except Exception as e:
        print(f"Error 6 : Extracting MD & A failed: {e}")
        write_error(link, tick, accnum, date, year, 'Error 6', 'Extracting MD & A failed')
        return
    
    if md_and_a != "":
        with lock:
            documents.write(md_and_a + '\n')
            document_ids.write(str(accnum) + '\n')

    pbar.update(1)

# Create progress bar
with tqdm(total=maxCount) as pbar:
    threads = []
    # Create and start threads for processing each link
    for link in links:
        thread = threading.Thread(target=process_document, args=(link, pbar))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

# Close files
documents.close()
document_ids.close()
