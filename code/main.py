import json
import pandas as pd
import source
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

######### CONTROL VARIABLES #########

linkPath = "../input/allLinks.csv"
docidtofirmPath = "../results/docidtofirm.csv"
documentsPath = "../results/documents.txt"
documentIdsPath = "../results/document_ids.txt"

######### IMPORT AND FORMAT DATA #########

linkData = pd.read_csv(linkPath).head(20)
links = linkData['link'].tolist()

# Improve efficiency by reading document IDs once
try:
    with open(documentIdsPath) as f:
        done_accnums = set(x.strip() for x in f.readlines())
    links = [link for accnum, link in zip(linkData['accessionNumber'], links) if str(accnum) not in done_accnums]
except FileNotFoundError:
    print("No previously processed document IDs found.")
print(f"Number of links to process: {len(links)}")

# DataFrame for document to firm mapping
docidtofirm = pd.DataFrame({
    'document_id': linkData['accessionNumber'],
    'firm_id': linkData['CompTick'],
    'date': linkData['reportDate'].astype(str).str[:4]
})
docidtofirm.to_csv(docidtofirmPath, index=False)

def process_document(link):
    try:
        relevant_data = linkData[linkData['link'] == link].iloc[0]
        accnum = relevant_data['accessionNumber']

        doc_code, doc_text = source.get_code(link)
        pages = source.split_doc(doc_text)
        normalized_text, repaired_pages = source.clean(pages)
        start_index, end_index = source.fix_page_numbers(normalized_text, repaired_pages)
        md_and_a = source.find_mda(repaired_pages, start_index, end_index)

        if md_and_a:
            with open(documentsPath, "a") as documents, open(documentIdsPath, "a") as document_ids:
                documents.write(md_and_a + '\n')
                document_ids.write(str(accnum) + '\n')

    except Exception as e:
        print(f"Error processing document {link}: {e}")

if __name__ == '__main__':
    with tqdm(total=len(links), desc="Processing Documents", unit="docs") as pbar:
        with Pool(cpu_count()) as pool:
            for _ in pool.imap_unordered(process_document, links):
                pbar.update(1)
