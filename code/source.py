######### IMPORT LIBRARY #########

import re
import requests
import unicodedata
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

######### DECODER FUNCTION #########

def restore_windows_1252_characters(restore_string):
    """
        Replace C1 control characters in the Unicode string s by the
        characters at the corresponding code points in Windows-1252,
        where possible.
    """

    def to_windows_1252(match):
        try:
            return bytes([ord(match.group(0))]).decode('windows-1252')
        except UnicodeDecodeError:
            # No character at the corresponding code point: remove it.
            return ''
        
    return re.sub(r'[\u0080-\u0099]', to_windows_1252, restore_string)

######### DEFINE SOUP + GET CODE #########

def load_response(url):
    with open("../input/content.pickle", 'rb') as f:
        response = pickle.load(f)
    return response.get(url)

def get_code(url):
    response_content = load_response(url)
    soup = BeautifulSoup(response, 'html.parser')
    for filing_document in soup.find_all('document'): 
        if soup.find('document').type.find(string=True, recursive=False).strip() == '10-K':
            document_code = filing_document.extract()
            document_text = filing_document.find('text').extract()
            break
        else: 
            next

    return document_code, document_text

######### SPLIT DOCUMENT #########

def split_doc(document_text):
    pages = {}

    # find page breaks
    all_thematic_breaks = document_text.find_all('hr')

    # convert breaks to string 
    all_thematic_breaks = [str(thematic_break) for thematic_break in all_thematic_breaks]

    # convert document text to string for splitting
    filing_doc_string = str(document_text)

    # for multi page docs
    if len(all_thematic_breaks) > 0:

        # define the regex delimiter pattern, this would just be all of our thematic breaks.
        regex_delimiter_pattern = '|'.join(map(re.escape, all_thematic_breaks))

        # split the document along each thematic breaks
        split_filing_string = re.split(regex_delimiter_pattern, filing_doc_string)

        # store the document itself
        pages = split_filing_string

    # handle the case where there are no thematic breaks.
    elif len(all_thematic_breaks) == 0:

        # handles so it will display correctly.
        split_filing_string = all_thematic_breaks

        # store the document as is, since there are no thematic breaks. In other words, no splitting.
        pages = split_filing_string = [filing_doc_string]
    
    return pages

######### CLEAN DOCUMENT #########

def clean(pages):
    
    # page length
    pages_length = len(pages)

    # initalize a dictionary that'll house our repaired html code for each page.
    repaired_pages = {}

    normalized_text = {}

    # loop through each page in that document.
    for index, page in enumerate(pages):

        # pass it through the parser
        page_soup = BeautifulSoup(page,'html5lib')
        
        # define the page number.
        page_number = index + 1

        # add the repaired html to the list. Also now we have a page number as the key.
        repaired_pages[page_number] = page_soup

        # grab all the text, notice I go to the BODY tag to do this
        page_text = page_soup.html.body.get_text(' ',strip = True)
    
        # normalize the text, remove messy characters. Additionally, restore missing window characters.
        page_text_norm = restore_windows_1252_characters(unicodedata.normalize('NFKD', page_text)) 
        
        # Additional cleaning steps, removing double spaces, and new line breaks.
        page_text_norm = page_text_norm.replace('\n',' ').replace('  ', ' ').replace('   ', ' ').replace('• ', ' ')

        #Save text in dictionary
        normalized_text[page_number] = page_text_norm

    return normalized_text, repaired_pages

######### FIX PAGE NUMBERS + EXTRACT INDEX #########

def fix_page_numbers(normalized_text, repaired_pages):

    # identify page number difference
    for ind, text in normalized_text.items():
        nospace = text.replace(' ', '')
        if re.search(r'partiitem1\.business', nospace, re.IGNORECASE):
            # Check if 'Part II Item 5.' is also present
            if re.search(r"partiiitem5.marketforregistrant's|partiiitem5\.marketfortheregistrant", nospace, re.IGNORECASE):
                index = text
                continue
        if re.search(r'Item7.Management’sDiscussionandAnalysisofFinancialConditionandResultsofOperations', nospace, re.IGNORECASE):
            last_space_index = text.rfind(' ')
            result = text[last_space_index + 1:]
            start_page = result
            start_index = ind
        
        if re.search(r'Item7A.QuantitativeandQualitativeDisclosuresAboutMarketRisk', nospace, re.IGNORECASE):
            last_space_index = text.rfind(' ')
            result = text[last_space_index + 1:]
            end_page = result
            end_index = ind

    return start_index, end_index

######### FIND PAGES OF MD&A #########

def find_pages(index):

    def extract_number_after_text(input_string, search_text):
        pattern = re.escape(search_text) + r'\s*(\d+)'
        # Search for the pattern in the input string
        match = re.search(pattern, input_string, re.IGNORECASE)
        
        # If a match is found, return the number (as a string), else return None
        if match:
            return match.group(1)  # match.group(1) contains the first captured group (the number)
        else:
            return None

    #sequence of characters for beginning
    search_beg = "Item 7. Management’s Discussion and Analysis of Financial Condition and Results of Operations"
    #sequence of characters for end
    search_end = "Item 7A. Quantitative and Qualitative Disclosures about Market Risk"

    #identify beginning and end of MD&A section
    beg = int(extract_number_after_text(index, search_beg))
    end = int(extract_number_after_text(index, search_end))   

    return beg, end

########## FIND, EXTRACT AND COMBINE MD&A SECTION ##########

def create_pattern_from_prompt(prompt):
    # Create a regex pattern to match the prompt with potential spacing issues
    return ''.join([f"{char}\\s*" for char in prompt])

def find_text_between_prompts(prompt1, prompt2, long_text):
    # Convert prompts into patterns that can match text with irregular spacing
    pattern1 = create_pattern_from_prompt(prompt1)
    pattern2 = create_pattern_from_prompt(prompt2)
    
    # Search for the first and second prompt in the long text
    match_for_prompt1 = re.search(pattern1, long_text, re.IGNORECASE)
    if match_for_prompt1:
        start_pos = match_for_prompt1.end()
        match_for_prompt2 = re.search(pattern2, long_text[start_pos:], re.IGNORECASE)
        if match_for_prompt2:
            end_pos = start_pos + match_for_prompt2.start()
            # Extract and return the text between the two prompts
            return long_text[start_pos:end_pos].strip()
    return long_text  # Return text as given if conditions are not met or prompts not found

def find_mda(repaired_pages, beg, end):

    # Generate Dictionary with only pages in MD&A
    md_and_a_dict = {key: value for key, value in repaired_pages.items() if beg <= key <= end}

    md_and_a = ''

    # Delete tables and normalize text
    for i, code in md_and_a_dict.items():
        for table in code.find_all('table'):
            table.decompose()

        # Normalize like before
        md_and_a_string = code.html.body.get_text(' ',strip = True)
        md_and_a_string = restore_windows_1252_characters(unicodedata.normalize('NFKD', md_and_a_string)) 
        md_and_a_string = md_and_a_string.replace('\n',' ').replace('  ', ' ').replace('   ', ' ').replace('• ', ' ')

        # Erase page number if present
        words = md_and_a_string.split()
        if words and words[-1].isdigit():
            words = words[:-1]
        md_and_a_string = ' '.join(words)

        #Compile strings
        md_and_a = md_and_a + ' ' + str(md_and_a_string)

    beg_spaces = 'Item 7. Management’s Discussion and Analysis of Financial Condition and Results of Operations'
    end_spaces = 'Item 7A. Quantitative and Qualitat'

    md_and_a = find_text_between_prompts(beg_spaces, end_spaces, md_and_a)
    md_and_a = md_and_a.replace('  ', ' ')

    return md_and_a
