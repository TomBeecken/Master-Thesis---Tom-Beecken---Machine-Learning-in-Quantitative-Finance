import pandas as pd
import requests
from requests.sessions import Session
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, local
import json
import pickle5 as pickle
from tqdm import tqdm

linkPath = "../input/allLinks.csv"
linkData = pd.read_csv(linkPath)
links = linkData['link'].tolist()
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
filename = "../results/content.pickle"

thread_local = local()

def get_session() -> Session:
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
    return thread_local.session

def download_link(url: str):
    session = get_session()
    try:
        with session.get(url, headers=headers) as response:
            print(f'Read {len(response.content)} from {url}')
            content[url] = response.content
    except requests.exceptions.ChunkedEncodingError as e:
        print(f"Error downloading {url}: {e}")

def download_all(urls: list) -> None:
    with ThreadPoolExecutor(max_workers=50) as executor:
        #tqdm um einen Fortschrittsbalken zu erstellen
        for url in tqdm(urls, desc="Downloading"):
            download_link(url)
            # Alle 50 URLS speichern
            if urls.index(url) % 15 == 0 and urls.index(url) != 0:
                save_content()

def load_content():
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        print("File Loading did not work: ", e)
        return {}

def save_content():
    try:
        with open(filename, 'wb') as f:
            pickle.dump(content, f, protocol=pickle.HIGHEST_PROTOCOL)
            print("Saved file")
    except Exception as e:
        print("File Saving did not work: ", e)
    
content = load_content()
print(len(content.keys()))
links = [link for link in links if link not in content.keys()]
download_all(links)