import pandas as pd
import requests
from requests.sessions import Session
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, local
import json
import pickle5 as pickle
from tqdm import tqdm
import os

linkPath = "../input/allLinks.csv"
linkData = pd.read_csv(linkPath)
links = linkData['link'].tolist()
maxLinks=len(links)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
directory = '../results/content'

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
            save_content(url, response.content)
    except requests.exceptions.ChunkedEncodingError as e:
        print(f"Error downloading {url}: {e}")

def download_all(urls: list) -> None:
    with ThreadPoolExecutor(max_workers=50) as executor:
        #tqdm um einen Fortschrittsbalken zu erstellen
        for _ in tqdm(executor.map(download_link, urls), total=len(urls)):
            pass

def save_content(url, content):
    try:
        with open(f"{directory}/{url.replace('/', '_')}", 'wb') as f:
            f.write(content)
            print("Saved file")
    except Exception as e:
        print("File Saving did not work: ", e)

def load_response(url):
    with open(f"{directory}/{url.replace('/', '_')}", 'rb') as f:
        content = f.read()
        return content

def save_to_do_links(data):
    try:
        with open("../results/links", 'w') as f:
            json.dump(data, f)
        print("Data saved to JSON file successfully.")
    except Exception as e:
        print("Failed to save data to JSON file:", e)

def load_urls():
    try:
        if os.listdir(directory):
            print("Data loaded from Directory.")
            return [x.replace("_", "/") for x in os.listdir(directory)]
        else:
            try:
                with open("../results/links", 'r') as f:
                    data = json.load(f)
                print("Data loaded from JSON file successfully.")
                return data
            except Exception as e:
                print("Failed to load data from JSON file:", e)
    except Exception as e:
        print("File Loading did not work: ", e)
        return {}

try:
    urls = load_urls()
    links = [link for link in links if link not in urls]
    save_to_do_links(links)
except:
    pass
download_all(links)