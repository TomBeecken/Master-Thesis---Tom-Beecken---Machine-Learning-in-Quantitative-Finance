import pandas as pd
import requests
import json
import os
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

linkPath = "../input/allLinks.csv"
linkData = pd.read_csv(linkPath)
links = linkData['link'].tolist()
maxLinks = len(links)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
directory = '../results/content'

# Maximal zulässige Anfragen pro Sekunde
MAX_REQUESTS_PER_SECOND = 10

def download_link(url):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print(f'Read {len(response.content)} from {url}')
        save_content(url, response.content)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")

def download_all(urls: list) -> None:
    with ThreadPoolExecutor(max_workers=50) as executor:
        # tqdm um einen Fortschrittsbalken zu erstellen
        for _ in tqdm(executor.map(download_link, urls), total=len(urls)):
            pass

def save_content(url, content):
    try:
        with open(f"{directory}/{url.replace('/', '_')}", 'wb') as f:
            f.write(content)
    except Exception as e:
        print("File Saving did not work: ", e)

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

# Berechne die erforderliche Wartezeit zwischen den Anfragen, um maximal 10 Anfragen pro Sekunde zu gewährleisten
if links:
    start_time = time.time()
    for link in links:
        download_link(link)
        elapsed_time = time.time() - start_time
        if elapsed_time < 1 / MAX_REQUESTS_PER_SECOND:
            time.sleep(1 / MAX_REQUESTS_PER_SECOND - elapsed_time)
            start_time = time.time()
