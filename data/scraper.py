import re
import csv
import time
import json
import requests
import unicodedata
from tqdm import tqdm
from urllib.parse import urljoin
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor
from requests.packages.urllib3.util.retry import Retry


class DivyaPrabandamScraper:
    def __init__(self, base_url: str, build_id: str):
        self.base_url = base_url
        self.build_id = build_id
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _clean_string(self, s: str) -> str:
        """Clean and normalize strings"""
        if not s:
            return ""
        s = unicodedata.normalize('NFKC', str(s))
        cleaned = ''.join(char for char in s if char.isalpha() or char.isspace())
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned.strip()

    def _construct_url(self, path: str) -> str:
        """Construct full URL with build ID"""
        # Ensure path starts with /
        if not path.startswith('/'):
            path = '/' + path
        return f"{self.base_url}/_next/data/{self.build_id}{path}.json"

    def _fetch_json(self, url: str) -> Dict:
        """Fetch JSON data with error handling"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {str(e)}")
            return {}

    def find_url_paths(self, obj: Any, url_paths: List[str]) -> None:
        """Recursively find URL paths"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == 'url_path_clean':
                    if isinstance(value, str):
                        url_paths.append(value)
                else:
                    self.find_url_paths(value, url_paths)
        elif isinstance(obj, list):
            for item in obj:
                self.find_url_paths(item, url_paths)

    def _safe_join_scriptures(self, scriptures: Any) -> str:
        """Safely join scriptures list"""
        if not scriptures:
            return ""
        if isinstance(scriptures, list):
            return ",".join(str(s) for s in scriptures if s)
        if isinstance(scriptures, str):
            return scriptures
        return str(scriptures)

    def process_paasuram(self, url: str) -> Dict:
        """Process individual paasuram data"""
        try:
            data = self._fetch_json(url)
            if not data or 'pageProps' not in data:
                return {}
            
            props = data['pageProps']
            return {
                'number': str(props.get('number_full', '')),
                'tamil': str(props.get('pasuram_ta_c', '')),
                'tamil_clear': str(props.get('pasuram_ta', '')),
                'english_transliteration': str(props.get('pasuram_en', '')),
                'simple_english': str(props.get('simple_en', '')),
                'explanatory_notes': str(props.get('explanatory_notes_en', '')),
                'purport': str(props.get('purport_en', '')),
                'ragam': str(props.get('ragam', '')),
                'thalam': str(props.get('thalam', '')),
                'mood': str(props.get('mood', '')),
                'scriptures': self._safe_join_scriptures(props.get('scriptures', []))
            }
        except Exception as e:
            print(f"Error processing paasuram at {url}: {str(e)}")
            return {}

    def scrape_and_save(self, output_file: str):
        """Main function to scrape data and save to CSV"""
        print("Starting Divya Prabandham scraping process...")
        
        # Get initial data
        print("Fetching initial structure...")
        initial_data = self._fetch_json(self._construct_url('/divya-prabandam'))
        if not initial_data:
            raise Exception("Failed to fetch initial data")

        url_paths = []
        self.find_url_paths(initial_data, url_paths)
        if not url_paths:
            raise Exception("No URL paths found in initial data")

        # Get prabandam JSON files
        print("Collecting Prabandam URLs...")
        prabandam_urls = [self._construct_url(path.rstrip('/')) for path in url_paths if path]
        if not prabandam_urls:
            raise Exception("No Prabandam URLs generated")
        
        # Collect paasuram URLs with progress bar
        print("Analyzing Prabandam structure...")
        paasuram_urls = []
        for url in tqdm(prabandam_urls, desc="Processing Prabandams", unit="prabandam"):
            try:
                data = self._fetch_json(url)
                if not data or 'pageProps' not in data:
                    continue
                    
                descendants = data['pageProps'].get('descendants_list', [])
                if not descendants:
                    continue
                    
                prabandam_depth = len(descendants[-1][1:]) if descendants else 0
                for paasuram in descendants:
                    if not isinstance(paasuram, list):
                        continue
                    try:
                        path_parts = [str(part) for part in paasuram if part]
                        if not path_parts:
                            continue
                        path = "/divya-prabandam/" + "/".join(path_parts)
                        if "taniyan" not in path and "advanced" not in path:
                            if prabandam_depth == len(paasuram[1:]):
                                paasuram_urls.append(self._construct_url(path))
                    except Exception as e:
                        print(f"Error processing paasuram path: {str(e)}")
                        continue
            except Exception as e:
                print(f"Error processing prabandam URL {url}: {str(e)}")
                continue

        if not paasuram_urls:
            raise Exception("No Paasuram URLs found")

        total_paasurams = len(paasuram_urls)
        print(f"\nFound {total_paasurams} Paasurams to process")

        # Process paasurams with ThreadPoolExecutor and progress bar
        paasurams = []
        with tqdm(total=total_paasurams, desc="Downloading Paasurams", unit="paasuram") as pbar:
            def process_with_progress(url):
                result = self.process_paasuram(url)
                pbar.update(1)
                return result

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(process_with_progress, url) 
                          for url in paasuram_urls]
                for future in futures:
                    try:
                        result = future.result()
                        if result:
                            paasurams.append(result)
                    except Exception as e:
                        print(f"Error processing future: {str(e)}")
                        continue

        if not paasurams:
            raise Exception("No paasuram data collected")

        # Save to CSV with progress bar
        print("\nSaving data to CSV...")
        fieldnames = paasurams[0].keys()
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            with tqdm(total=len(paasurams), desc="Writing to CSV", unit="row") as pbar:
                for paasuram in paasurams:
                    writer.writerow(paasuram)
                    pbar.update(1)
        
        print(f"\nSuccessfully saved {len(paasurams)} paasurams to {output_file}")

def main():
    base_url = 'https://www.uveda.org'
    build_id = 'df6mzA8-_tkrR74hTz9Sc'
    output_file = 'nalayira_divya_prabandam.csv'
    
    # Add title banner
    print("\n" + "="*50)
    print("Nalayira Divya Prabandham Scraper")
    print("="*50 + "\n")
    
    start_time = time.time()
    
    scraper = DivyaPrabandamScraper(base_url, build_id)
    try:
        scraper.scrape_and_save(output_file)
        elapsed_time = time.time() - start_time
        print(f"\nTotal execution time: {elapsed_time:.2f} seconds")
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        print("Please check your internet connection and try again.")

if __name__ == "__main__":
    main()