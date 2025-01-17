import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict
from constants import HEADERS

def fetch_usernames_from_page(url: str) -> List[str]:
    """Helper function to fetch usernames from a given page URL."""
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    user_links = soup.find_all("a", class_="name")
    return [link.get("href").strip("/").split("/")[-1] for link in user_links]

def scrape_popular_users(num_users: int = 2500) -> List[str]:
    """Scrape a list of popular users from Letterboxd, ensuring the exact number of unique users."""
    page_number = 1  # Start with the first page
    unique_usernames = set()  # Set to store unique usernames

    # Base URL pattern for popular user pages
    base_url = "https://letterboxd.com/members/popular/page/{}/"
    
    # Use ThreadPoolExecutor to fetch pages concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        while len(unique_usernames) < num_users:
            # Generate the URL for the current page
            url = base_url.format(page_number)
            
            # Submit the page scraping task to the executor
            future = executor.submit(fetch_usernames_from_page, url)
            
            try:
                # Get the usernames from the current page
                usernames = future.result()
                
                # Add the usernames to the set (duplicates will be automatically handled)
                unique_usernames.update(usernames)
                
                # If we've collected enough unique users, break out of the loop
                if len(unique_usernames) >= num_users:
                    break

            except Exception as e:
                print(f"Error fetching data from {url}: {e}")
            
            # Increment the page number for the next iteration
            page_number += 1

    # Return the exact number of unique users requested, by slicing the set
    return list(unique_usernames)[:num_users]