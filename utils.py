import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict
from constants import HEADERS
import aiohttp
import asyncio
import random
from tqdm import tqdm
import os

# --- Username Scraping ---
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

# --- Scrape film data ---

# Function to simulate a small delay between requests
async def async_sleep(seconds: float):
    await asyncio.sleep(seconds)

async def fetch_with_backoff(url: str, retries: int = 5) -> Optional[str]:
    """Fetch data with exponential backoff to handle rate-limiting."""
    attempt = 0
    errored = []
    while attempt < retries:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:
                    print(f"Fetching {url} - Status code: {response.status}")
                    if response.status == 200:
                        return await response.text()  # Return HTML content if successful
                    elif response.status == 429:  # Handle rate-limiting
                        print(f"Rate-limited on {url}, retrying after backoff...")
                        wait_time = random.uniform(1, 3) * (2 ** attempt)  # Exponential backoff
                        print(f"Waiting for {wait_time:.2f} seconds...")
                        await async_sleep(wait_time)  # Wait before retrying
                        attempt += 1
                    else:
                        print(f"Failed to fetch {url}, status: {response.status}")
                        errored.append(url)
                        return None
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                return None
    print(f"Max retries reached for {url}. Skipping.")
    print(errored)
    return None

# Async function to fetch pages (replacing requests)
async def get_total_pages(user_url: str) -> int:
    """
    Get the total number of pages for a user's film list.
    Args:
        user_url: The base URL of the user's films page.
    Returns:
        Total number of pages in the film list.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(user_url, headers=HEADERS, timeout=TIMEOUT) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    pagination = soup.find("div", class_="pagination")
                    if pagination:
                        last_page = pagination.find_all("a")[-1]
                        total_pages = int(last_page.text)
                        return total_pages
                    return 1
                else:
                    print(f"Error fetching total pages: {response.status}")
                    return 1
    except Exception as e:
        print(f"Error fetching total pages: {e}")
        return 1
    
# Async function to convert star rating into numeric values
def convert_star_rating(star_string: Optional[str]) -> Optional[float]:
    """
    Convert star rating strings (e.g., '★★★½') into numeric values (e.g., 3.5).
    Args:
        star_string: A string containing stars (★) and optional half-star (½).
    Returns:
        A float representing the numeric value of the rating, or None if input is invalid.
    """
    if not star_string:
        return None
    full_stars = star_string.count('★')
    return full_stars + 0.5 if '½' in star_string else full_stars

# Async function to extract film data
async def extract_film_data(film: BeautifulSoup) -> Dict[str, Optional[str]]:
    """
    Extract title, numeric rating, review URL, and "liked" status for a single film.
    Only return films that are rated or liked.
    
    Args:
        film: A BeautifulSoup object representing a film entry.
        
    Returns:
        A dictionary containing the film's title, numeric rating, review URL, and "liked" status.
    """
    title = film.find("img").get("alt")
    rating = film.find("span", class_="rating")
    rating_text = rating.text.strip() if rating else None
    numeric_rating = convert_star_rating(rating_text)
    review_link = film.find("a", class_="review-micro")
    review_url = f"https://letterboxd.com{review_link.get('href')}" if review_link else None
    liked = film.find("span", class_="like liked-micro has-icon icon-liked icon-16") is not None

    return {
        "Title": title,
        "Rating": numeric_rating,
        "Review URL": review_url,
        "Liked": liked
    }
    
# Async function to scrape a page of films
async def scrape_page(page_num: int, base_url: str) -> List[Dict[str, Optional[str]]]:
    """Scrape a single page of films asynchronously."""
    page_url = f"{base_url}/page/{page_num}/"
    print(f"Scraping page {page_num}...")
    html = await fetch_with_backoff(page_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    films = soup.find_all("li", class_="poster-container")

    # Extract basic data for each film
    film_data = [await extract_film_data(film) for film in films]
    return film_data

# Main function to extract user films and their details
async def extract_user_films(username: str) -> List[Dict[str, Optional[str]]]:
    """
    Extract all films and their details (title, rating, review URL) for a given user asynchronously.
    """
    base_url = f"https://letterboxd.com/{username}/films/"
    total_pages = await get_total_pages(base_url)
    all_films = []

    # Scrape pages concurrently
    tasks = [scrape_page(page_num, base_url) for page_num in range(1, total_pages + 1)]
    page_results = await asyncio.gather(*tasks)

    for page_data in page_results:
        all_films.extend(page_data)

    return all_films


# Function to save the data to a CSV file for each user
async def save_data_to_csv(user_data, username: str):
    """Save the user's data to a CSV file."""
    df = pd.DataFrame(user_data)
    filename = "data/{username}_film_data.csv"
    df.to_csv(filename, index=False)
    print(f"Data for {username} saved to {filename}")

# Function to process a single user
async def process_user(username: str):
    print(f"Scraping data for user: {username}")
    
    # Scrape the user's films
    user_data = await extract_user_films(username)

    # Save data to CSV
    await save_data_to_csv(user_data, username)

    # Simulate a delay between requests to prevent rate-limiting
    await async_sleep(random.uniform(3, 5))  # Sleep 3 to 5 seconds between users

# Function to split the user list into chunks of 4 users each
def chunk_users(user_list: List[str], chunk_size: int = 4) -> List[List[str]]:
    """Split the user list into chunks of 4 users each."""
    for i in range(0, len(user_list), chunk_size):
        yield user_list[i:i + chunk_size]

# Function to process a batch of 4 users concurrently
async def process_users_batch(user_batch: List[str]):
    """Process a batch of 4 users concurrently."""
    tasks = [process_user(username) for username in user_batch]
    await asyncio.gather(*tasks)  # Run all tasks concurrently

# Main function to manage concurrent scraping in batches of 4 users
async def scrape_multiple_users_concurrently(usernames: List[str]):
    # Split users into batches of 4 and process them
    for user_batch in chunk_users(usernames, 1):
        print(f"Processing batch of {len(user_batch)} users...")
        
        # Process each batch concurrently
        await process_users_batch(user_batch)

        # Sleep between batches to avoid hitting rate limits
        await async_sleep(3)  # Adjust the sleep time if necessary

def combine_csv_files(folder_path: str, output_file: str):
    # Get a list of all CSV files in the folder
    all_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    # Initialize an empty list to hold all DataFrames
    df_list = []
    
    # Initialize an empty list to track skipped (empty) files
    skipped_files = []

    # Loop through each CSV file and read it into a DataFrame
    for file in tqdm(all_files, desc="Processing CSV files", unit="file"):
        file_path = os.path.join(folder_path, file)
        try:
            df = pd.read_csv(file_path)
            
            # Check if the DataFrame is empty
            if df.empty:
                skipped_files.append(file)  # Add the empty file to skipped_files list
            else:
                # Extract the username from the filename (e.g., "username_film_data.csv")
                username = file.replace('_film_data.csv', '')

                # Add the "user" column with the username value
                df.insert(0, "user", username)  # Insert at the first position

                # Append the DataFrame to the list
                df_list.append(df)

        except pd.errors.EmptyDataError:
            # Handle the case where the file is empty
            print(f"Skipping empty file: {file}")
            skipped_files.append(file)  # Add the empty file to skipped_files list

    # Concatenate all non-empty DataFrames into one
    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)

        # Save the combined DataFrame to a new CSV file
        combined_df.to_csv(output_file, index=False)
        print(f"Combined CSV saved to {output_file}")
    else:
        print("No data to combine. All files were empty.")

    # Print the skipped files
    if skipped_files:
        print("Skipped the following empty files:")
        for skipped_file in skipped_files:
            print(skipped_file)