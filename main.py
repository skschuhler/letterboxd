import asyncio
import pandas as pd
from utils import *

# Running the asyncio event loop
async def main():
    # Read the list of usernames from the CSV
    users = pd.read_csv("C:/Users/sarah/Documents/GitHub/letterboxd-scraper/skipped_users.csv")['user'].to_list()

    # Start scraping users concurrently
    await scrape_multiple_users_concurrently(users)

if __name__ == "__main__":
    asyncio.run(main())