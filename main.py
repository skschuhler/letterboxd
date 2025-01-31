import asyncio
import pandas as pd
from utils import *

# Running the asyncio event loop
async def main():
    # Read the list of usernames from the CSV
    users = pd.read_csv("data/popular_users.csv")['user'].to_list()

    # Start scraping users concurrently
    await scrape_multiple_users_concurrently(users)

if __name__ == "__main__":
    asyncio.run(main())

#combine into one csv
folder_path = 'data'  # Folder containing your CSV files
output_file = 'combined_data.csv'  # Name for the output combined CSV
combine_csv_files(folder_path, output_file)