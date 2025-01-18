# Letterboxd Scraping

A scraper to collect data from Letterboxd, including the ratings, review URLs, and likes of the 2500 most popular users.


## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/letterboxd.git
   cd letterboxd

2. Install dependencies
   ```bash
   pip install -r requirements.txt

## Data Structure

The **scraped data** contains the following columns:
- `Username`: The username of the user.
- `Title`: The title of the movie.
- `Rating`: The rating given by the user (from 1 to 5).
- `Liked`: Whether the user liked the movie (True/False).
- `Review URL`: URL of the review written by the user.
