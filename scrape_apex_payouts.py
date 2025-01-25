import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import cloudscraper
import random
import time
from generate_report import generate_html_report

# List of different headers to rotate
HEADERS_LIST = [
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    },
    {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15A372 Safari/604.1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    },
    {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    },
    # Add more headers as needed
]

# Maximum number of retries per request
MAX_RETRIES = 3

scraper = cloudscraper.create_scraper()

def scrape_apex_payouts():
    # Define the base URL
    base_url = "https://apextraderfunding.com/payouts"
    successful_pages = 0
    failed_pages = []
    start_date = None
    end_date = None
    
    # Define the last page constant (set to 100 for testing)
    LAST_PAGE = 50  # Change to 'last' to dynamically find the last page
    
    # Initialize a list to hold all payout data
    all_payouts_data = []
    
    # Determine the number of pages to scrape
    if isinstance(LAST_PAGE, int):
        total_pages = LAST_PAGE
    elif LAST_PAGE == 'last':
        # Scrape the first page to find the last page number
        headers = random.choice(HEADERS_LIST)
        response = scraper.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve the first page. Status code: {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        pagination = soup.find('div', class_='pagination')
        if pagination:
            # Find the last page link (usually has '>>' text)
            last_page_link = pagination.find_all('a')[-1]
            if last_page_link:
                # Extract page number from the href attribute
                href = last_page_link.get('href', '')
                import re
                if match := re.search(r'p=(\d+)', href):
                    total_pages = int(match.group(1))
                else:
                    total_pages = 1
            else:
                total_pages = 1
            print(f"Detected last page: {total_pages}")
        else:
            total_pages = 1
            print("Pagination not found. Assuming only 1 page exists.")
    else:
        print("Invalid LAST_PAGE value. It should be an integer or 'last'.")
        return None
    
    print(f"Starting to scrape {total_pages} pages...")
    
    # Iterate through each page
    for page in range(1, total_pages + 1):
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}?p={page}"
        
        print(f"Scraping page {page}: {url}")
        
        # Initialize retry count
        retry_count = 0
        success = False
        
        while retry_count < MAX_RETRIES and not success:
            # Choose a random header
            headers = random.choice(HEADERS_LIST)
            try:
                response = scraper.get(url, headers=headers)
                if response.status_code == 200:
                    success = True
                    soup = BeautifulSoup(response.text, 'html.parser')
                elif response.status_code == 403:
                    retry_count += 1
                    print(f"Received 403 on page {page}. Retrying ({retry_count}/{MAX_RETRIES}) with different headers.")
                    continue
                else:
                    print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                    break
            except Exception as e:
                retry_count += 1
                print(f"Error retrieving page {page}: {e}. Retrying ({retry_count}/{MAX_RETRIES})...")
                continue
        
        if not success:
            print(f"Skipping page {page} after {MAX_RETRIES} failed attempts.")
            failed_pages.append(page)
            continue
        
        successful_pages += 1
        
        # Extract payout data from the current page
        payouts_data = []
        
        for row in soup.find_all('div', class_='divTableRow'):
            cells = row.find_all('div', class_='divTableCell')
            if len(cells) >= 4:
                date_str = cells[0].get_text(strip=True)
                if not date_str:  # Check if date string is empty
                    continue
                # Update date range
                try:
                    # Parse date with abbreviated month format
                    current_date = datetime.strptime(date_str, '%b %d, %Y').date()
                    
                    if start_date is None or current_date > start_date:
                        start_date = current_date
                    if end_date is None or current_date < end_date:
                        end_date = current_date
                except ValueError:
                    pass
                
                trader = cells[1].get_text(strip=True)
                location = cells[2].get_text(strip=True)
                amount_str_raw = cells[3].get_text(strip=True)
                
                # Validate that the amount starts with '$'
                if not amount_str_raw.startswith('$'):
                    print(f"Skipping non-payout row with amount: {amount_str_raw}")
                    continue
                
                amount_str = amount_str_raw.replace('$', '').replace(',', '')
                
                try:
                    amount = float(amount_str)
                    payouts_data.append({
                        'Name': trader,
                        'Location': location,
                        'Amount': amount,
                        'Page': page
                    })
                except ValueError:
                    print(f"Skipping invalid amount: {amount_str}")
                    continue
        
        print(f"Found {len(payouts_data)} payouts on page {page}.")
        
        # Extend the main list with data from the current page
        all_payouts_data.extend(payouts_data)
        
        # Optional: Add a random delay between requests
        delay = random.uniform(1, 3)  # Delay between 1 to 3 seconds
        print(f"Sleeping for {delay:.2f} seconds to mimic human behavior.")
        time.sleep(delay)
    
    # Create DataFrame from all collected data
    df = pd.DataFrame(all_payouts_data)
    
    if df.empty:
        print("No payout data found across all pages.")
        return None
    
    # Save the raw scraped data to CSV
    df.to_csv('apex_payouts.csv', index=False, encoding='utf-8-sig')
    print("Saved raw payout data to 'apex_payouts.csv'.")
    
    # First, create a DataFrame with total earnings
    earnings_df = df.groupby(['Name', 'Location'], as_index=False)['Amount'].sum()
    earnings_df.rename(columns={'Amount': 'Total Earnings'}, inplace=True)
    
    # Then, create a DataFrame with the list of pages for each Name and Location
    pages_df = df.groupby(['Name', 'Location'])['Page'].agg(lambda x: sorted(list(set(x)))).reset_index()
    pages_df.rename(columns={'Page': 'Pages'}, inplace=True)
    
    # Merge the two DataFrames
    aggregated_df = pd.merge(earnings_df, pages_df, on=['Name', 'Location'])
    
    # Save the aggregated data to CSV
    aggregated_df.to_csv('aggregated_payouts.csv', index=False, encoding='utf-8-sig')
    print("Saved aggregated payout data to 'aggregated_payouts.csv'.")
    
    # Generate HTML report
    print("Generating HTML report...")
    generate_html_report(
        successful_pages=successful_pages,
        total_pages=total_pages,
        start_date=start_date,
        end_date=end_date,
        failed_pages=failed_pages
    )
    
    return aggregated_df


if __name__ == "__main__":
    print("Starting to scrape payout data...")
    aggregated_df = scrape_apex_payouts()
    if aggregated_df is not None and not aggregated_df.empty:
        print(f"Found {len(aggregated_df)} unique payouts")
        print("\nSample of aggregated data:")
        print(aggregated_df.head())
        print("\nHTML report has been generated as 'payout_report.html'")
    else:
        print("No payouts found.") 