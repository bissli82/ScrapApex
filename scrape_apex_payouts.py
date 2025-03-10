import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import random
import time
from generate_report import generate_html_report
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os

# Create directories for data and reports if they don't exist
os.makedirs('data', exist_ok=True)
os.makedirs('reports', exist_ok=True)

# Maximum number of retries per request
MAX_RETRIES = 3

def get_selenium_driver(headless=True):
    """Initialize and return a Selenium WebDriver"""
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    
    # Add common options to make the browser more stealthy
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # Suppress console logging
    options.add_argument("--log-level=3")  # Only show fatal errors
    options.add_experimental_option('excludeSwitches', ['enable-logging'])  # Disable DevTools logging
    
    # Set a realistic user agent
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    
    # Add performance options
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-images")  # Disable image loading for faster page loads
    options.add_argument("--blink-settings=imagesEnabled=false")  # Another way to disable images
    options.add_argument("--disable-webgl")  # Disable WebGL to avoid related errors
    
    # Initialize the driver without using ChromeDriverManager
    try:
        # First try to use the driver directly
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Error initializing Chrome driver: {e}")
        print("Trying alternative method...")
        try:
            # Try using the Service class without ChromeDriverManager
            driver = webdriver.Chrome(options=options)
        except Exception as e2:
            print(f"Second attempt failed: {e2}")
            print("Please make sure Chrome and chromedriver are installed and compatible.")
            raise
    
    # Execute CDP commands to make the browser more stealthy
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    # Set page load strategy to eager for faster loading
    driver.execute_cdp_cmd('Page.setDownloadBehavior', {'behavior': 'deny'})
    
    return driver

# Add this function to check if a page is problematic
def is_problematic_page(page_num):
    """Check if a page is known to be problematic"""
    # List of known problematic pages
    problematic_pages = []  # Add more page numbers if you discover others
    return page_num in problematic_pages

def scrape_single_page(page_info):
    """Scrape a single page using Selenium and return its data"""
    page, base_url = page_info
    
    # Skip known problematic pages
    if is_problematic_page(page):
        print(f"Skipping known problematic page {page}")
        return page, []  # Return empty data but don't mark as failure
    
    if page == 1:
        url = base_url
    else:
        url = f"{base_url}?p={page}"
    
    # Only print detailed info for first page
    if page == 1:
        print(f"Scraping page {page}: {url}")
    
    retry_count = 0
    success = False
    payouts_data = []
    
    # Create a new driver for each page to avoid session tracking
    driver = get_selenium_driver(headless=True)
    
    try:
        # Set a timeout for the entire operation
        driver.set_page_load_timeout(30)
        
        # Navigate to the URL
        driver.get(url)
        
        # Wait for the page to load - reduced wait time
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Wait for table content to load
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tr, .divTableRow"))
            )
        except:
            # If we can't find table rows, the page might be empty or have a different structure
            pass
        
        # Reduced delay
        time.sleep(random.uniform(0.5, 1))
        
        # Save the page source for debugging only on first page
        if page == 1:
            with open(f"page_{page}_selenium.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print(f"Saved Selenium HTML to page_{page}_selenium.html")
        
        # Parse the page with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Debug: Print the HTML structure only on first page
        if page == 1:
            print("Page structure:")
            print(soup.prettify()[:1000])
        
        # Try different possible table structures
        table = (
            soup.find('div', class_='divTable') or 
            soup.find('table', class_='payout-table') or
            soup.find('table')
        )
        
        if page == 1:  # Only log for first page
            if table:
                print(f"Found table element: {table.name} with classes: {table.get('class', [])}")
            else:
                print("No table element found")
        
        # Try multiple possible row structures
        rows = (
            soup.find_all('div', class_='divTableRow') or
            soup.find_all('tr', class_='payout-row') or
            soup.select('table tr') or
            soup.select('.payout-table tr')
        )
        
        # Check if we have any rows
        if len(rows) <= 1:  # Only header or no rows
            print(f"Warning: Page {page} has no data rows, retrying with longer wait...")
            # Try again with longer wait
            time.sleep(random.uniform(3, 5))
            
            # Refresh the page
            driver.refresh()
            
            # Wait longer for content
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Parse again
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Try to find rows again
            rows = (
                soup.find_all('div', class_='divTableRow') or
                soup.find_all('tr', class_='payout-row') or
                soup.select('table tr') or
                soup.select('.payout-table tr')
            )
        
        # Only print row details for first page
        if page == 1:
            print(f"Found {len(rows)} potential data rows")
            
            # Only print first 5 rows for first page
            for i, row in enumerate(rows[:5]):
                # Try both div cells and td elements
                cells = row.find_all('div', class_='divTableCell') or row.find_all('td')
                
                if len(cells) >= 4:
                    date_str = cells[0].get_text(strip=True)
                    if not date_str:
                        continue
                        
                    trader = cells[1].get_text(strip=True)
                    location = cells[2].get_text(strip=True)
                    amount_str_raw = cells[3].get_text(strip=True)
                    
                    if not amount_str_raw.startswith('$'):
                        continue
                        
                    amount_str = amount_str_raw.replace('$', '').replace(',', '')
                    
                    try:
                        amount = float(amount_str)
                        payouts_data.append({
                            'Name': trader,
                            'Location': location,
                            'Amount': amount,
                            'Page': page,
                            'Date': date_str
                        })
                    except ValueError:
                        continue
                    
                    print(f"Sample row {i+1}: Date={date_str}, Trader={trader}, Location={location}, Amount={amount_str_raw}")
        else:
            # For other pages, just process silently
            for row in rows:
                # Try both div cells and td elements
                cells = row.find_all('div', class_='divTableCell') or row.find_all('td')
                
                if len(cells) >= 4:
                    date_str = cells[0].get_text(strip=True)
                    if not date_str:
                        continue
                        
                    trader = cells[1].get_text(strip=True)
                    location = cells[2].get_text(strip=True)
                    amount_str_raw = cells[3].get_text(strip=True)
                    
                    if not amount_str_raw.startswith('$'):
                        continue
                        
                    amount_str = amount_str_raw.replace('$', '').replace(',', '')
                    
                    try:
                        amount = float(amount_str)
                        payouts_data.append({
                            'Name': trader,
                            'Location': location,
                            'Amount': amount,
                            'Page': page,
                            'Date': date_str
                        })
                    except ValueError:
                        continue
        
        success = True
        
    except Exception as e:
        if page == 1:  # Only print detailed errors for first page
            print(f"Error scraping page {page}: {e}")
    finally:
        # Always close the driver to free resources
        try:
            driver.quit()
        except Exception as e:
            if page == 1:  # Only print for first page
                print(f"Error closing driver for page {page}: {e}")
    
    # Reduced delay between requests
    time.sleep(random.uniform(0.2, 0.8))
    return page, payouts_data if success else None

def retry_scrape_page(page_info):
    """Retry scraping a failed page with different settings"""
    page, base_url = page_info
    if page == 1:
        url = base_url
    else:
        url = f"{base_url}?p={page}"
    
    print(f"Retrying page {page}: {url}")
    
    # Use different settings for retry
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # Use a completely different user agent for retry
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/115.0.1901.203 Safari/537.36"
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    
    # Disable logging
    options.add_argument("--log-level=3")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    # Performance options
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-images")
    
    payouts_data = []
    success = False
    
    try:
        # Create a new driver for retry
        driver = webdriver.Chrome(options=options)
        
        # Set a longer timeout for retries
        driver.set_page_load_timeout(45)
        
        # Navigate to the URL
        driver.get(url)
        
        # Wait longer for the page to load on retry
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Add a longer delay for retry
        time.sleep(random.uniform(3, 5))
        
        # Parse the page with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Try multiple possible row structures
        rows = (
            soup.find_all('div', class_='divTableRow') or
            soup.find_all('tr', class_='payout-row') or
            soup.select('table tr') or
            soup.select('.payout-table tr')
        )
        
        print(f"Retry found {len(rows)} potential data rows")
        
        for row in rows:
            # Try both div cells and td elements
            cells = row.find_all('div', class_='divTableCell') or row.find_all('td')
            
            if len(cells) >= 4:
                date_str = cells[0].get_text(strip=True)
                if not date_str:
                    continue
                    
                trader = cells[1].get_text(strip=True)
                location = cells[2].get_text(strip=True)
                amount_str_raw = cells[3].get_text(strip=True)
                
                if not amount_str_raw.startswith('$'):
                    continue
                    
                amount_str = amount_str_raw.replace('$', '').replace(',', '')
                
                try:
                    amount = float(amount_str)
                    payouts_data.append({
                        'Name': trader,
                        'Location': location,
                        'Amount': amount,
                        'Page': page,
                        'Date': date_str
                    })
                except ValueError:
                    continue
        
        success = True
        
    except Exception as e:
        print(f"Error in retry for page {page}: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass
    
    # Longer delay between retries
    time.sleep(random.uniform(2, 4))
    return page, payouts_data if success else None

def scrape_apex_payouts():
    # Define the base URL
    base_url = "https://apextraderfunding.com/payouts"
    successful_pages = 0
    failed_pages = []
    start_date = None
    end_date = None
    
    # Initialize a list to hold all payout data
    all_payouts_data = []
    
    # Counter for periodic report updates
    pages_since_last_report = 0
    
    # Adaptive batch size parameters
    initial_batch_size = 10
    current_batch_size = initial_batch_size
    max_batch_size = 100
    consecutive_successes = 0
    consecutive_failures = 0
    
    # Add a counter for consecutive warnings about the same page
    consecutive_same_page_warnings = 0
    last_warned_page = None
    
    # First, determine the total number of pages
    print("Determining total number of pages...")
    driver = get_selenium_driver(headless=True)
    try:
        # Navigate to the first page
        driver.get(base_url)
        
        # Wait for the page to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Add a random delay to simulate human behavior
        time.sleep(random.uniform(1, 2))
        
        # Parse the page with BeautifulSoup to find pagination
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Look for pagination elements
        pagination = soup.find('div', class_='pagination') or soup.find('ul', class_='pagination')
        
        if pagination:
            # Find all page links
            page_links = pagination.find_all('a')
            last_page = 1
            
            # Try to find the highest page number
            for link in page_links:
                try:
                    page_num = int(link.text.strip())
                    if page_num > last_page:
                        last_page = page_num
                except ValueError:
                    continue
                    
            # Also check for "Last" link which might contain the last page in its href
            last_links = [link for link in page_links if 'last' in link.text.lower() or '>>' in link.text]
            if last_links:
                for link in last_links:
                    href = link.get('href', '')
                    import re
                    if match := re.search(r'p=(\d+)', href):
                        potential_last = int(match.group(1))
                        if potential_last > last_page:
                            last_page = potential_last
            
            print(f"Found {last_page} pages to scrape")
        else:
            print("No pagination found. Assuming only 1 page exists.")
            last_page = 1
    except Exception as e:
        print(f"Error determining page count: {e}")
        print("Defaulting to 1 page")
        last_page = 1
    finally:
        driver.quit()
    
    # Prepare the list of pages to scrape
    pages_to_scrape = [(page, base_url) for page in range(1, last_page + 1)]
    
    # Add progress tracking variables
    total_pages = len(pages_to_scrape)
    completed_pages = 0
    start_time = time.time()
    
    # Use ThreadPoolExecutor with adaptive batch size
    max_workers = current_batch_size
    
    print(f"Starting to scrape {total_pages} pages in parallel (initial batch size: {max_workers})...")
    print(f"Time started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Reports will be updated after every 10 successful pages")
    print(f"Batch size will adapt based on success/failure rate")
    
    # Track failed pages for retry
    failed_pages_batch = []
    
    # Track batch size history for reporting
    batch_size_history = [(0, current_batch_size)]  # (completed_pages, batch_size)
    
    # Add a timeout mechanism to prevent getting stuck
    max_wait_time = 300  # 5 minutes max wait time for any remaining futures
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit initial batch of pages
        pages_to_process = pages_to_scrape[:current_batch_size]
        remaining_pages = pages_to_scrape[current_batch_size:]
        
        # Map of futures to page numbers
        future_to_page = {executor.submit(scrape_single_page, page_info): page_info[0] 
                         for page_info in pages_to_process}
        
        # Process pages adaptively
        while future_to_page:
            try:
                # Wait for the next future to complete with a timeout
                done, pending = wait(future_to_page, timeout=60, return_when=FIRST_COMPLETED)
                
                # If no futures completed within the timeout, log and check if we should force continue
                if not done:
                    print(f"\nWARNING: No pages completed in the last 60 seconds. {len(future_to_page)} pages still pending.")
                    pending_pages = sorted(future_to_page.values())
                    print(f"Pending pages: {pending_pages}")
                    
                    # Check if we're stuck on the same page
                    if len(pending_pages) == 1 and pending_pages[0] == last_warned_page:
                        consecutive_same_page_warnings += 1
                    else:
                        consecutive_same_page_warnings = 1
                        last_warned_page = pending_pages[0] if pending_pages else None
                    
                    # If we've warned about the same page 3 times, cancel it immediately
                    if consecutive_same_page_warnings >= 3 and last_warned_page is not None:
                        print(f"\nALERT: Stuck on page {last_warned_page} for too long. Cancelling this task.")
                        for future, page_num in list(future_to_page.items()):
                            if page_num == last_warned_page:
                                future.cancel()
                                future_to_page.pop(future)
                                failed_pages.append(page_num)
                                print(f"Cancelled page {page_num} to prevent deadlock")
                                consecutive_same_page_warnings = 0
                                last_warned_page = None
                                break
                else:
                    # Update the last progress time when we make progress
                    scrape_apex_payouts.last_progress_time = time.time()
                
                # Process completed futures
                for future in done:
                    page = future_to_page.pop(future)
                    try:
                        page_num, page_data = future.result()
                        completed_pages += 1
                        
                        # Calculate progress and ETA
                        elapsed_time = time.time() - start_time
                        pages_per_second = completed_pages / elapsed_time if elapsed_time > 0 else 0
                        remaining_count = total_pages - completed_pages
                        eta_seconds = remaining_count / pages_per_second if pages_per_second > 0 else 0
                        eta_str = str(timedelta(seconds=int(eta_seconds)))
                        
                        # Print progress information
                        progress_pct = (completed_pages / total_pages) * 100
                        print(f"[{progress_pct:.1f}% | {completed_pages}/{total_pages} | ETA: {eta_str} | Batch: {current_batch_size}] ", end="")
                        
                        if page_data is not None:
                            successful_pages += 1
                            pages_since_last_report += 1
                            records_count = len(page_data)
                            all_payouts_data.extend(page_data)
                            
                            # Update date range from the page data
                            for row in page_data:
                                try:
                                    current_date = datetime.strptime(row['Date'], '%b %d, %Y').date()
                                    if start_date is None or current_date > start_date:
                                        start_date = current_date
                                    if end_date is None or current_date < end_date:
                                        end_date = current_date
                                except ValueError:
                                    pass
                            
                            print(f"Page {page}: SUCCESS ({records_count} records)")
                            
                            # Adaptive batch size - increase on success
                            consecutive_successes += 1
                            consecutive_failures = 0
                            if consecutive_successes >= 5 and current_batch_size < max_batch_size:
                                old_batch_size = current_batch_size
                                current_batch_size = min(current_batch_size * 2, max_batch_size)
                                if old_batch_size != current_batch_size:
                                    print(f"\nIncreasing batch size from {old_batch_size} to {current_batch_size} after 5 consecutive successes")
                                    batch_size_history.append((completed_pages, current_batch_size))
                                    consecutive_successes = 0
                            
                            # Generate interim report after every 10 successful pages
                            if pages_since_last_report >= 10:
                                # Create an interim DataFrame for the report
                                interim_df = pd.DataFrame(all_payouts_data)
                                
                                # First save interim data to CSV
                                interim_df.to_csv('data/apex_payouts_interim.csv', index=False, encoding='utf-8-sig')
                                
                                # Then generate HTML report with interim data
                                print(f"\nUpdating HTML report after {successful_pages} successful pages...")
                                
                                # Create interim aggregated data for the report
                                if not interim_df.empty:
                                    # Create a DataFrame with total earnings
                                    interim_earnings_df = interim_df.groupby(['Name', 'Location'], as_index=False)['Amount'].sum()
                                    interim_earnings_df.rename(columns={'Amount': 'Total Earnings'}, inplace=True)
                                    
                                    # Create a DataFrame with the list of pages for each Name and Location
                                    interim_pages_df = interim_df.groupby(['Name', 'Location'])['Page'].agg(lambda x: sorted(list(set(x)))).reset_index()
                                    interim_pages_df.rename(columns={'Page': 'Pages'}, inplace=True)
                                    
                                    # Merge the two DataFrames
                                    interim_aggregated_df = pd.merge(interim_earnings_df, interim_pages_df, on=['Name', 'Location'])
                                    
                                    # Save interim aggregated data
                                    interim_aggregated_df.to_csv('data/aggregated_payouts_interim.csv', index=False, encoding='utf-8-sig')
                                
                                generate_html_report(
                                    csv_file='data/aggregated_payouts_interim.csv',  # Use aggregated interim data
                                    successful_pages=successful_pages,
                                    total_pages=total_pages,
                                    start_date=start_date,
                                    end_date=end_date,
                                    failed_pages=failed_pages,
                                    is_interim=True,
                                    current_progress=progress_pct,
                                    batch_size_history=batch_size_history,
                                    current_batch_size=current_batch_size
                                )
                                
                                # Reset the counter
                                pages_since_last_report = 0
                                print(f"Interim report generated. Continuing scraping...")
                        else:
                            failed_pages.append(page)
                            failed_pages_batch.append((page, base_url))
                            print(f"Page {page}: FAILED - will retry later")
                            
                            # Adaptive batch size - decrease on failure
                            consecutive_failures += 1
                            consecutive_successes = 0
                            if consecutive_failures >= 2 and current_batch_size > initial_batch_size:
                                old_batch_size = current_batch_size
                                current_batch_size = max(current_batch_size // 2, initial_batch_size)
                                if old_batch_size != current_batch_size:
                                    print(f"\nDecreasing batch size from {old_batch_size} to {current_batch_size} after 2 consecutive failures")
                                    batch_size_history.append((completed_pages, current_batch_size))
                                    consecutive_failures = 0
                    except Exception as e:
                        completed_pages += 1
                        failed_pages.append(page)
                        failed_pages_batch.append((page, base_url))
                        print(f"Page {page}: ERROR - {str(e)[:100]}... - will retry later")
                        
                        # Adaptive batch size - decrease on error
                        consecutive_failures += 1
                        consecutive_successes = 0
                        if consecutive_failures >= 2 and current_batch_size > initial_batch_size:
                            old_batch_size = current_batch_size
                            current_batch_size = max(current_batch_size // 2, initial_batch_size)
                            if old_batch_size != current_batch_size:
                                print(f"\nDecreasing batch size from {old_batch_size} to {current_batch_size} after 2 consecutive errors")
                                batch_size_history.append((completed_pages, current_batch_size))
                                consecutive_failures = 0
                
                # Submit a new page if there are any remaining
                if remaining_pages:
                    # Submit more pages up to the current batch size
                    pages_to_add = min(len(remaining_pages), current_batch_size - len(future_to_page))
                    if pages_to_add > 0:
                        for i in range(pages_to_add):
                            if remaining_pages:
                                next_page = remaining_pages.pop(0)
                                future = executor.submit(scrape_single_page, next_page)
                                future_to_page[future] = next_page[0]
            
            except Exception as e:
                print(f"\nError in main scraping loop: {e}")
                # Try to continue with remaining futures
                continue
    
    # Add this after the main scraping loop completes
    if future_to_page:
        print(f"\nScraping completed but {len(future_to_page)} pages are still pending.")
        print(f"Pending pages: {sorted(future_to_page.values())}")
        print("Cancelling remaining tasks to complete the process...")
        
        # Cancel all remaining futures
        for future in list(future_to_page.keys()):
            page = future_to_page.pop(future)
            future.cancel()
            failed_pages.append(page)
            print(f"Cancelled page {page} to finalize the process")

    # Retry failed pages one by one
    if failed_pages:
        print("\n" + "="*70)
        print(f"RETRYING {len(failed_pages)} FAILED PAGES")
        print("="*70)
        
        # Create a copy of failed pages to iterate through
        pages_to_retry = sorted(list(set(failed_pages)))
        retry_successful = []
        
        for page in pages_to_retry:
            print(f"Retrying page {page}...")
            try:
                # Try with a longer timeout and more retries
                page_num, page_data = scrape_single_page((page, base_url))
                
                if page_data is not None and len(page_data) > 0:
                    print(f"Retry successful for page {page} ({len(page_data)} records)")
                    all_payouts_data.extend(page_data)
                    retry_successful.append(page)
                    successful_pages += 1
                    
                    # Update date range from the page data
                    for row in page_data:
                        try:
                            current_date = datetime.strptime(row['Date'], '%b %d, %Y').date()
                            if start_date is None or current_date > start_date:
                                start_date = current_date
                            if end_date is None or current_date < end_date:
                                end_date = current_date
                        except ValueError:
                            pass
                else:
                    print(f"Retry failed for page {page} - no data returned")
            except Exception as e:
                print(f"Retry failed for page {page}: {str(e)[:100]}...")
        
        # Remove successfully retried pages from failed_pages list
        for page in retry_successful:
            while page in failed_pages:
                failed_pages.remove(page)
        
        print(f"Retry results: {len(retry_successful)} pages recovered, {len(failed_pages)} pages still failed")

    # Print completion information
    total_time = time.time() - start_time
    print(f"\nScraping completed in {timedelta(seconds=int(total_time))}")
    print(f"Success rate: {successful_pages}/{total_pages} ({successful_pages/total_pages*100:.1f}%)")
    print(f"Final batch size: {current_batch_size}")
    print("\nBatch size history:")
    for pages_completed, batch_size in batch_size_history:
        print(f"  After {pages_completed} pages: {batch_size}")

    # Create DataFrame from all collected data
    df = pd.DataFrame(all_payouts_data)
    
    if df.empty:
        print("No payout data found across all pages.")
        return None
    
    # Create a summary of records per page
    records_per_page = df.groupby('Page').size().to_dict()
    
    # Print summary information in table format
    print("\n" + "="*70)
    print("SCRAPING SUMMARY")
    print("="*70)
    print(f"Total pages attempted: {last_page}")
    print(f"Successfully scraped pages: {successful_pages}")
    print(f"Failed pages: {len(failed_pages)}")
    if failed_pages:
        print(f"Failed page numbers: {sorted(failed_pages)}")
    
    # Create a table for records per page
    print("\nRECORDS PER PAGE:")
    print("-"*70)
    print(f"{'Page':<10}{'Records':<15}{'Page':<10}{'Records':<15}{'Page':<10}{'Records':<15}")
    print("-"*70)
    
    # Print 3 pages per row in the table
    pages = sorted(records_per_page.keys())
    for i in range(0, len(pages), 3):
        row = ""
        for j in range(3):
            if i+j < len(pages):
                page_num = pages[i+j]
                row += f"{page_num:<10}{records_per_page[page_num]:<15}"
        print(row)
    
    total_records = sum(records_per_page.values())
    print("-"*70)
    print(f"Total records scraped: {total_records}")
    print(f"Average records per page: {total_records / successful_pages:.1f}")
    print("="*70)
    
    # Save the raw scraped data to CSV
    df.to_csv('data/apex_payouts.csv', index=False, encoding='utf-8-sig')
    print("Saved raw payout data to 'data/apex_payouts.csv'.")
    
    # First, create a DataFrame with total earnings
    earnings_df = df.groupby(['Name', 'Location'], as_index=False)['Amount'].sum()
    earnings_df.rename(columns={'Amount': 'Total Earnings'}, inplace=True)
    
    # Then, create a DataFrame with the list of pages for each Name and Location
    pages_df = df.groupby(['Name', 'Location'])['Page'].agg(lambda x: sorted(list(set(x)))).reset_index()
    pages_df.rename(columns={'Page': 'Pages'}, inplace=True)
    
    # Merge the two DataFrames
    aggregated_df = pd.merge(earnings_df, pages_df, on=['Name', 'Location'])
    
    # Save the aggregated data to CSV
    aggregated_df.to_csv('data/aggregated_payouts.csv', index=False, encoding='utf-8-sig')
    print("Saved aggregated payout data to 'data/aggregated_payouts.csv'.")
    
    # Generate HTML report
    print("Generating HTML report...")
    generate_html_report(
        successful_pages=successful_pages,
        total_pages=last_page,
        start_date=start_date,
        end_date=end_date,
        failed_pages=failed_pages
    )
    
    # Generate standalone HTML report with embedded data
    print("Generating standalone HTML report with embedded data...")
    standalone_report = generate_html_report(
        successful_pages=successful_pages,
        total_pages=last_page,
        start_date=start_date,
        end_date=end_date,
        failed_pages=failed_pages,
        df=aggregated_df,  # Pass the DataFrame directly
        embed_data=True    # Embed data in the HTML
    )
    print(f"Standalone report generated as '{standalone_report}'. You can share this file directly.")
    
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