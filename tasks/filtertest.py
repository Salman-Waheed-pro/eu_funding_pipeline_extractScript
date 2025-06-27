from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from collections import Counter
import time
import tempfile
from contextlib import contextmanager
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import sys
import random

# Configure logging for detailed debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---- CONFIGURATION ----
# Use your filtered URL - this should contain exactly 804 calls
BASE_URL = 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals?order=DESC&pageNumber=1&pageSize=50&sortBy=startDate&isExactMatch=true&status=31094501,31094502'
HEADLESS = True
MAX_CALLS = 820  # Safety limit - should stop at 804 based on your filter
WAIT_TIME = 5    # Wait time for page loads
# -----------------------

class EUScraper:
    def __init__(self, base_url, headless=True):
        self.headless = headless
        self.driver = None
        self.base_url = base_url
        self.base_params = self._parse_url_params(base_url)
        logger.info(f"Initialized scraper with base URL: {base_url}")
        logger.info(f"Parsed base parameters: {self.base_params}")
    
    def _parse_url_params(self, url):
        """Parse URL and extract query parameters for building paginated URLs"""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        # Convert list values to single values (parse_qs returns lists)
        return {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in params.items()}
    
    def validate_url_format(self, url):
        """Validate that the URL format matches what the website expects"""
        logger.debug(f"Validating URL format: {url}")
        
        # Check if status parameter has unencoded commas
        if 'status=' in url:
            status_part = url.split('status=')[1].split('&')[0]
            if '%2C' in status_part:
                logger.warning(f"URL contains encoded comma in status parameter: {status_part}")
                return False
            elif ',' in status_part:
                logger.debug(f"Status parameter format looks correct: {status_part}")
                return True
        
        return True
    
    def _build_paginated_url(self, page_number):
        """Build URL for specific page number while preserving all original filters"""
        params = self.base_params.copy()
        params['pageNumber'] = str(page_number)
        
        parsed = urlparse(self.base_url)
        
        # Handle URL encoding carefully - don't encode commas in status parameter
        query_parts = []
        for key, value in params.items():
            if key == 'status' and isinstance(value, str) and ',' in value:
                # Don't URL-encode commas in status parameter
                query_parts.append(f"{key}={value}")
            else:
                # Use normal URL encoding for other parameters
                encoded_value = urlencode({key: value}).split('=', 1)[1]
                query_parts.append(f"{key}={encoded_value}")
        
        query_string = '&'.join(query_parts)
        new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query_string, parsed.fragment))
        
        # Validate the URL format
        if not self.validate_url_format(new_url):
            logger.error(f"Generated URL has incorrect format: {new_url}")
        
        logger.debug(f"Built URL for page {page_number}: {new_url}")
        return new_url
    
    def setup_driver(self):
        """Initialize Chromium driver with options"""
        logger.info("Setting up Chrome driver...")
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/chromium-browser"
        
        if self.headless:
            chrome_options.add_argument("--headless")
            logger.info("Running in headless mode")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        user_data_dir = tempfile.mkdtemp()
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
        logger.debug(f"Using temporary user data directory: {user_data_dir}")
        
        service = Service("/usr/bin/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Execute script to remove webdriver property
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logger.info("Chrome driver setup complete")
        return self.driver
    
    def wait_for_page_load(self, timeout=10):
        """Wait for page to fully load"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            logger.debug("Page load complete")
        except Exception as e:
            logger.warning(f"Page load wait timeout: {e}")
    
    def get_page_info(self):
        """Extract current page information for debugging"""
        try:
            # Try to find pagination info or total results
            current_url = self.driver.current_url
            page_title = self.driver.title
            
            logger.debug(f"Current URL: {current_url}")
            logger.debug(f"Page title: {page_title}")
            
            return {
                'url': current_url,
                'title': page_title
            }
        except Exception as e:
            logger.error(f"Error getting page info: {e}")
            return {}
    
    def extract_cards_from_page(self):
        """Extract all cards from current page with detailed logging"""
        logger.info(f"Extracting cards from current page...")
        
        # Log the current URL being processed
        current_url = self.driver.current_url
        logger.info(f"🌐 Currently visiting URL: {current_url}")
        
        # Wait for cards to load
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "eui-card"))
            )
        except Exception as e:
            logger.warning(f"No cards found or timeout waiting for cards: {e}")
            return []
        
        cards = self.driver.find_elements(By.CSS_SELECTOR, "eui-card")
        logger.info(f"Found {len(cards)} cards on this page")
        
        if len(cards) == 0:
            logger.warning("No cards found - this might indicate end of results or page load issue")
            # Let's check what's actually on the page
            page_source_snippet = self.driver.page_source[:500]
            logger.debug(f"Page source snippet: {page_source_snippet}")
        
        page_data = []
        
        for idx, card in enumerate(cards):
            card_data = self.extract_card_data(card, idx + 1)
            page_data.append(card_data)
            
            # Random status logging instead of every 10th
            if random.random() < 0.2:  # 20% chance to log each card
                logger.info(f"Card {idx + 1}: Status='{card_data['status']}', Title='{card_data['title'][:50]}...'")
        
        return page_data
    
    def extract_card_data(self, card, card_number):
        """Extract data from a single card with error handling"""
        card_data = {
            'card_number': card_number,
            'status': 'UNKNOWN',
            'title': 'UNKNOWN',
            'link': 'UNKNOWN'
        }
        
        try:
            # Extract status
            status_elem = card.find_element(By.CSS_SELECTOR, "span.eui-label")
            card_data['status'] = status_elem.text.strip()
        except Exception as e:
            logger.debug(f"Card {card_number}: Could not extract status - {e}")
            
        try:
            # Extract title and link
            link_elem = card.find_element(By.CSS_SELECTOR, "a.eui-u-text-link")
            card_data['title'] = link_elem.text.strip()
            card_data['link'] = link_elem.get_attribute("href")
        except Exception as e:
            logger.debug(f"Card {card_number}: Could not extract title/link - {e}")
        
        return card_data
    
    def check_if_end_of_results(self, cards_data, page_number):
        """Check if we've reached the end of results"""
        if not cards_data:
            logger.info(f"No cards found on page {page_number} - likely end of results")
            return True
        
        # Check for "No results" or similar messages on the page
        try:
            no_results_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'No results') or contains(text(), 'no results') or contains(text(), 'No calls')]")
            if no_results_elements:
                logger.info(f"Found 'no results' message on page {page_number}")
                return True
        except Exception as e:
            logger.debug(f"Error checking for 'no results' message: {e}")
        
        # Check if we have fewer cards than expected (less than 50 suggests last page)
        if len(cards_data) > 0 and len(cards_data) < 50:
            logger.info(f"Found only {len(cards_data)} cards on page {page_number} (less than 50) - likely last page")
            return True
        
        return False

    def navigate_to_next_page(self, current_page):
        """Navigate from current page to next page maintaining session"""
        next_page = current_page + 1
        
        # Get current URL and modify only the pageNumber parameter
        current_url = self.driver.current_url
        parsed = urlparse(current_url)
        params = parse_qs(parsed.query)
        
        # Update page number
        params['pageNumber'] = [str(next_page)]
        
        # Rebuild URL with new page number
        new_query = urlencode(params, doseq=True)
        new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
        
        logger.info(f"Navigating from page {current_page} to page {next_page}")
        logger.info(f"🔗 Next page URL: {new_url}")
        
        self.driver.get(new_url)
        self.wait_for_page_load()
        
        return next_page
    
    def validate_current_page(self, expected_page):
        """Validate we're on the expected page and not redirected"""
        current_url = self.driver.current_url
        
        # Check if we got redirected to page 1
        if expected_page > 1:
            if f"pageNumber={expected_page}" not in current_url:
                if "pageNumber=1" in current_url or "pageNumber" not in current_url:
                    logger.error(f"REDIRECT DETECTED: Expected page {expected_page}, but on page 1")
                    return False
        
        logger.debug(f"Successfully on page {expected_page}")
        return True

def main():
    logger.info("="*60)
    logger.info("Starting EU Funding Calls Scraper")
    logger.info("="*60)
    
    scraper = EUScraper(BASE_URL, HEADLESS)
    driver = scraper.setup_driver()
    
    all_data = []
    page_number = 1
    consecutive_empty_pages = 0
    pages_with_less_than_50_cards = 0
    
    try:
        # STEP 1: Start with page 1 to establish session
        logger.info(f"\n--- ESTABLISHING SESSION ON PAGE 1 ---")
        logger.info(f"🔗 Starting URL: {BASE_URL}")
        driver.get(BASE_URL)  # This is your filtered URL for page 1
        scraper.wait_for_page_load()
        
        current_page = 1
        
        # STEP 2: Sequential navigation through pages
        while len(all_data) < MAX_CALLS:
            logger.info(f"\n--- PROCESSING PAGE {current_page} ---")
            
            # Validate we're on the expected page
            if not scraper.validate_current_page(current_page):
                logger.error("Page validation failed - stopping scraper")
                break
            
            # Extract cards from current page
            page_cards = scraper.extract_cards_from_page()
            
            # Check if we've hit the end
            if scraper.check_if_end_of_results(page_cards, current_page):
                consecutive_empty_pages += 1
                logger.warning(f"End of results detected on page {current_page} ({consecutive_empty_pages}/2)")
                
                if consecutive_empty_pages >= 2:
                    logger.info("Found 2 consecutive end-of-results indicators - stopping scraper")
                    break
            else:
                consecutive_empty_pages = 0
            
            # Track pages with less than 50 cards (indicates end of results)
            if len(page_cards) > 0 and len(page_cards) < 50:
                pages_with_less_than_50_cards += 1
                logger.info(f"Page {current_page} has {len(page_cards)} cards (less than 50) - likely near end")
                
                # If we get 2 pages in a row with less than 50 cards, probably at the end
                if pages_with_less_than_50_cards >= 2:
                    logger.info("Found 2 pages with less than 50 cards - likely reached end of filtered results")
                    # Add current page data and then stop
                    all_data.extend(page_cards)
                    logger.info(f"Page {current_page} complete. Total cards so far: {len(all_data)}")
                    break
            else:
                pages_with_less_than_50_cards = 0
            
            # Add page data to overall collection
            all_data.extend(page_cards)
            
            logger.info(f"Page {current_page} complete. Total cards so far: {len(all_data)}")
            
            # Status distribution check randomly (instead of every 5 pages)
            if random.random() < 0.3:  # 30% chance to show status distribution
                current_statuses = [card['status'] for card in all_data]
                status_counter = Counter(current_statuses)
                logger.info(f"📊 Current status distribution: {dict(status_counter)}")
            
            # Break if we have no more cards
            if not page_cards:
                logger.info("No more cards found - reached end of results")
                break
            
            # Stop if we're close to our expected limit (804) to avoid overscraping
            if len(all_data) >= 800:
                logger.info(f"Approaching expected limit (804 calls), currently at {len(all_data)} - checking if we should continue")
                if len(page_cards) < 50:  # If this page has less than 50, we're probably at the end
                    logger.info("This page has less than 50 cards and we're near the limit - stopping")
                    break
            
            # Navigate to next page (maintains session)
            time.sleep(2)  # Be nice to the server
            try:
                current_page = scraper.navigate_to_next_page(current_page)
            except Exception as e:
                logger.error(f"Failed to navigate to next page: {e}")
                break

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error during scraping: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("Closing driver...")
        driver.quit()

    # ===== RESULTS ANALYSIS =====
    logger.info("\n" + "="*60)
    logger.info("SCRAPING RESULTS ANALYSIS")
    logger.info("="*60)
    
    total_calls = len(all_data)
    print(f"\nTotal calls scraped: {total_calls}")
    
    if total_calls == 0:
        print("ERROR: No calls were scraped!")
        return
    
    # Extract lists for compatibility with your original analysis
    all_statuses = [card['status'] for card in all_data]
    all_titles = [card['title'] for card in all_data]
    all_links = [card['link'] for card in all_data]
    
    # Status analysis
    counter = Counter(all_statuses)
    print(f"\nStatus counts:")
    for status, count in counter.items():
        print(f"- {status}: {count}")
    
    # Find status changes
    changes = []
    prev_status = None
    for idx, status in enumerate(all_statuses):
        if prev_status is not None and status != prev_status:
            changes.append(idx)
            print(f"STATUS CHANGE at position {idx + 1}: '{prev_status}' -> '{status}'")
        prev_status = status
    
    print(f"\nTotal number of status changes: {len(changes)}")
    if changes:
        print("Status changed after cards (1-based index):", [i+1 for i in changes])
    
    # Details of first few calls
    print(f"\n--- First 3 calls ---")
    for i in range(min(3, len(all_data))):
        card = all_data[i]
        print(f"Call {i+23}:")
        print(f"  Status: {card['status']}")
        print(f"  Title: {card['title']}")
        print(f"  Link: {card['link']}")
        print()
    
    # Details of last call
    if all_data:
        last_card = all_data[-1]
        print(f"--- Last call (#{total_calls}) ---")
        print(f"Status: {last_card['status']}")
        print(f"Title: {last_card['title']}")
        print(f"Link: {last_card['link']}")
    
    # Validation check
    print(f"\n--- VALIDATION ---")
    if total_calls == 804:
        print("✅ SUCCESS: Scraped exactly 804 calls as expected!")
    elif total_calls < 804:
        print(f"⚠️  WARNING: Only scraped {total_calls} calls, expected 804")
        print("This might be correct if the filter results changed or we stopped at the right boundary.")
    else:
        print(f"❌ ERROR: Scraped {total_calls} calls, but filter should only have 804!")
        print("This suggests the scraper went beyond the filtered results.")

if __name__ == "__main__":
    main()
