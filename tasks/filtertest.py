from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from collections import Counter
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ---- CONFIGURATION ----
URL = 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals?order=DESC&pageNumber=1&pageSize=50&sortBy=startDate&isExactMatch=true&status=31094501,31094502'
HEADLESS = True
MAX_CALLS = 820
# -----------------------

def setup_driver(self):
        """Initialize Chromium driver with options"""
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/chromium-browser"  # Chromium binary locaion
    
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--window-size=1920,1080")

        # A unique temporary directory for user data
        user_data_dir = tempfile.mkdtemp()
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
    
        # Use the known chromedriver path
        service = Service("/usr/bin/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        return self.driver
    
    @contextmanager
    def tab_context(self, url):
        """Context manager for handling new tabs safely"""
        original_windows = self.driver.window_handles.copy()
        try:
            self.driver.execute_script(f"window.open('{url}', '_blank');")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            yield
        finally:
            # Close any new tabs and return to original
            current_windows = self.driver.window_handles
            for window in current_windows:
                if window not in original_windows:
                    self.driver.switch_to.window(window)
                    self.driver.close()
            if original_windows:
                self.driver.switch_to.window(original_windows[0])
    
    def safe_find_element(self, by, value, parent=None, default=""):
        """Safely find element with default fallback"""
        try:
            element = (parent or self.driver).find_element(by, value)
            return element.text.strip() if hasattr(element, 'text') else str(element)
        except Exception as e:
            logger.debug(f"Element not found: {by}={value}, error: {e}")
            return default
    
    def safe_find_elements(self, by, value, parent=None):
        """Safely find elements with empty list fallback"""
        try:
            return (parent or self.driver).find_elements(by, value)
        except Exception as e:
            logger.debug(f"Elements not found: {by}={value}, error: {e}")
            return []

def main():
    driver = get_driver(HEADLESS)
    all_statuses = []
    all_titles = []
    all_links = []
    call_count = 0
    page_number = 1

    while call_count < MAX_CALLS:
        url = (
            f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/"
            f"opportunities/calls-for-proposals?order=DESC&pageNumber={page_number}"
            f"&pageSize=50&sortBy=startDate&isExactMatch=true&status=31094501,31094502"
        )
        driver.get(url)
        time.sleep(3)  # Use WebDriverWait in production

        cards = driver.find_elements(By.CSS_SELECTOR, "eui-card")
        if not cards:
            break

        for card in cards:
            if call_count >= MAX_CALLS:
                break
            # Status
            try:
                status_elem = card.find_element(By.CSS_SELECTOR, "span.eui-label")
                status = status_elem.text.strip()
            except:
                status = "UNKNOWN"
            all_statuses.append(status)
            # Title
            try:
                title_elem = card.find_element(By.CSS_SELECTOR, "a.eui-u-text-link")
                title = title_elem.text.strip()
            except:
                title = "UNKNOWN"
            all_titles.append(title)
            # Link
            try:
                link_elem = card.find_element(By.CSS_SELECTOR, "a.eui-u-text-link")
                link = link_elem.get_attribute("href")
            except:
                link = "UNKNOWN"
            all_links.append(link)
            call_count += 1

        page_number += 1

    # Print total
    print(f"Total calls (up to {MAX_CALLS}): {len(all_statuses)}")

    # Count each status
    counter = Counter(all_statuses)
    print("Status counts:")
    for status, count in counter.items():
        print(f"- {status}: {count}")

    # Find changes in status
    changes = []
    prev = None
    for idx, status in enumerate(all_statuses):
        if prev is not None and status != prev:
            changes.append(idx)
        prev = status
    print(f"Total number of status changes: {len(changes)}")
    print("Status changed after cards (1-based index):", [i+1 for i in changes])

    # Print details of last call
    if all_statuses:
        last_idx = len(all_statuses) - 1
        print("\n--- Details of the last call ---")
        print(f"Index: {last_idx+1}")
        print(f"Title: {all_titles[last_idx]}")
        print(f"Status: {all_statuses[last_idx]}")
        print(f"Link: {all_links[last_idx]}")
    else:
        print("No calls found.")

    driver.quit()

if __name__ == "__main__":
    main()
