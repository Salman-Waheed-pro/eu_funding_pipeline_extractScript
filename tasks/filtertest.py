from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from collections import Counter
import time

# ---- CONFIGURATION ----
URL = 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals?order=DESC&pageNumber=1&pageSize=50&sortBy=startDate&isExactMatch=true&status=31094501,31094502'
HEADLESS = True
MAX_CALLS = 820
# -----------------------

def get_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

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
