import luigi
import json
import time
import tempfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from collections import OrderedDict
import logging
from contextlib import contextmanager
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FundingOpportunitiesScraper:
    """Encapsulates scraping logic with better error handling and reusability"""
    
    def __init__(self, headless=True, wait_time=10):
        self.headless = headless
        self.wait_time = wait_time
        self.driver = None
    
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
    
    def extract_card_basic_info(self, card):
        """Extract basic information from a funding card"""
        try:
            # Extract title and link
            title_elem = card.find_element(By.CSS_SELECTOR, "a.eui-u-text-link")
            title = title_elem.text.strip()
            link = title_elem.get_attribute("href")
            
            # Extract subtitle information
            subtitle_elem = card.find_element(By.CLASS_NAME, "eui-card-header__title-container-subtitle")
            spans = subtitle_elem.find_elements(By.TAG_NAME, "span")
            strongs = subtitle_elem.find_elements(By.TAG_NAME, "strong")
            status = card.find_element(By.CSS_SELECTOR, "span.eui-label").text.strip()

            return {
                "title": title,
                "link": link,
                "code": spans[0].text.strip() if spans else "",
                "type": spans[2].text.strip() if len(spans) > 2 else "",
                "opening_date": datetime.strptime((strongs[0].text.strip() if strongs else ""), "%d %B %Y").strftime("%Y-%m-%d") if strongs and strongs[0].text.strip() else "",
                "deadline_date": datetime.strptime((strongs[1].text.strip() if len(strongs) > 1 else ""), "%d %B %Y").strftime("%Y-%m-%d") if len(strongs) > 1 and strongs[1].text.strip() else "",
                "stage": spans[-1].text.strip() if spans else "",
                "status": status
            }
        except Exception as e:
            logger.error(f"Error extracting card basic info: {e}")
            return None
    
    def click_show_more_buttons(self, content_div):
        """Click all 'Show more' buttons to reveal hidden content"""
        try:
            # Try multiple selectors for show more buttons
            selectors = [
                "sedia-show-more button",
                "button[class*='show-more']",
                "button[class*='expand']",
                ".show-more",
                "[data-toggle='collapse']"
            ]
            
            buttons_clicked = 0
            for selector in selectors:
                show_more_buttons = content_div.find_elements(By.CSS_SELECTOR, selector)
                for button in show_more_buttons:
                    try:
                        if button.is_displayed() and button.is_enabled():
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(1)  # Brief pause between clicks
                            buttons_clicked += 1
                            logger.debug(f"Clicked 'Show more' button with selector: {selector}")
                    except Exception as e:
                        logger.debug(f"Couldn't click show more button: {e}")
            
            if buttons_clicked > 0:
                logger.info(f"Clicked {buttons_clicked} show more buttons")
                time.sleep(2)  # Wait for content to expand
                
        except Exception as e:
            logger.debug(f"No show more buttons found: {e}")
    
    def extract_text_with_links(self, element):
        """Extract text content while preserving links with their href attributes"""
        try:
            # Get all text nodes and link nodes
            content_parts = []
            
            # Process all child nodes
            for child in element.find_elements(By.CSS_SELECTOR, "*"):
                if child.tag_name.lower() == 'a':
                    href = child.get_attribute('href')
                    text = child.text.strip()
                    if text and href:
                        content_parts.append(f"[{text}]({href})")
                    elif text:
                        content_parts.append(text)
                elif child.text.strip():
                    # For non-link elements, just get the text
                    text = child.text.strip()
                    # Avoid duplicating text that's already captured by parent
                    if text and text not in " ".join(content_parts):
                        content_parts.append(text)
            
            # If no child elements found, get direct text
            if not content_parts and element.text.strip():
                content_parts.append(element.text.strip())
            
            return " ".join(content_parts) if content_parts else ""
            
        except Exception as e:
            logger.debug(f"Error extracting text with links: {e}")
            return element.text.strip() if hasattr(element, 'text') else ""
    
    def extract_hierarchical_content_from_card(self, content_div):
        """
        Extract content from a card in hierarchical structure
        Returns dict with heading as key and content as value
        """
        try:
            # Click show more buttons first
            self.click_show_more_buttons(content_div)
            
            # Look for structured content with section headers
            section_headers = content_div.find_elements(By.CSS_SELECTOR, "eui-card-header__title-container-title ng-star-inserted")

            if section_headers:
                logger.debug(f"Found {len(section_headers)} structured sections")
                return self._extract_sections_by_headers(content_div, section_headers)
            else:
                # Fallback: try to extract any meaningful content
                logger.debug("No structured sections found, using fallback extraction")
                content = self.extract_text_with_links(content_div)
                return {"content": [content]} if content else {"content": ["No content found"]}
                
        except Exception as e:
            logger.error(f"Error in hierarchical content extraction: {e}")
            return {"error": [f"Content extraction failed: {str(e)}"]}
    
    def _extract_sections_by_headers(self, content_div, section_headers):
        """Extract content organized by section headers"""
        sections = OrderedDict()
        
        try:
            for i, header in enumerate(section_headers):
                header_text = header.text.strip()
                if not header_text:
                    continue
                
                # Find the content following this header
                content_elements = []
                
                # Try to find the parent container of the header
                header_container = header.find_element(By.XPATH, "..")
                
                # Look for siblings or following elements
                following_elements = header_container.find_elements(By.XPATH, "following-sibling::*")
                
                # Also look within the same container
                parent_container = header_container.find_element(By.XPATH, "..")
                all_elements = parent_container.find_elements(By.CSS_SELECTOR, "*")
                
                # Find elements that come after this header
                header_index = -1
                for idx, elem in enumerate(all_elements):
                    if elem == header or header_text in elem.text:
                        header_index = idx
                        break
                
                if header_index >= 0:
                    # Get next elements until we hit another header or end
                    next_header_index = len(all_elements)
                    for j in range(i + 1, len(section_headers)):
                        next_header_text = section_headers[j].text.strip()
                        for idx in range(header_index + 1, len(all_elements)):
                            if next_header_text in all_elements[idx].text:
                                next_header_index = idx
                                break
                        if next_header_index < len(all_elements):
                            break
                    
                    # Extract content between headers
                    content_list = []
                    for idx in range(header_index + 1, next_header_index):
                        elem = all_elements[idx]
                        elem_text = self.extract_text_with_links(elem)
                        
                        if (elem_text and 
                            len(elem_text.strip()) > 5 and 
                            elem_text.strip() not in [h.text.strip() for h in section_headers]):
                            
                            # Format based on element type
                            if elem.tag_name.lower() == 'li':
                                content_list.append(f"• {elem_text}")
                            elif elem.tag_name.lower() in ['p', 'div']:
                                content_list.append(elem_text)
                            elif elem.tag_name.lower() in ['strong', 'b']:
                                content_list.append(f"**{elem_text}**")
                            else:
                                content_list.append(elem_text)
                
                sections[header_text] = content_list if content_list else ["No content found"]
                
        except Exception as e:
            logger.error(f"Error extracting sections by headers: {e}")
            sections["extraction_error"] = [f"Error: {str(e)}"]
        
        return sections
    
    def extract_all_card_details(self):
        """
        Extract all cards as independent units with hierarchical content
        Skip 'General info' and stop after 'Partner search announcements'
        """
        cards_data = OrderedDict()
        
        try:
            # Wait for content to load
            WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "eui-card"))
            )
            
            cards = self.driver.find_elements(By.CSS_SELECTOR, "eui-card")
            logger.info(f"Found {len(cards)} cards on detail page")
            
            for i, card in enumerate(cards):
                try:
                    # Get card title
                    header_title = self.safe_find_element(
                        By.CSS_SELECTOR, 
                        "eui-card-header-title.eui-card-header__title-container-title",
                        parent=card
                    )
                    
                    if not header_title:
                        logger.debug(f"Card {i+1} has no header title, skipping")
                        continue
                    
                    # Skip 'General info' cards
                    if "General info" in header_title:
                        logger.info(f"Skipping 'General info' card: '{header_title}'")
                        continue
                    
                    # Stop processing after extracting 'Partner search announcements'
                    if "Partner search announcements" in header_title:
                        logger.info(f"Processing final card: '{header_title}'")
                        
                        # Extract this card's content
                        try:
                            content_div = card.find_element(By.CSS_SELECTOR, "eui-card-content")
                            card_content = self.extract_hierarchical_content_from_card(content_div)
                            cards_data.update(card_content)  # Add sections as independent units
                        except Exception as e:
                            logger.error(f"Error extracting Partner search announcements: {e}")
                            cards_data["Partner search announcements"] = [f"Extraction error: {str(e)}"]
                        
                        logger.info("Stopping extraction after 'Partner search announcements'")
                        break
                    
                    logger.info(f"Processing card: '{header_title}'")
                    
                    # Get card content
                    try:
                        content_div = card.find_element(By.CSS_SELECTOR, "eui-card-content")
                    except:
                        logger.debug(f"No content div found for card: '{header_title}'")
                        cards_data[header_title] = ["No content available"]
                        continue
                    
                    # Extract hierarchical content
                    card_content = self.extract_hierarchical_content_from_card(content_div)
                    
                    # Add each section as an independent unit
                    if isinstance(card_content, dict):
                        for section_title, section_content in card_content.items():
                            # Create a unique key combining card title and section title
                            if section_title in ["content", "error"]:
                                key = header_title
                            else:
                                key = section_title
                            cards_data[key] = section_content
                    else:
                        cards_data[header_title] = card_content if card_content else ["No content extracted"]
                    
                    logger.info(f"Successfully extracted content for: '{header_title}'")
                    
                except Exception as e:
                    logger.error(f"Error processing card {i+1}: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error in extract_all_card_details: {e}")
        
        return cards_data
    
    def extract_page_sections(self):
        """
        Extract content from page sections (non-card elements)
        Handles pages with section-based layout instead of cards
        """
        sections_data = OrderedDict()
        
        try:
            # Wait for content to load
            WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "section"))
            )
            
            # Look for sections with IDs like scroll-gi, scroll-sep, etc.
            sections = self.driver.find_elements(By.CSS_SELECTOR, "section[id^='scroll-']")
            
            if not sections:
                # Fallback: look for any sections with h2 headers
                sections = self.driver.find_elements(By.CSS_SELECTOR, "section h2")
                if sections:
                    sections = [header.find_element(By.XPATH, "..") for header in sections]
            
            logger.info(f"Found {len(sections)} sections on page")
            
            for section in sections:
                try:
                    # Get section title from h2
                    title_elem = section.find_element(By.CSS_SELECTOR, "h2")
                    section_title = title_elem.text.strip()
                    
                    if not section_title:
                        continue
                    
                    logger.info(f"Processing section: '{section_title}'")
                    
                    # Click any show more buttons in this section
                    self.click_show_more_buttons(section)
                    
                    # Extract content from this section
                    content_list = []
                    
                    # Look for structured content (tables, lists, divs with data)
                    content_elements = section.find_elements(By.CSS_SELECTOR, 
                        "div.eui-input-group, div.sedia-base, ol, ul, p, div.row")
                    
                    for elem in content_elements:
                        elem_text = self.extract_text_with_links(elem)
                        
                        if elem_text and len(elem_text.strip()) > 3:
                            # Format based on element structure
                            if elem.find_elements(By.CSS_SELECTOR, "strong"):
                                # This is likely a label-value pair
                                labels = elem.find_elements(By.CSS_SELECTOR, "strong")
                                for label in labels:
                                    label_text = label.text.strip()
                                    # Find the value after the label
                                    parent = label.find_element(By.XPATH, "../..")
                                    value_text = parent.text.replace(label_text, "").strip()
                                    if value_text:
                                        content_list.append(f"**{label_text}**: {value_text}")
                            elif elem.tag_name.lower() == 'li':
                                content_list.append(f"• {elem_text}")
                            elif elem.tag_name.lower() in ['ol', 'ul']:
                                # Process list items
                                list_items = elem.find_elements(By.TAG_NAME, "li")
                                for li in list_items:
                                    li_text = self.extract_text_with_links(li)
                                    if li_text:
                                        content_list.append(f"• {li_text}")
                            else:
                                content_list.append(elem_text)
                    
                    sections_data[section_title] = content_list if content_list else ["No content found"]
                    logger.info(f"Successfully extracted content for section: '{section_title}'")
                    
                except Exception as e:
                    logger.error(f"Error processing section: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in extract_page_sections: {e}")
            sections_data["extraction_error"] = [f"Error: {str(e)}"]
        
        return sections_data
    
    def detect_page_type(self):
        """
        Detect whether the page contains cards or sections
        Returns 'cards' or 'sections'
        """
        try:
            # Check for cards first
            cards = self.driver.find_elements(By.CSS_SELECTOR, "eui-card")
            if cards:
                logger.info(f"Detected card-based page with {len(cards)} cards")
                return 'cards'
            
            # Check for sections
            sections = self.driver.find_elements(By.CSS_SELECTOR, "section[id^='scroll-'], section h2")
            if sections:
                logger.info(f"Detected section-based page with {len(sections)} sections")
                return 'sections'
            
            logger.warning("Could not detect page type, defaulting to cards")
            return 'cards'
            
        except Exception as e:
            logger.error(f"Error detecting page type: {e}")
            return 'cards'
    
    def scrape_section_only_page(self, url):
        """
        Scrape pages that don't have card listings, only sections
        """
        self.driver.get(url)
        
        try:
            WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "section"))
            )
        except Exception as e:
            logger.error(f"Error waiting for page to load: {e}")
            return {}
        
        # Extract page title if available
        title = self.safe_find_element(By.CSS_SELECTOR, "h1, title")
        
        # Extract all sections
        detailed_sections = self.extract_page_sections()
        
        return {
            "title": title,
            "link": url,
            "type": "section-based",
            **detailed_sections
        }
    
    def scrape_page(self, page_number, page_size=50, start_index=0, end_index=None):
        """
        Scrape a single page of funding opportunities with option to limit to specific range
        start_index and end_index allow processing only specific cards (0-based indexing)
        """
        url = (
            f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/"
            f"opportunities/calls-for-proposals?order=DESC&pageNumber={page_number}"
            f"&pageSize={page_size}&sortBy=startDate&isExactMatch=true"
            f"&status=31094501,31094502"
        )
        
        self.driver.get(url)
        
        try:
            WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "eui-card-header"))
            )
        except Exception as e:
            logger.error(f"Error waiting for page to load: {e}")
            return []
        
        cards = self.driver.find_elements(By.TAG_NAME, "eui-card-header")
        
        if not cards:
            logger.info("No cards found on this page")
            return []
        
        # Apply range filtering
        if end_index is None:
            end_index = len(cards)
        
        # Ensure indices are within bounds
        start_index = max(0, start_index)
        end_index = min(len(cards), end_index)
        
        if start_index >= end_index:
            logger.info(f"Invalid range: start_index={start_index}, end_index={end_index}")
            return []
        
        selected_cards = cards[start_index:end_index]
        logger.info(f"Processing cards {start_index+1}-{end_index} out of {len(cards)} total cards on page {page_number}")
        
        calls = []
        for i, card in enumerate(selected_cards):
            actual_index = start_index + i + 1  # 1-based for logging
            try:
                basic_info = self.extract_card_basic_info(card)
                if not basic_info:
                    continue
                
                logger.info(f"Processing card {actual_index}/{len(cards)}: {basic_info['title']}")
                
                # Extract all detailed information dynamically
                with self.tab_context(basic_info['link']):
                    time.sleep(3)  # Allow page to load
                    
                    # Detect page type and extract accordingly
                    page_type = self.detect_page_type()
                    if page_type == 'cards':
                        detailed_sections = self.extract_all_card_details()
                    else:
                        detailed_sections = self.extract_page_sections()
                
                # Merge basic info with detailed sections at the same level
                call_data = {**basic_info, **detailed_sections}
                calls.append(call_data)
                
                logger.info(f"Successfully processed: {basic_info['title']}")
                
            except Exception as e:
                logger.error(f"Error processing card {actual_index}: {e}")
                continue
        
        return calls
    
    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()

class FetchFundingOpportunities(luigi.Task):
    """Luigi task for fetching EU funding opportunities"""
    
    max_pages = luigi.IntParameter(default=None)
    page_size = luigi.IntParameter(default=50)
    output_file = luigi.Parameter(default="calls_raw.json")
    
    def run(self):
        scraper = FundingOpportunitiesScraper()
        
        try:
            scraper.setup_driver()
            logger.info("Browser launched successfully")
            
            all_calls = []
            page_num = 1
            
            # If max_pages is not specified, we'll scrape until no more results
            max_pages_to_check = self.max_pages if self.max_pages else float('inf')
            
            while page_num <= max_pages_to_check:
                logger.info(f"Scraping page {page_num}")
                
                page_calls = scraper.scrape_page(
                    page_num, 
                    self.page_size
                )
                
                # If no results returned, we've reached the end
                if not page_calls:
                    logger.info(f"No more results found on page {page_num}, stopping pagination")
                    break
                
                all_calls.extend(page_calls)
                logger.info(f"Page {page_num} completed. Found {len(page_calls)} calls. Total calls so far: {len(all_calls)}")
                
                # If we got fewer results than page_size, this might be the last page
                if len(page_calls) < self.page_size:
                    logger.info(f"Page {page_num} returned fewer items than page_size ({len(page_calls)} < {self.page_size}), likely the last page")
                    break
                
                page_num += 1
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Save results
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(all_calls, f, indent=4, ensure_ascii=False)
            
            logger.info(f"Successfully saved {len(all_calls)} calls from {page_num-1} pages to {self.output_file}")
            
        except Exception as e:
            logger.error(f"Fatal error in scraping process: {e}")
            raise
        finally:
            scraper.cleanup()
    
    def output(self):
        return luigi.LocalTarget(self.output_file)

# Usage example for testing last 6 calls (45-50) from page 1:
# python -m luigi --module this_file FetchFundingOpportunities --start-index 44 --end-index 50 --local-scheduler
