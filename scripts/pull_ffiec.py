import os
from pathlib import Path
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import time

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATA_DIR = Path(__file__).resolve().parent.parent / "_data"
REPORT_DATE = os.getenv("REPORT_DATE_SLASH", "12/31/2025")


def download_ffiec(report_date="03/31/2022"):
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # configuring chrome options
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": str(DATA_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # initializing browser
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        print("Opening FFIEC page...")
        driver.get("https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx")
        
        # waiting for page to load
        wait = WebDriverWait(driver, 10)
        
        # selecting Call Reports -- Single Period from Available Products dropdown
        print("Selecting Call Reports -- Single Period...")
        products_dropdown = wait.until(
            EC.presence_of_element_located((By.ID, "ListBox1"))
        )
        select_products = Select(products_dropdown)
        select_products.select_by_value("ReportingSeriesSinglePeriod")
        time.sleep(2)
        
        # selecting the reporting period date
        print(f"Selecting reporting period: {report_date}...")
        date_dropdown = wait.until(
            EC.presence_of_element_located((By.ID, "DatesDropDownList"))
        )
        select_date = Select(date_dropdown)
        
        # printing all available dates for debugging
        available_dates = [option.text for option in select_date.options]
        print(f"Available dates: {available_dates}")
        
        # checking if requested date exists
        if report_date in available_dates:
            select_date.select_by_visible_text(report_date)
            print(f"Selected: {report_date}")
        else:
            print(f"Date {report_date} not available!")
            print(f"Using most recent date: {available_dates[0]}")
            select_date.select_by_index(0)
        
        time.sleep(1)
        
        # ensuring Tab Delimited is selected
        print("Ensuring Tab Delimited format is selected...")
        tab_delimited_radio = driver.find_element(By.ID, "TSVRadioButton")
        if not tab_delimited_radio.is_selected():
            tab_delimited_radio.click()
        time.sleep(1)
        
        # clicking the download button
        print("Clicking download button...")
        download_button = driver.find_element(By.ID, "Download_0")
        download_button.click()
        
        # waiting for download to complete
        print("Downloading... (this may take a few minutes)")
        time.sleep(15)
        
        print(f"Download complete! Check {DATA_DIR} for the file.")
        
    except Exception as e:
        print(f"Error: {e}")
        driver.save_screenshot(str(DATA_DIR / "error_screenshot.png"))
        print(f"Error screenshot saved to {DATA_DIR / 'error_screenshot.png'}")
        
    finally:
        driver.quit()


if __name__ == "__main__":
    download_ffiec(REPORT_DATE)