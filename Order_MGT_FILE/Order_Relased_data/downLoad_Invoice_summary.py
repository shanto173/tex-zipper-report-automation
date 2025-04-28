from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os
import time
import re
from pathlib import Path
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime  # 🔹 Import for timestamp

# === Setup: download directory ===
download_dir = r"C:\Users\Ariful\Documents\selenium_download_file\Order_MGT_FILE\Order_Relased_data"
os.makedirs(download_dir, exist_ok=True)

chrome_options = webdriver.ChromeOptions()
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")  # 🔹 Run Chrome in headless mode
chrome_options.add_argument("--disable-gpu")  # Optional: disable GPU usage
chrome_options.add_argument("--window-size=1920,1080")  # Optional: set window size for full rendering
chrome_options.add_argument("--no-sandbox")  # Optional: for Linux environments
chrome_options.add_argument("--disable-dev-shm-usage")  # Optional: prevents crashes on some systems
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

pattern = "Packing and Invoice Summery"

def is_file_downloaded():
    return any(Path(download_dir).glob(f"*{pattern}*.xlsx"))

while True:
    try:
        # === Start driver ===
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        wait = WebDriverWait(driver, 20)

        # === Step 1: Log into Odoo ===
        driver.get("https://taps.odoo.com")
        wait.until(EC.presence_of_element_located((By.NAME, "login"))).send_keys("supply.chain3@texzipperbd.com")
        driver.find_element(By.NAME, "password").send_keys("@Shanto@86")
        time.sleep(2)
        driver.find_element(By.XPATH, "//button[contains(text(), 'Log in')]").click()
        time.sleep(2)

        # === Step 2: Click user/company switch ===
        time.sleep(2)
        try:
            wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".modal-backdrop")))
        except:
            pass

        switcher_span = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR,
            "div.o_menu_systray div.o_switch_company_menu > button > span"
        )))
        driver.execute_script("arguments[0].scrollIntoView(true);", switcher_span)
        switcher_span.click()
        time.sleep(2)

        # === Step 3: Click 'Zipper' company ===
        target_div = wait.until(EC.element_to_be_clickable((By.XPATH,
            "//div[contains(@class, 'log_into')][span[contains(text(), 'Zipper')]]"
        )))
        driver.execute_script("arguments[0].scrollIntoView(true);", target_div)
        target_div.click()
        time.sleep(2)

        # step 4
        # === Trigger global search box by sending a keystroke ===
        from selenium.webdriver.common.keys import Keys  # 🔹 Add this import at the top if not already present
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys("MRP")  # or use Keys.A if needed
        time.sleep(2)  # Wait for search box to appear
        
        # Step 5
        # Click on MRP option
        
        wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[2]/div/div/div/div/main/div/div[2]/div/div[1]/a/div/span"))).click() 
        time.sleep(4)

        # Step 6
        # click on list of report
        
        wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[2]/div/div/div/div/main/div/div/div/div/div/div[1]/div[2]/div/select"))).click() 
        time.sleep(4)
        
        # Step 7
        # click on Invoice summary of report
        
        wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div/div/main/div/div/div/div/div/div[1]/div[2]/div/select/option[19]"))).click() 
        time.sleep(4)
        
        # Step 8
        # download the report
        
        wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[2]/div/div/div/div/footer/footer/button[1]"))).click() 
        time.sleep(10)
       
        # === Step 9: Confirm file downloaded ===
        if is_file_downloaded():
            print("File download complete!")

            # === Step 10: Clean up older files ===
            try:
                files = list(Path(download_dir).glob(f"*{pattern}*.xlsx"))
                if len(files) > 1:
                    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                    for file in files[1:]:
                        file.unlink()
                        print(f"Deleted old file: {file.name}")
                print("File cleanup complete. Only latest report is kept.")
            except Exception as e:
                print(f"Failed during file cleanup: {e}")

            driver.quit()
            break  # Exit loop

        else:
            raise Exception(" File not downloaded. Retrying...")

    except Exception as e:
        print(f"\ Error occurred: {e}\nRetrying in 10 seconds...\n")
        try:
            driver.quit()
        except:
            pass
        time.sleep(5)
        

# === Step 11: Load latest file and paste to Google Sheet ===
try:
    files = list(Path(download_dir).glob(f"*{pattern}*.xlsx"))
    if not files:
        raise Exception("No matching file found.")

    # Sort and get the latest file
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    latest_file = files[0]
    print(f"Latest file found: {latest_file.name}")

    # Load into DataFrame
    df_production_pcs = pd.read_excel(latest_file,sheet_name=0)
    print("File loaded into DataFrame.")

    df_production_usd = pd.read_excel(latest_file,sheet_name=1)
    print("File loaded into DataFrame.")
    
    # Setup Google Sheets API
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "testing-456510-f87e37b9a9e4.json", scope)
    client = gspread.authorize(creds)

    # Open the sheet and paste the data
    sheet = client.open_by_key("1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc")
    worksheet = sheet.worksheet("Production Data")

    # Clear old content (optional)
    worksheet.clear()

    # Paste new data
    set_with_dataframe(worksheet, df_production_pcs)
    print("Data pasted to Google Sheet (Sheet4).")
    
    # === ✅ Add timestamp to Y2 ===
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("AC2", [[f"{timestamp}"]])
    print(f"Timestamp written to AC2: {timestamp}")
    
    # USD paste
    
    sheet = client.open_by_key("1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc")
    worksheet = sheet.worksheet("Production Data value")

    # Clear old content (optional)
    worksheet.clear()

    # Paste new data
    set_with_dataframe(worksheet, df_production_usd)
    print("Data pasted to Google Sheet (Sheet4).")

    # === ✅ Add timestamp to Y2 ===
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("AC2", [[f"{timestamp}"]])
    print(f"Timestamp written to AC2: {timestamp}")

except Exception as e:
    print(f"Error while pasting to Google Sheets: {e}")
