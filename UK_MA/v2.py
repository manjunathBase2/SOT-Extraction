import pandas as pd
import os
import requests
import time
import re
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from threading import Lock

# Path to your Excel file containing product names
input_excel_path = r"Scotland_MA.xlsx"  # Update with your actual file path
output_excel_path = r"Scotland_MA_output.xlsx"  # Save updated status here

# Directory to save downloaded PDFs
output_folder = r"Output"  # Update with your desired folder path
os.makedirs(output_folder, exist_ok=True)

# Read the product names from the Excel file
df = pd.read_excel(input_excel_path)

# Log file for progress tracking
log_file = r"process_log.txt"

# Create a lock for thread-safe operations
lock = Lock()

# Function to sanitize file names by replacing invalid characters
def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*\n]', '_', filename).strip()

# Function to download PDF for a given product
def process_product(row):
    product = row['Product Name']  # Adjust to your actual column name
    try:
        # URL-encode the product name
        encoded_product = quote_plus(product)
        search_url = f"https://products.mhra.gov.uk/search/?search={encoded_product}&page=1"

        # Initialize a new WebDriver instance for each thread
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.get(search_url)

        # Handle the checkbox and agree button
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "agree-checkbox"))
        ).click()
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Agree')]"))
        ).click()

        # Wait for the first result and extract the PDF link
        search_result = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.search-result"))
        )
        pdf_link = search_result.find_element(By.CSS_SELECTOR, "dd.right a").get_attribute("href")

        # Download the PDF
        response = requests.get(pdf_link, stream=True)
        if response.status_code == 200:
            sanitized_name = sanitize_filename(product)
            pdf_filename = os.path.join(output_folder, f"{sanitized_name}.pdf")
            with open(pdf_filename, 'wb') as pdf_file:
                pdf_file.write(response.content)
            status = 'Downloaded'
        else:
            status = f"Failed: {response.status_code}"

        driver.quit()
        return product, pdf_link, status

    except Exception as e:
        return product, None, f"Error: {e}"

# Main function to manage threading and periodic saving
def main():
    # Create a thread pool with 5 workers
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_product, row): index for index, row in df.iterrows()}
        completed_count = 0

        for future in as_completed(futures):
            index = futures[future]
            try:
                product, pdf_link, status = future.result()

                # Update DataFrame with results
                with lock:
                    df.at[index, 'PDF Link'] = pdf_link
                    df.at[index, 'Download Status'] = status

                # Log progress to a file
                with lock:
                    with open(log_file, 'a') as log:
                        log.write(f"Product: {product}, PDF Link: {pdf_link}, Status: {status}\n")

                print(f"Processed: {product}, Status: {status}")
            except Exception as e:
                print(f"Error processing index {index}: {e}")

            completed_count += 1

            # Save progress every 50 files
            if completed_count % 50 == 0:
                with lock:
                    df.to_excel(output_excel_path, index=False)
                print(f"Progress saved: {completed_count} files processed.")

    # Save the final output
    df.to_excel(output_excel_path, index=False)
    print("All products processed and saved.")

if __name__ == "__main__":
    main()
