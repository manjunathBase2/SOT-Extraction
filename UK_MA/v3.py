import pandas as pd
import os
import requests
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

# Paths and configurations
input_excel_path = r"Scotland_MA.xlsx"  # Update with your file path
output_excel_path = r"Scotland_MA_output.xlsx"
base_output_folder = r"Output"  # Base output folder
os.makedirs(base_output_folder, exist_ok=True)
log_file = r"process_log.txt"

df = pd.read_excel(input_excel_path)
lock = Lock()

# Sanitize file names
def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*\n]', '_', filename).strip()

# Determine output folder
def get_output_folder(index):
    batch_number = (index // 5000) + 1
    folder_name = f"Output_{(batch_number - 1) * 5000 + 1}_to_{batch_number * 5000}"
    folder_path = os.path.join(base_output_folder, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

# Process product
def process_product(row):
    product = row['Product Name']
    index = row.name
    try:
        encoded_product = quote_plus(product)
        search_url = f"https://products.mhra.gov.uk/search/?search={encoded_product}&page=1"

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.get(search_url)

        # Handle agreements
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "agree-checkbox"))
        ).click()
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Agree')]"))
        ).click()

        # Find the PDF link
        search_result = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.search-result"))
        )
        pdf_link = search_result.find_element(By.CSS_SELECTOR, "dd.right a").get_attribute("href")

        # Download PDF
        response = requests.get(pdf_link, stream=True)
        if response.status_code == 200:
            output_folder = get_output_folder(index)
            pdf_filename = os.path.join(output_folder, f"{sanitize_filename(product)}.pdf")
            with open(pdf_filename, 'wb') as pdf_file:
                pdf_file.write(response.content)
            status = 'Downloaded'
        else:
            status = f"Failed: {response.status_code}"

        driver.quit()
        return product, pdf_link, status

    except Exception as e:
        return product, None, f"Error: {e}"

# Main function
def main():
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_product, row): index for index, row in df.iterrows()}
        completed_count = 0

        for future in as_completed(futures):
            index = futures[future]
            try:
                product, pdf_link, status = future.result()

                # Update DataFrame
                with lock:
                    df.at[index, 'PDF Link'] = pdf_link
                    df.at[index, 'Download Status'] = status

                # Log progress
                with lock:
                    with open(log_file, 'a') as log:
                        log.write(f"Product: {product}, PDF Link: {pdf_link}, Status: {status}\n")

                print(f"Processed: {product}, Status: {status}")
            except Exception as e:
                print(f"Error processing index {index}: {e}")

            completed_count += 1

            # Save progress
            if completed_count % 50 == 0:
                with lock:
                    df.to_excel(output_excel_path, index=False)
                print(f"Progress saved: {completed_count} files processed.")

    df.to_excel(output_excel_path, index=False)
    print("All products processed and saved.")

if __name__ == "__main__":
    main()
