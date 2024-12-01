import os
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# File paths
excel_file_path = r"AUSTRALIA_MA_new.xlsx"  # Replace with your Excel file path
output_folder = r"Output"  # Folder to save downloaded PDFs
progress_file = r"progress_log.txt"  # File to log progress

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Load the Excel file
df = pd.read_excel(excel_file_path)

# Thread safety lock for writing progress
progress_lock = Lock()

# Function to download a single PDF
def download_pdf(row):
    pdf_link = row["PDF Link"]
    product_name = row["Product Name"]
    
    # Sanitize the product name to create a valid file name
    valid_file_name = "".join(c if c.isalnum() else "_" for c in product_name)
    pdf_file_path = os.path.join(output_folder, f"{valid_file_name}.pdf")
    
    try:
        # Download the PDF
        response = requests.get(pdf_link, stream=True)
        response.raise_for_status()  # Raise an error for HTTP issues
        
        # Write the content to a file
        with open(pdf_file_path, "wb") as pdf_file:
            for chunk in response.iter_content(chunk_size=1024):
                pdf_file.write(chunk)
        
        # Log progress
        with progress_lock:
            with open(progress_file, "a") as log_file:
                log_file.write(f"Downloaded: {pdf_file_path}\n")
        
        return pdf_file_path
    except requests.exceptions.RequestException as e:
        error_message = f"Failed to download {pdf_link}: {e}"
        with progress_lock:
            with open(progress_file, "a") as log_file:
                log_file.write(error_message + "\n")
        return error_message

# Main function to run downloads in parallel
def main():
    chunk_size = 50
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for _, row in df.iterrows():
            futures.append(executor.submit(download_pdf, row))
        
        # Save progress every 50 files
        completed_count = 0
        for future in as_completed(futures):
            result = future.result()
            print(result)
            completed_count += 1
            
            if completed_count % chunk_size == 0:
                print(f"Progress: {completed_count} files downloaded. Saving progress...")
    
    print("All files processed.")

if __name__ == "__main__":
    main()
