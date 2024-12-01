import os
import pandas as pd
import requests

# File paths
excel_file_path = r"AUSTRALIA_MA_new.xlsx"  # Replace with your Excel file path
output_folder = r"/Output/"  # Folder to save downloaded PDFs

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Load the Excel file
df = pd.read_excel(excel_file_path)

# Iterate through the rows of the DataFrame
for index, row in df.iterrows():
    pdf_link = row["PDF Link"]  # PDF link column
    product_name = row["Product Name"]  # Product name column
    
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
        
        print(f"Downloaded: {pdf_file_path}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {pdf_link}: {e}")