import pandas as pd
import os
import time

def merge_datasets():
    base_dir = "/Users/rtjaiany/Library/CloudStorage/OneDrive-Personal/01_Documentos/01 - In Progress/03 - Dissertation/data_cnpj"
    file_active = os.path.join(base_dir, "brasil-active.csv")
    file_inactive = os.path.join(base_dir, "sp_inactive.csv")
    file_output = os.path.join(base_dir, "geoloc.csv")
    
    print(f"Active file: {file_active} ({os.path.getsize(file_active) / (1024*1024):.1f} MB)")
    print(f"Inactive file: {file_inactive} ({os.path.getsize(file_inactive) / (1024*1024):.1f} MB)")
    print(f"Output file: {file_output}")
    
    # Target column order
    target_columns = [
        "cnpj_basico",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "uf",
        "cidade",
        "bairro",
        "cep",
        "latitude",
        "longitude"
    ]
    
    start_time = time.time()
    
    # We will write the header first
    header_written = False
    
    # 1. Process sp_inactive.csv (85MB, can be processed directly or in chunks)
    print("Processing sp_inactive.csv...")
    # Clean/strip whitespaces in columns
    df_inactive = pd.read_csv(file_inactive, on_bad_lines='skip')
    df_inactive.columns = [col.strip() for col in df_inactive.columns]
    
    # Select and reorder target columns
    df_inactive_reordered = df_inactive[target_columns]
    
    # Save to output file (creating the file with header)
    df_inactive_reordered.to_csv(file_output, index=False, mode='w')
    header_written = True
    print(f"Added {len(df_inactive_reordered)} rows from sp_inactive.csv.")
    
    # 2. Process brasil-active.csv (1.1GB, process in chunks to be memory safe)
    print("Processing brasil-active.csv in chunks...")
    chunk_count = 0
    total_active_rows = 0
    chunk_size = 100000
    
    for chunk in pd.read_csv(file_active, chunksize=chunk_size, on_bad_lines='skip'):
        chunk.columns = [col.strip() for col in chunk.columns]
        chunk_reordered = chunk[target_columns]
        chunk_reordered.to_csv(file_output, index=False, mode='a', header=False)
        
        total_active_rows += len(chunk_reordered)
        chunk_count += 1
        if chunk_count % 10 == 0:
            print(f"Processed {total_active_rows} rows from brasil-active.csv...")
            
    print(f"Added {total_active_rows} rows from brasil-active.csv.")
    end_time = time.time()
    
    print("\nMerge complete!")
    print(f"Total time: {end_time - start_time:.2f} seconds.")
    print(f"Output file size: {os.path.getsize(file_output) / (1024*1024):.1f} MB")
    
    # Verification check
    print("\nVerification check:")
    df_check = pd.read_csv(file_output, nrows=5)
    print("Columns in geoloc.csv:", list(df_check.columns))
    print("First few rows:")
    print(df_check.to_string())

if __name__ == "__main__":
    merge_datasets()
