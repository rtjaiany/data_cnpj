import os
import sys
import time
import csv
import io
import psycopg2
from dotenv import load_dotenv

class CleanedCSVStream(io.RawIOBase):
    """
    File-like binary stream that reads geoloc.csv line-by-line using csv.reader,
    sanitizes CNPJ and CEP fields, validates latitude/longitude coordinates,
    and outputs tab-separated UTF-8 bytes for COPY.
    """
    def __init__(self, file_path):
        self.file = open(file_path, 'r', encoding='utf-8', errors='ignore')
        self.reader = csv.reader(self.file)
        next(self.reader)  # Skip header row
        self.buffer = b''
        self.skipped_count = 0
        self.total_rows_read = 0

    def readable(self):
        return True

    def readinto(self, b):
        if not self.buffer:
            try:
                row = next(self.reader)
                self.total_rows_read += 1
            except StopIteration:
                return 0  # End of file
            
            if len(row) != 10:
                self.skipped_count += 1
                return self.readinto(b)
            
            cnpj_basico, tipo_logradouro, logradouro, numero, uf, city, bairro, cep, latitude, longitude = row
            
            # Check coordinate validity
            lat_str = latitude.strip()
            lon_str = longitude.strip()
            
            lat_empty = (lat_str == '' or lat_str.upper() == 'NAN')
            lon_empty = (lon_str == '' or lon_str.upper() == 'NAN')
            
            # If not empty, try parsing as float. If it fails, skip the shifted/malformed row.
            if not lat_empty:
                try:
                    float(lat_str)
                except ValueError:
                    self.skipped_count += 1
                    return self.readinto(b)
                    
            if not lon_empty:
                try:
                    float(lon_str)
                except ValueError:
                    self.skipped_count += 1
                    return self.readinto(b)
            
            # 1. Normalize and pad cnpj_basico to 8 chars
            cnpj_basico = cnpj_basico.strip().zfill(8)
            
            # 2. Normalize, strip .0, and pad cep to 8 chars
            cep = cep.strip()
            if cep.endswith('.0'):
                cep = cep[:-2]
            cep = cep.zfill(8)
            
            # Helper to escape tab, newline, and backslashes for PostgreSQL text copy format
            def clean_val(val):
                if not val or val.strip() == '' or val.strip().upper() == 'NAN':
                    return r'\N'
                val = val.replace('\\', '\\\\').replace('\t', ' ').replace('\n', ' ')
                return val.strip()
            
            row_cleaned = [
                cnpj_basico,
                clean_val(tipo_logradouro),
                clean_val(logradouro),
                clean_val(numero),
                clean_val(uf),
                clean_val(city),
                clean_val(bairro),
                cep,
                clean_val(latitude),
                clean_val(longitude)
            ]
            
            cleaned_line = "\t".join(row_cleaned) + "\n"
            self.buffer = cleaned_line.encode('utf-8')
            
        bytes_to_copy = min(len(self.buffer), len(b))
        b[:bytes_to_copy] = self.buffer[:bytes_to_copy]
        self.buffer = self.buffer[bytes_to_copy:]
        return bytes_to_copy

    def close(self):
        self.file.close()
        super().close()

def main():
    # 1. Load environment configurations
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    dotenv_path = os.path.join(parent_dir, '.env')
    
    if not os.path.isfile(dotenv_path):
        print(f"Error: .env file not found at {dotenv_path}")
        sys.exit(1)
        
    print(f"Loading environment from: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
    
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASSWORD', '')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    
    csv_path = os.path.join(parent_dir, 'geoloc.csv')
    if not os.path.isfile(csv_path):
        print(f"Error: geoloc.csv not found at {csv_path}")
        sys.exit(1)
        
    print(f"Connecting to database '{db_name}' on '{db_host}'...")
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    
    start_total_time = time.time()
    
    try:
        with conn.cursor() as cur:
            # Step 1: Create the table
            print("Dropping existing public.geoloc table if it exists...")
            cur.execute('DROP TABLE IF EXISTS public.geoloc;')
            
            print("Creating public.geoloc table schema...")
            create_table_sql = """
                CREATE TABLE public.geoloc (
                    cnpj_basico text,
                    tipo_logradouro text,
                    logradouro text,
                    numero text,
                    uf text,
                    cidade text,
                    bairro text,
                    cep text,
                    latitude double precision,
                    longitude double precision
                );
            """
            cur.execute(create_table_sql)
            conn.commit()
            
            # Step 2: Stream copy the dataset into PostgreSQL
            print("Streaming and loading geoloc.csv (this might take a minute)...")
            start_load_time = time.time()
            
            stream = CleanedCSVStream(csv_path)
            copy_sql = "COPY public.geoloc FROM STDIN WITH (FORMAT text, NULL '\\N')"
            cur.copy_expert(copy_sql, stream)
            
            # Commit the table load
            conn.commit()
            end_load_time = time.time()
            print(f"Data load completed in {end_load_time - start_load_time:.2f} seconds.")
            print(f"Total rows read: {stream.total_rows_read}")
            print(f"Malformed or invalid coordinate rows skipped: {stream.skipped_count}")
            stream.close()
            
            # Verify row counts in the table
            cur.execute('SELECT COUNT(*) FROM public.geoloc;')
            total_rows = cur.fetchone()[0]
            print(f"Total rows inserted into public.geoloc: {total_rows}")
            
            # Step 3: Create indexes
            print("\nCreating indexes...")
            
            # Index 1: cnpj_basico
            print("Creating index on 'cnpj_basico'...")
            start_idx = time.time()
            cur.execute('CREATE INDEX IF NOT EXISTS idx_geoloc_cnpj_basico ON public.geoloc (cnpj_basico);')
            conn.commit()
            print(f"Index idx_geoloc_cnpj_basico created in {time.time() - start_idx:.2f} seconds.")
            
            # Index 2: cep
            print("Creating index on 'cep'...")
            start_idx = time.time()
            cur.execute('CREATE INDEX IF NOT EXISTS idx_geoloc_cep ON public.geoloc (cep);')
            conn.commit()
            print(f"Index idx_geoloc_cep created in {time.time() - start_idx:.2f} seconds.")
            
            # Index 3: cidade (city)
            print("Creating index on 'cidade'...")
            start_idx = time.time()
            cur.execute('CREATE INDEX IF NOT EXISTS idx_geoloc_cidade ON public.geoloc (cidade);')
            conn.commit()
            print(f"Index idx_geoloc_cidade created in {time.time() - start_idx:.2f} seconds.")
            
            # Index 4: composite (cnpj_basico, cep)
            print("Creating composite index on '(cnpj_basico, cep)'...")
            start_idx = time.time()
            cur.execute('CREATE INDEX IF NOT EXISTS idx_geoloc_cnpj_cep ON public.geoloc (cnpj_basico, cep);')
            conn.commit()
            print(f"Index idx_geoloc_cnpj_cep created in {time.time() - start_idx:.2f} seconds.")
            
            # Step 4: Verification checks
            print("\nRunning verification queries...")
            
            # Count distinct cnpj_basico
            cur.execute('SELECT COUNT(DISTINCT cnpj_basico) FROM public.geoloc;')
            unique_cnpj = cur.fetchone()[0]
            print(f"Unique CNPJ basics: {unique_cnpj}")
            
            # Length validation for CNPJ basic
            cur.execute("SELECT COUNT(*) FROM public.geoloc WHERE length(cnpj_basico) != 8;")
            invalid_cnpj_len = cur.fetchone()[0]
            print(f"Rows with invalid CNPJ basic length (!= 8): {invalid_cnpj_len}")
            
            # Length validation for CEP
            cur.execute("SELECT COUNT(*) FROM public.geoloc WHERE length(cep) != 8;")
            invalid_cep_len = cur.fetchone()[0]
            print(f"Rows with invalid CEP length (!= 8): {invalid_cep_len}")
            
            # Check how join with establishment table uses indexes
            print("\nExplain query check (verify index usage):")
            explain_sql = """
                EXPLAIN ANALYZE
                SELECT e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv, g.latitude, g.longitude
                FROM public.estabelecimento e
                JOIN public.geoloc g ON e.cnpj_basico = g.cnpj_basico AND e.cep = g.cep
                LIMIT 5;
            """
            cur.execute(explain_sql)
            for row in cur.fetchall():
                print(row[0])
                
        end_total_time = time.time()
        print(f"\nTotal process took {end_total_time - start_total_time:.2f} seconds.")
        
    except Exception as e:
        conn.rollback()
        print(f"\nError occurred during loading: {e}")
        sys.exit(1)
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == '__main__':
    main()
