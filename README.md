# Brazilian RFB CNPJ Database: Temporal Reconstruction & Analytics Pipeline

An enterprise-grade, relational data pipeline built on **PostgreSQL** and **Python** to ingest, parse, track, and reconstruct the historical registry states of Brazilian companies using open data releases from the Federal Revenue of Brazil (Receita Federal do Brasil - RFB).

---

## 🏛️ Project Architecture & Contributions

This repository represents the collaborative evolution of a database framework designed to transform raw administrative data dumps into a reliable longitudinal panel dataset.

### 🔹 1. Ingestion & ETL Foundation (Developed by Afonso - @aphonsoar)

- **Automated Downloader & Parser:** Scripts to download raw compressed `.zip` files from the official RFB portal, unpack them, and stream-write the records to database tables.
- **Database Schema Definition:** Production table layouts (`empresa`, `estabelecimento`, `socios`, `simples`, and auxiliary tables) and indexes optimized for `cnpj_basico`.
- **Incremental Snapshots Tracker:** A delta detection utility (`ETL_incremental_dados_RFB.py`) that compares monthly staging tables against production baselines, logging changes (`INSERT`, `UPDATE`, `DELETE`) into a centralized `public.snapshots` event ledger.

### 🔸 2. Temporal Reconstruction & Analytical Framework (Developed by @rtjaiany)

- **Schema `analytics` Layout:** A sandboxed analytical schema created to support reproducible panel generation for temporal and business intelligence research without modifying the core production tables.
- **Procedure `analytics.reconstruct_temporal_data()`:** A reverse-chronological batch engine that starts from the June 2026 database baseline and steps backward through months (`2023-12` down to `2023-05`), applying the delta events in `snapshots` to reconstruct correct historical states.
- **PII Hashing & Security:** Integration of secure `SHA-256` hashing functions to anonymize names, CPFs, and contact details in compliance with Brazilian General Data Protection Law (LGPD).
- **Gaps & Islands Compression:** A state-reduction routine (`analytics.compress_longitudinal_intervals`) that compresses hundreds of millions of redundant monthly records into a compact event-history format containing active validity intervals (`valid_from_month` to `valid_to_month`).
- **Transition Trackers:** Database procedures to output detailed mutation audits for key variables (CNAE, municipality, status, Simples/MEI status).

---

## 📂 Repository Structure

```
├── code/
│   ├── ETL_coletar_dados_e_gravar_BD.py  # [Afonso] Ingests full RFB dumps
│   ├── ETL_incremental_dados_RFB.py     # [Afonso] Incremental snapshot logger
│   ├── banco_de_dados.sql               # [Afonso] Production schema definition
│   ├── reconstruct_analytics.sql        # [rtjaiany] Reconstruction & Analytics SQL layer
│   └── export_csv.py                    # [rtjaiany] Pipeline export pipeline to CSV
├── Dados_RFB_ERD.png                    # Entity-Relationship Diagram (ERD)
├── requirements.txt                     # Python dependency list
└── README.md                            # Documentation
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL 14.2+ (with `pgcrypto` extension for SHA-256 hashing)

### 1. Database Setup & Ingestion (Afonso's Layer)

1. Initialize the PostgreSQL schema:
    ```bash
    psql -h localhost -U postgres -d cnpj -f code/banco_de_dados.sql
    ```
2. Set up environment variables in `code/.env` matching the template:
    ```env
    DB_HOST=localhost
    DB_PORT=5432
    DB_USER=postgres
    DB_PASSWORD=your_password
    DB_NAME=cnpj
    OUTPUT_FILES_PATH=/path/to/downloads
    EXTRACTED_FILES_PATH=/path/to/extracted
    ```
3. Install dependencies and run the core ETL:
    ```bash
    pip install -r requirements.txt
    python code/ETL_coletar_dados_e_gravar_BD.py
    ```

### 2. Historical Reconstruction & Analytics (rtjaiany's Layer)

1. Compile the analytical procedures in your database:
    ```bash
    psql -h localhost -U postgres -d cnpj -f code/reconstruct_analytics.sql
    ```
2. Execute the reconstruction process:
    ```bash
    psql -h localhost -U postgres -d cnpj -c "CALL analytics.reconstruct_temporal_data();"
    ```
    _This will run the reverse chronological loop, resolve transition tables, apply PII hashes, and construct the Gaps & Islands longitudinal compression._
3. Execute the Gaps & Islands compression:
    ```bash
    psql -h localhost -U postgres -d cnpj -c "CALL analytics.compress_longitudinal_intervals();"
    ```

---

## 📊 Reconstructed Analytics Schema Dictionary

Our reconstruction process produces five highly optimized targets in the `analytics` schema:

1.  `analytics.reconstructed_establishments`: Historical month-by-month SP establishment cohorts.
2.  `analytics.reconstructed_companies`: Company metadata matching the establishment panels.
3.  `analytics.reconstructed_simples`: Historical MEI and Simples Nacional enrollment records.
4.  `analytics.reconstructed_partner_summaries`: Monthly aggregate statistics of board members per company (age spreads, entry/exit deltas).
5.  `analytics.longitudinal_establishment_intervals`: Invariant interval representation compressing the dataset to save disk space and accelerate longitudinal analytical models.

---

## 🔐 Privacy & Anonymization

To protect personally identifiable information (PII) of business owners and representatives, the analytics procedure processes all names, CPFs, and representatives using PG's crypto extensions:

$$\text{hashed\_field} = \text{SHA256}(\text{UTF-8}(\text{value}))$$

This preserves relationships and entity linkages across months while ensuring complete privacy and anonymization.

---

## 📈 Entity-Relationship Diagram

Refer to the database entity relationships depicted below:

![Entity-Relationship Diagram](docs/Dados_RFB_ERD.png)
