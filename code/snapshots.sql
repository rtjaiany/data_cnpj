-- Table for tracking processed snapshots metadata
CREATE TABLE IF NOT EXISTS snapshots_metadata (
    id SERIAL PRIMARY KEY,
    reference_month VARCHAR(7) UNIQUE NOT NULL,    -- Represents YYYY-MM format
    collection_date TIMESTAMP NOT NULL,            -- The actual timestamp of collection
    status VARCHAR(20) NOT NULL,                  -- 'SUCCESS' or 'FAILED'
    duration_seconds INTEGER,
    num_inserts INTEGER DEFAULT 0,
    num_updates INTEGER DEFAULT 0,
    num_deletes INTEGER DEFAULT 0,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for tracking individual processed file checkpoints for the current month
CREATE TABLE IF NOT EXISTS processed_files (
    file_path VARCHAR(255) PRIMARY KEY,            -- Key format: 'YYYY-MM/filename.zip'
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for tracking historical change events (snapshots)
CREATE TABLE IF NOT EXISTS snapshots (
    id BIGSERIAL PRIMARY KEY,
    tabela VARCHAR(50) NOT NULL,                   -- 'empresa', 'estabelecimento', 'socios', 'simples'
    cnpj_basico VARCHAR(8) NOT NULL,
    cnpj_ordem VARCHAR(4),                         -- Only for estabelecimento
    cnpj_dv VARCHAR(2),                            -- Only for estabelecimento
    chave JSONB NOT NULL,                          -- Unique business key as JSON
    conteudo_anterior JSONB,                       -- Old row state (NULL for INSERT)
    conteudo_novo JSONB,                           -- New row state (NULL for DELETE)
    tipo_alteracao VARCHAR(10) NOT NULL,           -- 'INSERT', 'UPDATE', 'DELETE'
    mes_referencia VARCHAR(7) NOT NULL,            -- Reference month in YYYY-MM format
    data_coleta TIMESTAMP NOT NULL,                -- Original collection date/time
    data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes to optimize snapshot operations
CREATE INDEX IF NOT EXISTS snapshots_tabela_mes ON snapshots(tabela, mes_referencia);
CREATE INDEX IF NOT EXISTS snapshots_cnpj_busca ON snapshots(cnpj_basico, cnpj_ordem, cnpj_dv);
CREATE INDEX IF NOT EXISTS snapshots_chave ON snapshots USING gin (chave);

-- Table for latest state of changed empresas (without ignored column: razao_social)
CREATE TABLE IF NOT EXISTS latest_state_empresa (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social DOUBLE PRECISION,
    porte_empresa INTEGER,
    ente_federativo_responsavel TEXT,
    is_deleted BOOLEAN DEFAULT FALSE,
    last_updated_month VARCHAR(7) NOT NULL,
    data_coleta TIMESTAMP NOT NULL
);

-- Table for latest state of changed estabelecimentos (without ignored columns)
CREATE TABLE IF NOT EXISTS latest_state_estabelecimento (
    cnpj_basico VARCHAR(8) NOT NULL,
    cnpj_ordem VARCHAR(4) NOT NULL,
    cnpj_dv VARCHAR(2) NOT NULL,
    identificador_matriz_filial INTEGER,
    situacao_cadastral INTEGER,
    data_situacao_cadastral INTEGER,
    motivo_situacao_cadastral INTEGER,
    pais TEXT,
    data_inicio_atividade INTEGER,
    cnae_fiscal_principal INTEGER,
    cnae_fiscal_secundaria TEXT,
    uf TEXT,
    municipio INTEGER,
    situacao_especial TEXT,
    data_situacao_especial INTEGER,
    is_deleted BOOLEAN DEFAULT FALSE,
    last_updated_month VARCHAR(7) NOT NULL,
    data_coleta TIMESTAMP NOT NULL,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

-- Table for latest state of changed socios
CREATE TABLE IF NOT EXISTS latest_state_socios (
    cnpj_basico VARCHAR(8) NOT NULL,
    nome_socio_razao_social TEXT NOT NULL,
    identificador_socio INTEGER,
    cpf_cnpj_socio TEXT,
    qualificacao_socio INTEGER,
    data_entrada_sociedade INTEGER,
    pais INTEGER,
    representante_legal TEXT,
    nome_do_representante TEXT,
    qualificacao_representante_legal INTEGER,
    faixa_etaria INTEGER,
    is_deleted BOOLEAN DEFAULT FALSE,
    last_updated_month VARCHAR(7) NOT NULL,
    data_coleta TIMESTAMP NOT NULL,
    PRIMARY KEY (cnpj_basico, nome_socio_razao_social)
);

-- Table for latest state of changed simples
CREATE TABLE IF NOT EXISTS latest_state_simples (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    opcao_pelo_simples TEXT,
    data_opcao_simples INTEGER,
    data_exclusao_simples INTEGER,
    opcao_mei TEXT,
    data_opcao_mei INTEGER,
    data_exclusao_mei INTEGER,
    is_deleted BOOLEAN DEFAULT FALSE,
    last_updated_month VARCHAR(7) NOT NULL,
    data_coleta TIMESTAMP NOT NULL
);
