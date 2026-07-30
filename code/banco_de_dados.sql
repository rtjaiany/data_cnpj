-- Criar a base de dados "Dados_RFB"
CREATE DATABASE "Dados_RFB"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "Dados_RFB"
    IS 'Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil';

-- Diretório físico do banco de dados:
--SHOW data_directory;

-- Tabela para rastrear alterações (snapshots) de dados ao longo do tempo
CREATE TABLE IF NOT EXISTS snapshots (
    id SERIAL PRIMARY KEY,
    tabela VARCHAR(50) NOT NULL,                -- Nome da tabela (empresa, estabelecimento, etc.)
    cnpj_basico VARCHAR(8) NOT NULL,            -- CNPJ Básico associado à alteração
    cnpj_ordem VARCHAR(4),                      -- CNPJ Ordem (apenas estabelecimento)
    cnpj_dv VARCHAR(2),                         -- CNPJ DV (apenas estabelecimento)
    chave JSONB NOT NULL,                       -- Chave primária como objeto JSON
    conteudo_anterior JSONB,                    -- Dados antigos (NULL para INSERT)
    conteudo_novo JSONB,                        -- Dados novos (NULL para DELETE)
    tipo_alteracao VARCHAR(10) NOT NULL,        -- 'INSERT', 'UPDATE', 'DELETE'
    mes_referencia VARCHAR(20) NOT NULL,        -- Mês do snapshot (ex: '2026-06')
    data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para otimizar pesquisas
CREATE INDEX IF NOT EXISTS snapshots_tabela_mes ON snapshots(tabela, mes_referencia);
CREATE INDEX IF NOT EXISTS snapshots_cnpj_busca ON snapshots(cnpj_basico, cnpj_ordem, cnpj_dv);
CREATE INDEX IF NOT EXISTS snapshots_chave ON snapshots USING gin (chave);