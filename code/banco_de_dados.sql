-- Criar a base de dados "cnpj"
CREATE DATABASE "cnpj"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "cnpj"
    IS 'Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil';
