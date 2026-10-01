-- Banco Atlântico cards platform (SQL Server). CDC is enabled per table.
-- Batches are separated by GO lines; banking_cdc.seed runs them in order.
IF DB_ID('cards') IS NULL CREATE DATABASE cards;
GO
USE cards;
GO
IF OBJECT_ID('dbo.customers') IS NULL
CREATE TABLE dbo.customers (
    customer_id   INT           NOT NULL PRIMARY KEY,
    full_name     NVARCHAR(200) NOT NULL,
    nif           CHAR(9)       NOT NULL,
    email         NVARCHAR(200) NOT NULL,
    risk_segment  VARCHAR(10)   NOT NULL,
    created_at    DATETIME2(3)  NOT NULL
);
GO
IF OBJECT_ID('dbo.accounts') IS NULL
CREATE TABLE dbo.accounts (
    account_id    INT           NOT NULL PRIMARY KEY,
    customer_id   INT           NOT NULL REFERENCES dbo.customers(customer_id),
    iban          CHAR(25)      NOT NULL UNIQUE,
    account_type  VARCHAR(10)   NOT NULL,
    balance       DECIMAL(18,2) NOT NULL,
    status        VARCHAR(10)   NOT NULL,
    updated_at    DATETIME2(3)  NOT NULL
);
GO
IF OBJECT_ID('dbo.card_transactions') IS NULL
CREATE TABLE dbo.card_transactions (
    tx_id       BIGINT        NOT NULL PRIMARY KEY,
    account_id  INT           NOT NULL REFERENCES dbo.accounts(account_id),
    amount      DECIMAL(18,2) NOT NULL,
    currency    CHAR(3)       NOT NULL,
    merchant    NVARCHAR(100) NOT NULL,
    mcc         CHAR(4)       NOT NULL,
    country     CHAR(2)       NOT NULL,
    status      VARCHAR(10)   NOT NULL,
    tx_ts       DATETIME2(3)  NOT NULL
);
GO
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'cards') = 0
    EXEC sys.sp_cdc_enable_db;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_customers')
    EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customers',
         @role_name = NULL, @supports_net_changes = 0;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_accounts')
    EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'accounts',
         @role_name = NULL, @supports_net_changes = 0;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_card_transactions')
    EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'card_transactions',
         @role_name = NULL, @supports_net_changes = 0;
GO
-- Heartbeat: written after each batch; its arrival in the change table proves the capture job
-- has caught up (also the standard way to monitor CDC capture lag in production).
IF OBJECT_ID('dbo.cdc_heartbeat') IS NULL
CREATE TABLE dbo.cdc_heartbeat (id INT NOT NULL PRIMARY KEY, beat INT NOT NULL, beat_at DATETIME2(3) NOT NULL);
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_cdc_heartbeat')
    EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'cdc_heartbeat',
         @role_name = NULL, @supports_net_changes = 0;
GO
