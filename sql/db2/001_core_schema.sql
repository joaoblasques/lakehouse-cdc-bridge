-- Banco Atlântico core banking on DB2 (plays the mainframe). Statements are separated by ';'
-- on its own line; banking_cdc.seed runs them in order and skips "already exists" errors.
CREATE SCHEMA CORE
;
CREATE TABLE CORE.LOANS (
    LOAN_ID      INTEGER       NOT NULL PRIMARY KEY,
    CUSTOMER_ID  INTEGER       NOT NULL,
    PRODUCT      VARCHAR(12)   NOT NULL,
    PRINCIPAL    DECIMAL(18,2) NOT NULL,
    OUTSTANDING  DECIMAL(18,2) NOT NULL,
    RATE_PCT     DECIMAL(5,3)  NOT NULL,
    STATUS       VARCHAR(10)   NOT NULL,
    OPENED_ON    DATE          NOT NULL,
    -- Set by DB2 on every insert and update. IMPLICITLY HIDDEN keeps existing applications'
    -- SELECT * unchanged: adding CDC must not break the mainframe's own programs.
    ROW_CHANGED  TIMESTAMP     NOT NULL IMPLICITLY HIDDEN
                 GENERATED ALWAYS FOR EACH ROW ON UPDATE AS ROW CHANGE TIMESTAMP
)
;
-- The watermark query filters and sorts on this column on every run.
CREATE INDEX CORE.IX_LOANS_ROW_CHANGED ON CORE.LOANS (ROW_CHANGED)
;
