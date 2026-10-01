CREATE TABLE [dbo].[merchants] (
    [merchant_id]   BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [legal_name]    NVARCHAR(200) NOT NULL,
    [mcc]           CHAR(4) NOT NULL,
    [country]       CHAR(2) NOT NULL,
    [contact_email] NVARCHAR(320) NULL,
    [risk_score]    FLOAT NULL,
    [is_blocked]    BIT NOT NULL DEFAULT 0,
    [onboarded_at]  DATETIME2(3) NOT NULL,
    [notes]         NVARCHAR(MAX) NULL
);
