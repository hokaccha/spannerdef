CREATE TABLE Users (
    Id INT64 NOT NULL,
    Name STRING(100),
    Email STRING(255),
    CreatedAt TIMESTAMP
) PRIMARY KEY (Id);

CREATE INDEX IdxEmail ON Users (Email);

CREATE TABLE Posts (
    Id INT64 NOT NULL,
    UserId INT64 NOT NULL,
    Title STRING(255),
    Content STRING(MAX),
    CreatedAt TIMESTAMP
) PRIMARY KEY (Id);

CREATE INDEX IdxUserId ON Posts (UserId);
