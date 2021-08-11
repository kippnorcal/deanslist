CREATE TABLE custom.DeansList_APIConnection (
    ID INT NOT NULL,
    SchoolName VARCHAR(50) NOT NULL,
    SchoolUsername VARCHAR(50) NOT NULL,
    APIKey VARCHAR(50) NOT NULL,
    AddedOn DATETIME NOT NULL,
    Active BIT NOT NULL,
    SZ_SchoolKEY INT NOT NULL,
    SystemSchoolID VARCHAR(150) NOT NULL
)
GO

