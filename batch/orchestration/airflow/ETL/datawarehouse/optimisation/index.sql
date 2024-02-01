-- Index pour DimCar
CREATE INDEX idx_CarMake ON DimCar (CarMake); -- Recherche ou tri par marque de voiture
CREATE INDEX idx_CarModel ON DimCar (CarModel); -- Recherche ou tri par mod�le de voiture
CREATE INDEX idx_OriginCountry ON DimCar (OriginCountry); -- Recherche ou tri par pays d'origine de la voiture

-- Index pour DimCustomer
CREATE INDEX idx_CustomerName ON DimCustomer (CustomerName); -- Recherche ou tri par nom de client
CREATE INDEX idx_Country ON DimCustomer (Country); -- Recherche ou tri par pays du client

-- Index pour DimDate
CREATE INDEX idx_Date ON DimDate (Date); -- Recherche ou tri par date
CREATE INDEX idx_CarYear ON DimDate (CarYear); -- Recherche ou tri par ann�e de la voiture

-- Index pour DimSalesperson
CREATE INDEX idx_SalespersonName ON DimSalesperson (SalespersonName); -- Recherche ou tri par nom du vendeur

-- Index pour FactSales
CREATE INDEX idx_CarID ON FactSales (CarID); -- Jointure sur la cl� �trang�re CarID
CREATE INDEX idx_CustomerID ON FactSales (CustomerID); -- Jointure sur la cl� �trang�re CustomerID
CREATE INDEX idx_DateID ON FactSales (DateID); -- Jointure sur la cl� �trang�re DateID
CREATE INDEX idx_SalespersonID ON FactSales (SalespersonID); -- Jointure sur la cl� �trang�re SalespersonID