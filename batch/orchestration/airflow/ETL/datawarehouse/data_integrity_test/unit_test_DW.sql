-- Test unitaire de la Data Warehouse

-- V�rification des cl�s primaires
SELECT COUNT(*) - COUNT(DISTINCT CarID) AS DuplicateCarIDs
FROM DimCar;
-- Justification : Cette requ�te v�rifie que les cl�s primaires dans la table DimCar sont uniques.

-- V�rification des cl�s �trang�res
SELECT COUNT(*) AS UnmatchedSales
FROM FactSales fs
LEFT JOIN DimCar dc ON fs.CarID = dc.CarID
WHERE dc.CarID IS NULL;
-- Justification : Cette requ�te v�rifie si toutes les ventes dans FactSales ont une correspondance dans la table de dimension DimCar.

-- V�rification de la qualit� des donn�es
SELECT COUNT(*) - COUNT(DISTINCT DateID) AS DuplicateDates
FROM DimDate;
-- Justification : Cette requ�te v�rifie si toutes les dates ID dans DimDate sont uniques, contribuant � la qualit� des donn�es.

-- V�rification de l'exhaustivit� des donn�es
SELECT COUNT(*) AS UnmatchedSales
FROM FactSales fs
LEFT JOIN DimSalesperson dsp ON fs.SalespersonID = dsp.SalespersonID
WHERE dsp.SalespersonID IS NULL;
-- Justification : Cette requ�te v�rifie si toutes les ventes ont un enregistrement correspondant dans la table de dimension DimSalesperson.

-- Test de performance
SELECT AVG(SalePrice) AS AverageSalePrice
FROM FactSales where SalePrice < 18000;
-- Justification : Cette requ�te mesure la performance d'une requ�te courante pour s'assurer qu'elle s'ex�cute de mani�re optimale.
