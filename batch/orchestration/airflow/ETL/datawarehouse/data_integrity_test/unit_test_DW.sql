-- Test unitaire de la Data Warehouse

-- Vérification des clés primaires
SELECT COUNT(*) - COUNT(DISTINCT CarID) AS DuplicateCarIDs
FROM DimCar;
-- Justification : Cette requête vérifie que les clés primaires dans la table DimCar sont uniques.

-- Vérification des clés étrangères
SELECT COUNT(*) AS UnmatchedSales
FROM FactSales fs
LEFT JOIN DimCar dc ON fs.CarID = dc.CarID
WHERE dc.CarID IS NULL;
-- Justification : Cette requête vérifie si toutes les ventes dans FactSales ont une correspondance dans la table de dimension DimCar.

-- Vérification de la qualité des données
SELECT COUNT(*) - COUNT(DISTINCT DateID) AS DuplicateDates
FROM DimDate;
-- Justification : Cette requête vérifie si toutes les dates ID dans DimDate sont uniques, contribuant à la qualité des données.

-- Vérification de l'exhaustivité des données
SELECT COUNT(*) AS UnmatchedSales
FROM FactSales fs
LEFT JOIN DimSalesperson dsp ON fs.SalespersonID = dsp.SalespersonID
WHERE dsp.SalespersonID IS NULL;
-- Justification : Cette requête vérifie si toutes les ventes ont un enregistrement correspondant dans la table de dimension DimSalesperson.

-- Test de performance
SELECT AVG(SalePrice) AS AverageSalePrice
FROM FactSales where SalePrice < 18000;
-- Justification : Cette requête mesure la performance d'une requête courante pour s'assurer qu'elle s'exécute de manière optimale.
