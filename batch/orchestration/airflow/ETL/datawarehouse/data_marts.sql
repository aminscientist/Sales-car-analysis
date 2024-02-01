CREATE VIEW VenteParPays AS
SELECT
    fs.SaleID,
    dc.CarMake,
    dc.CarModel,
    dc.OriginCountry AS CarOriginCountry,
    dc.Fuel,
    dc.VehicleType,
    dc.Transmission,
    dc.CarID,
    dcm.CustomerName,
    dcm.Country AS CustomerCountry,
    dd.Date,
    dd.CarYear,
    dd.DateID,
    ds.SalespersonName,
    ds.SalespersonID,
    fs.CommissionEarned,
    fs.CommissionRate,
    fs.SalePrice,
    fs.KilometersDriven,
    fs.EngineSize,
    fs.Horsepower,
    fs.Cylinders,
    fs.Weight,
    fs.Speed,
    fs.Doors,
    fs.EPAFuelEconomyScore,
    fs.FuelEfficiency,
    fs.SafetyRatings
FROM
    FactSales fs
JOIN DimCar dc ON fs.CarID = dc.CarID
JOIN DimCustomer dcm ON fs.CustomerID = dcm.CustomerID
JOIN DimDate dd ON fs.DateID = dd.DateID
JOIN DimSalesperson ds ON fs.SalespersonID = ds.SalespersonID;

CREATE VIEW VenteParVehicule AS
SELECT
    fs.SaleID,
    dc.CarMake,
    dc.CarModel,
    dc.OriginCountry,
    dc.Fuel,
    dc.VehicleType,
    dc.Transmission,
    fs.EngineSize,
    fs.Horsepower,
    fs.Cylinders,
    fs.Weight,
    fs.Speed,
    fs.Doors,
    fs.EPAFuelEconomyScore,
    fs.FuelEfficiency,
    fs.SafetyRatings,
    ds.SalespersonName,
    dd.Date,
    dd.CarYear,
    dc.CarID,
    dc.OriginCountry,
    dc.Fuel,
    dc.VehicleType,
    dc.Transmission,
    fs.CommissionEarned,
    fs.CommissionRate,
    fs.SalePrice,
    fs.KilometersDriven
FROM FactSales fs
JOIN DimCar dc ON fs.CarID = dc.CarID
JOIN DimSalesperson ds ON fs.SalespersonID = ds.SalespersonID
JOIN DimDate dd ON fs.DateID = dd.DateID;
