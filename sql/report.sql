-- Spark SQL dialect for data load/download

CREATE TEMPORARY TABLE Flights1987 
USING CSV
OPTIONS (
	PATH "/home/neumann/Downloads/data/1987.csv",
	HEADER "true"
);

-- INSERT OVERWRITE DIRECTORY USING CSV OPTIONS(PATH "/home/neumann/data/csv")
SELECT Origin, SUM(CAST(Distance AS Integer)) AS TotalDistance
FROM Flights1987
GROUP BY Origin
ORDER BY Origin;

quit;