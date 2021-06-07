--1 Tabla intermedia

CREATE TABLE intermedia --TODO agregar validaciones del formato en las fechas
(
    Quarter       text not null CHECK ( Quarter ~ '^Q[1-4]/[0-9]{4}$' ),
    Month         text not null CHECK ( Month ~ '^[0-9]{2}-[A-Z][a-z]{2}$' ),
    Week          text not null CHECK ( Week ~ '^W[1-5]-[0-9]{4}$' ),
    Product_type  text not null,
    Territory     text not null,
    Sales_Channel text not null,
    Customer_type text not null,
    Revenue       float,
    Cost          float,
    PRIMARY KEY (Quarter, Month, Week, Product_type, Territory, Sales_Channel, Customer_type)
);

--2 Tabla definitiva

CREATE TABLE definitiva
(
    Sales_Date    date not null,
    Product_type  text not null,
    Territory     text not null,
    Sales_Channel text not null,
    Customer_type text not null,
    Revenue       float,
    Cost          float,
    PRIMARY KEY (Sales_Date, Product_type, Territory, Sales_Channel, Customer_type)
);

--3 Importación de datos

CREATE OR REPLACE FUNCTION insertarEnDefinitiva() RETURNS TRIGGER
AS
$$
DECLARE
    aDay   int;
    aMonth int;
    aYear  int;
BEGIN
    aDay := CASE substr(new.Week, 1, 2)
                WHEN 'W1' THEN 1
                WHEN 'W2' THEN 8
                WHEN 'W3' THEN 15
                WHEN 'W4' THEN 22
                WHEN 'W5' THEN 29
        END;
    aMonth := EXTRACT(MONTH FROM TO_DATE(substr(new.Month, 4, 3), 'Mon'));
    aYear := substr(new.Quarter, 4, 4)::INTEGER;

    INSERT INTO definitiva
    VALUES (make_date(aYear, aMonth, aDay), new.Product_type, new.Territory, new.Sales_Channel, new.Customer_type,
            new.Revenue, new.Cost);
    RETURN new;
END
$$ LANGUAGE plpgsql;

-----------------------

CREATE TRIGGER insertoEnIntermedia
    AFTER INSERT
    ON intermedia
    FOR EACH ROW
EXECUTE PROCEDURE insertarEnDefinitiva();

-----------------------

COPY intermedia FROM 'C:\Mati\DataGripProjects\TPE_BD\SalesbyRegion.csv' WITH DELIMITER ',' CSV HEADER;

--4 Cálculo de la mediana

CREATE OR REPLACE FUNCTION MedianaMargenMovil(fecha date, n int) RETURNS float
AS
$$
DECLARE
    initDate       date;
    margenDeVentas float;
BEGIN
    initDate := fecha - INTERVAL '1 month' * n;

    SELECT percentile_cont(0.5) within group (order by Revenue - Cost)
    INTO margenDeVentas
    FROM definitiva
    WHERE Sales_Date > initDate
      AND Sales_Date <= fecha;

    return margenDeVentas;
END
$$ LANGUAGE plpgsql;

-----------------------

SELECT MedianaMargenMovil(to_date('2012-11-01', 'YYYY-MM-DD'), 4); --TODO los decimales e imprimir un mensaje si el parametro es 0

--5 Reporte de Ventas
CREATE VIEW salesView(Sales_Year, Product_type, Sales_Channel, Customer_type, Revenue, Cost) AS
SELECT EXTRACT(YEAR from Sales_Date) AS Sales_Year,
       Product_type,
       Sales_Channel,
       Customer_type,
       Revenue,
       Cost
FROM definitiva;


CREATE OR REPLACE FUNCTION ReporteVentas(n int) RETURNS void -- TODO: Chequear input 0 o NULL etc
AS
$$
DECLARE
    aRec         record;
    initDate     date;
    initYear     int;
    lastYear     int;
    yearIdx      int;
    totalRevenue int;
    totalCost    int;
    customerTypeCursor CURSOR (selectedYear int) FOR SELECT Customer_type,
                                                            Sum(Cost)::Int    AS Cost,
                                                            Sum(Revenue)::Int as Revenue
                                                     FROM salesView
                                                     WHERE Sales_Year = selectedYear
                                                     GROUP BY Customer_type
                                                     ORDER BY Customer_type;
    productTypeCursor CURSOR  (selectedYear int) FOR SELECT Product_type,
                                                            Sum(Cost)::Int    AS Cost,
                                                            Sum(Revenue)::Int as Revenue
                                                     FROM salesView
                                                     WHERE Sales_Year = selectedYear
                                                     GROUP BY Product_type
                                                     ORDER BY Product_type;
    salesChannelCursor CURSOR (selectedYear int) FOR SELECT Sales_Channel,
                                                            Sum(Cost)::Int    AS Cost,
                                                            Sum(Revenue)::Int as Revenue
                                                     FROM salesView
                                                     WHERE Sales_Year = selectedYear
                                                     GROUP BY Sales_Channel
                                                     ORDER BY Sales_Channel;
BEGIN
    SELECT MIN(Sales_Date) INTO initDate FROM definitiva;
    initYear := EXTRACT(YEAR FROM initDate);
    lastYear := initYear + n - 1;

    yearIdx := initYear;

    RAISE NOTICE '--------------------- HISTORIC SALES REPORT -----------------------';
    RAISE NOTICE '-------------------------------------------------------------------';
    RAISE NOTICE 'Year--Category------------------------------Revenue---Cost---Margin';

    WHILE (yearIdx <= lastYear)
        LOOP
            totalRevenue := 0;
            totalCost := 0;
            RAISE NOTICE '-------------------------------------------------------------------';
            RAISE NOTICE '%', yearIdx;
            OPEN customerTypeCursor(selectedYear := yearIdx);
            LOOP
                FETCH customerTypeCursor INTO aRec;
                EXIT WHEN NOT FOUND;
                totalRevenue := totalRevenue + aRec.Revenue;
                totalCost := totalCost + aRec.Cost;
                RAISE NOTICE '--- Customer Type: % % % %', aRec.Customer_type, aRec.Revenue, aRec.Cost, aRec.Revenue - aRec.Cost;
            END LOOP;
            CLOSE customerTypeCursor;

            OPEN productTypeCursor(selectedYear := yearIdx);
            LOOP
                FETCH productTypeCursor INTO aRec;
                EXIT WHEN NOT FOUND;
                totalRevenue := totalRevenue + aRec.Revenue;
                totalCost := totalCost + aRec.Cost;
                RAISE NOTICE '--- Product Type: % % % %', aRec.Product_type, aRec.Revenue, aRec.Cost, aRec.Revenue - aRec.Cost;
            END LOOP;
            CLOSE productTypeCursor;

            OPEN salesChannelCursor(selectedYear := yearIdx);
            LOOP
                FETCH salesChannelCursor INTO aRec;
                EXIT WHEN NOT FOUND;
                totalRevenue := totalRevenue + aRec.Revenue;
                totalCost := totalCost + aRec.Cost;
                RAISE NOTICE '--- Sales Channel: % % % %', aRec.Sales_Channel, aRec.Revenue, aRec.Cost, aRec.Revenue - aRec.Cost;
            END LOOP;
            CLOSE salesChannelCursor;

            RAISE NOTICE '---------------------------------------------- % % %', totalRevenue, totalCost, totalRevenue - totalCost;
            yearIdx := yearIdx + 1;
        END LOOP;


END
$$ LANGUAGE plpgsql;

-----------------------

DO
$$
    BEGIN
        PERFORM ReporteVentas(1);
    END;
$$;

DROP FUNCTION ReporteVentas;
