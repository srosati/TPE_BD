--1 Tabla intermedia

CREATE TABLE intermedia
(
    Quarter       TEXT NOT NULL CHECK ( Quarter ~ '^Q[1-4]/[0-9]{4}$' ),
    Month         TEXT NOT NULL CHECK ( substr(Month, 4, 3) IN ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                                                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
                                            AND substr(Month, 1, 3) ~ '^[0-9]{2}-$' ),
    Week          TEXT NOT NULL CHECK ( Week ~ '^W[1-5]-[0-9]{4}$' ),
    Product_type  TEXT NOT NULL,
    Territory     TEXT NOT NULL,
    Sales_Channel TEXT NOT NULL,
    Customer_type TEXT NOT NULL,
    Revenue       FLOAT CHECK ( Revenue >= 0 ),
    Cost          FLOAT CHECK ( Cost >= 0 ),
    PRIMARY KEY (Month, Week, Product_type, Territory, Sales_Channel, Customer_type)
);

--2 Tabla definitiva

CREATE TABLE definitiva
(
    Sales_Date    DATE NOT NULL,
    Product_type  TEXT NOT NULL,
    Territory     TEXT NOT NULL,
    Sales_Channel TEXT NOT NULL,
    Customer_type TEXT NOT NULL,
    Revenue       FLOAT CHECK ( Revenue >= 0 ),
    Cost          FLOAT CHECK ( Cost >= 0 ),
    PRIMARY KEY (Sales_Date, Product_type, Territory, Sales_Channel, Customer_type)
);

--3 Importación de datos

CREATE OR REPLACE FUNCTION calcularDia(semana TEXT) RETURNS INT -- Funcion auxiliar para el mapeo entre semana y número
    RETURNS NULL ON NULL INPUT
AS
$$
BEGIN
    RETURN CASE substr(semana, 1, 2)
               WHEN 'W1' THEN 1
               WHEN 'W2' THEN 8
               WHEN 'W3' THEN 15
               WHEN 'W4' THEN 22
               WHEN 'W5' THEN 29
        END;
END
$$ LANGUAGE plpgsql;

-----------------------

CREATE OR REPLACE FUNCTION insertarEnDefinitiva() RETURNS TRIGGER -- Trigger llamado al insertar tupla en intermedia
AS
$$
DECLARE
    aDay   INT;
    aMonth INT;
    aYear  INT;
BEGIN
    aDay := calcularDia(substr(new.Week, 1, 2));
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

COPY intermedia FROM '/absolute/path/to/csvFile' WITH DELIMITER ',' CSV HEADER; -- Reemplazar por el path absoluto al archivo que se desea importar

--4 Cálculo de la mediana

CREATE OR REPLACE FUNCTION MedianaMargenMovil(fecha date, n int) RETURNS DECIMAL(6, 2)
    RETURNS NULL ON NULL INPUT
AS
$$
DECLARE
    initDate       DATE;
    margenDeVentas DECIMAL(6, 2);
BEGIN
    IF (n <= 0) THEN
        RAISE NOTICE 'La cantidad de meses anteriores debe ser mayor a 0';
        RETURN NULL;
    END IF;
    initDate := fecha - INTERVAL '1 month' * n;

    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY Revenue - Cost)
    INTO margenDeVentas
    FROM definitiva
    WHERE Sales_Date > initDate
      AND Sales_Date <= fecha;

    IF (margenDeVentas IS NULL) THEN
        RAISE NOTICE 'No hay datos para el rango seleccionado';
    END IF;

    RETURN margenDeVentas;
END
$$ LANGUAGE plpgsql;

-----------------------

--5 Reporte de Ventas

CREATE OR REPLACE FUNCTION ReporteVentas(n INT) RETURNS VOID
    RETURNS NULL ON NULL INPUT
AS
$$
DECLARE
    aRec         RECORD;
    lastYear     INT;
    yearIdx      INT;
    totalRevenue FLOAT;
    totalCost    FLOAT;
    yearStr      TEXT;
    customerTypeCursor CURSOR (selectedYear INT) FOR SELECT Customer_type,
                                                            Sum(Cost)    AS Cost,
                                                            Sum(Revenue) AS Revenue
                                                     FROM definitiva
                                                     WHERE EXTRACT(YEAR FROM Sales_Date) = selectedYear
                                                     GROUP BY Customer_type
                                                     ORDER BY Customer_type;

    productTypeCursor CURSOR  (selectedYear INT) FOR SELECT Product_type,
                                                            Sum(Cost)    AS Cost,
                                                            Sum(Revenue) AS Revenue
                                                     FROM definitiva
                                                     WHERE EXTRACT(YEAR FROM Sales_Date) = selectedYear
                                                     GROUP BY Product_type
                                                     ORDER BY Product_type;

    salesChannelCursor CURSOR (selectedYear INT) FOR SELECT Sales_Channel,
                                                            Sum(Cost)    AS Cost,
                                                            Sum(Revenue) AS Revenue
                                                     FROM definitiva
                                                     WHERE EXTRACT(YEAR FROM Sales_Date) = selectedYear
                                                     GROUP BY Sales_Channel
                                                     ORDER BY Sales_Channel;
BEGIN
    IF (n <= 0) THEN
        RAISE NOTICE 'La cantidad de años debe ser mayor a 0';
        RETURN;
    END IF;

    SELECT EXTRACT(YEAR FROM MIN(Sales_Date)), EXTRACT(YEAR FROM MAX(Sales_Date)) INTO yearIdx, lastYear FROM definitiva;

    IF (yearIdx IS NULL) THEN
        RAISE NOTICE 'No hay valores en la tabla';
        RETURN;
    END IF;

    IF (lastYear > yearIdx + n - 1) THEN
        lastYear := yearIdx + n - 1;
    END IF;

    RAISE NOTICE '--------------------- HISTORIC SALES REPORT -----------------------';
    RAISE NOTICE '-------------------------------------------------------------------';
    RAISE NOTICE 'Year--Category------------------------------Revenue---Cost---Margin';

    WHILE (yearIdx <= lastYear)
        LOOP
            yearStr := yearIdx;
            totalRevenue := 0;
            totalCost := 0;
            RAISE NOTICE '-------------------------------------------------------------------';

            OPEN customerTypeCursor(selectedYear := yearIdx);
            LOOP
                FETCH customerTypeCursor INTO aRec;
                EXIT WHEN NOT FOUND;
                totalRevenue := totalRevenue + aRec.Revenue;
                totalCost := totalCost + aRec.Cost;
                RAISE NOTICE '% Customer Type: % % % %', yearStr, aRec.Customer_type, aRec.Revenue::INT, aRec.Cost::INT, (aRec.Revenue - aRec.Cost)::INT;
                yearStr := '----';
            END LOOP;
            CLOSE customerTypeCursor;

            OPEN productTypeCursor(selectedYear := yearIdx);
            LOOP
                FETCH productTypeCursor INTO aRec;
                EXIT WHEN NOT FOUND;
                RAISE NOTICE '---- Product Type: % % % %', aRec.Product_type, aRec.Revenue::INT, aRec.Cost::INT, (aRec.Revenue - aRec.Cost)::INT;
            END LOOP;
            CLOSE productTypeCursor;

            OPEN salesChannelCursor(selectedYear := yearIdx);
            LOOP
                FETCH salesChannelCursor INTO aRec;
                EXIT WHEN NOT FOUND;
                RAISE NOTICE '---- Sales Channel: % % % %', aRec.Sales_Channel, aRec.Revenue::INT, aRec.Cost::INT, (aRec.Revenue - aRec.Cost)::INT;
            END LOOP;
            CLOSE salesChannelCursor;

            RAISE NOTICE '---------------------------------------------- % % %', totalRevenue::INT, totalCost::INT, (totalRevenue - totalCost)::INT;

            yearIdx := yearIdx + 1;
        END LOOP;
END
$$ LANGUAGE plpgsql;

-----------------------

-- Sentencias para ejecutar ejemplos MedianaMargenMovil
SELECT MedianaMargenMovil(to_date('2012-11-01','YYYY-MM-DD'),3);

SELECT MedianaMargenMovil(to_date('2012-11-01','YYYY-MM-DD'),4);

SELECT MedianaMargenMovil(to_date('2011-09-01','YYYY-MM-DD'),5);

SELECT MedianaMargenMovil(to_date('2012-11-01','YYYY-MM-DD'),0);

-- Sentencias para ejecutar ejemplos ReporteDeVentas
DO
$$
    BEGIN
        PERFORM ReporteVentas(1);
    END;
$$;

-----------------------

DO
$$
    BEGIN
        PERFORM ReporteVentas(2);
    END;
$$;

-----------------------

DO
$$
    BEGIN
        PERFORM ReporteVentas(3);
    END;
$$;

-----------------------

DO
$$
    BEGIN
        PERFORM ReporteVentas(0);
    END;
$$;

-----------------------

-- Sentencias para borrar las distintas funciones y tablas creadas

DROP FUNCTION reporteventas;

DROP FUNCTION MedianaMargenMovil;

DROP FUNCTION calcularDia;

DROP TRIGGER insertoEnIntermedia ON intermedia;

DROP FUNCTION insertarEnDefinitiva;

DROP TABLE definitiva;

DROP TABLE intermedia;