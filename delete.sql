SELECT * FROM layoff_staging;

SELECT *,
row_number() OVER(
PARTITION BY company,location,total_laid_off,percentage_laid_off, `date`) AS row_num
FROM layoff_staging;

WITH cte_duplicate AS 
(SELECT *,
row_number() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off, `date`, stage,country,funds_raised_millions) AS row_num
FROM layoff_staging)
SELECT * FROM cte_duplicate
WHERE row_num>1;


CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoff_staging2;
 
 INSERT INTO layoff_staging2
 SELECT *,
row_number() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off, `date`, stage,country,funds_raised_millions) AS row_num
 FROM layoff_staging;

 SELECT * FROM layoff_staging2
 WHERE row_num>1;

 DELETE FROM layoff_staging2
 WHERE row_num > 1;
 
SELECT * FROM layoff_staging2;
 
 -- Standardizing data
 
 SELECT company, TRIM(company) 
 FROM layoff_staging2;
 
UPDATE layoff_staging2
SET company= TRIM(company);

SELECT DISTINCT country
 FROM layoff_staging2
 ORDER BY 1 ;

UPDATE layoff_staging2
SET industry='CRYPTO'
WHERE industry like 'Crypto%';

UPDATE layoff_staging
SET country='United States'
WHERE country LIKE 'United States%';
 
 SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) 
 from layoff_staging2
 ORDER BY 1;
 
 UPDATE layoff_staging2
 SET country=TRIM(TRAILING '.' FROM country) 
WHERE country LIKE 'United States%';
 
 UPDATE layoff_staging2
 SET `date` = str_to_date(`date`,'%m/%d/%Y');
  
  SELECT `date`
  FROM layoff_staging2;
  
  ALTER TABLE layoff_staging2
  MODIFY column `date` DATE;
  
  SELECT * FROM layoff_staging2
  WHERE total_laid_off  is NULL
  AND percentage_laid_off IS NULL;
  
  UPDATE layoff_staging2
  SET industry = NULL
  WHERE industry='';

SELECT * 
FROM layoff_staging2 t1
JOIN layoff_staging2 t2
ON t1.company=t2.company
AND t1.location= t2.location
WHERE t1.industry is NULL
AND t2.industry IS NOT NULL;


UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT * FROM layoff_staging2
WHERE company='Airbnb';

SELECT * FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE  FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
 
 
 SELECT * from layoff_staging2;

 ALTER TABLE layoff_staging2
 DROP COLUMN row_num;





















