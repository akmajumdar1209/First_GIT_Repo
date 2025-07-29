-- Data Cleaning

SELECT * FROM layoffs;

-- 1. Remove Duplicates
-- 2. Stadardise data , fix the spellings
-- 3 . NULL values or blanks
-- 4. Remove Any column (unwanted/irrelevant) we should not remove from raw file , so we will create a table 

CREATE TABLE layoffs_staging
like layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs;


SELECT * ,
ROW_NUMBER() OVER( 
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off
,'date',stage,country,funds_raised_millions) as row_numb
FROM layoffs_staging;


WITH dublicate_cte AS 
(
SELECT * ,
ROW_NUMBER() OVER( 
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off
,`date`,stage,country,funds_raised_millions) as row_numb
FROM layoffs_staging
)
SELECT *
FROM dublicate_cte
WHERE row_numb > 1;

SELECT * FROM layoffs_staging
WHERE company = 'Oda';


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_numb` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;
-- truncate layoffs_staging2;

INSERT layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER( 
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,
"date",stage,country,funds_raised_millions) as row_numb
FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_numb >1;


SELECT * FROM layoffs_staging2
WHERE company = "Ola";

-- Standardizing data

SELECT DISTINCT (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT `date`
FROM layoffs_staging2;


-- NULL and blank values 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry =""; 

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'BALLY%';

SELECT DISTINCT company,industry,location
FROM layoffs_staging2 ;

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry is NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;    

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry is NULL )
AND t2.industry IS NOT NULL;  -- did not update blanks

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''; -- set the blanks to NULL and try again with update join
    
SELECT DISTINCT t1.industry,t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
 ORDER BY 1  ;
 
 SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_numb;