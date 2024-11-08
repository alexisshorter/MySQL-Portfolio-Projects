-- EXPLORATORY DATA ANALYSIS
-- DATASET: https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT *
FROM layoffs_staging2;

# Exploring the data freely

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

# Seeing who had the highest amount of layoffs and most funds raised

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

# Looking at the total number of people companies let go of

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

# Evaluating timeline of these layoffs

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

# Interestingly enough it's within COVID years

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

# Makes sense that the consumer and retail industries would have the most layoffs these years due to COVID

# Curious to see which countries had the most layoffs

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

# By far the United States had the most

SELECT YEAR (`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR (`date`)
ORDER BY 1 DESC;

# 3 months into 2023 (when this data was recorded)
# and layoffs are nearly exceeding the amount for the entire year of 2022

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

# Large companies that are Post-IPO had the most layoffs

# Performing a rolling sum of the months

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

# Using a CTE to visualize rolling total of layoffs by month

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off
, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

# Layoffs in 2021 were quite comparatively low
# Layoffs in 2022 ramped up dramatically

# Want to see how many layoffs for companies per year

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

# Creating another CTE to rank layoffs by company for each year

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5;




