-- Remove Duplicates

-- copy the raw data layoffs 
create table layoffs_staging
like layoffs;

insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;


-- creating new table for to delete the duplicate value
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
   `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
from layoffs_staging;

-- deleting the duplicate value as row_num > 1
delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;


-- standerdizing data


-- cheking if the column company has an issue
select distinct company
from layoffs_staging2;

-- fix the column company in TRIM  function
select company, trim(company)
from layoffs_staging2;

-- update the column company value 
update layoffs_staging2
set company =  trim(company);


-- cheking if the column industry has an issue
select distinct industry
from layoffs_staging2
order by 1;

-- update the proper keywords in the industry column 
update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

-- see the result
select  distinct industry
from layoffs_staging2
where industry like 'crypto%';


-- cheking if the column country has an issue
select distinct country
from layoffs_staging2
order by 1;

-- find the simillar value in the country column
select distinct country
from layoffs_staging2
where country like 'United States%';

-- using trim trailing to remove the extention value in column country
select distinct country, trim(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(TRAILING '.' FROM country)
where country like 'United States%';


-- chage the fomrat of date
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

-- alter the column text-date to DATE
alter table layoffs_staging2
modify column `date` DATE;


-- modify/change the blank value to null 


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = ''
;

-- changeing the null into same value of the identical row value
select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- Removing Any Columns that are not needed

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


-- dropping the unnecessary column 
alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;

