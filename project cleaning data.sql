select *
from layoffs;

-- 1. remove duplicates
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove not nesscary 

create table layoffs_staging
like layoffs;

select *
from layoffs_staging; 

insert layoffs_staging
select *
from layoffs;


select *, -- to remove the duplicate value by this filtter
ROW_NUMBER() OVER(
Partition by company ,location,industry,total_laid_off,percentage_laid_off,`date`,stage, country,funds_raised_millions) as row_num
from layoffs_staging ;


WITH DUBLICATE_CTE AS  -- we use CTE here 
(
select *,
ROW_NUMBER() OVER(
Partition by company ,location,industry,total_laid_off,percentage_laid_off,`date`,stage, country,funds_raised_millions) as row_num
from layoffs_staging
)

select *
from DUBLICATE_CTE 
where row_num>1;


        CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int -- me add this
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
             
             
select *
from layoffs_staging2;

             
             
insert  into layoffs_staging2
select *,
ROW_NUMBER() OVER(
Partition by company ,location,industry,total_laid_off,percentage_laid_off,`date`,stage, country,funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num>1;

delete
from layoffs_staging2
where row_num>1;

select *
from layoffs_staging2;



-- standrizing the data

select company ,trim(company) -- to make space first
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);


select DISTINCT industry 
from layoffs_staging2
ORDER BY 1 ;  -- THERE some same industry but different writting

select *
from layoffs_staging2
where industry like 'Crypto%';


update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select DISTINCT country  -- look on country to know if we want to fix it
from layoffs_staging2
ORDER BY 1 ;

delete 
from layoffs_staging2
where country = 'Israel';

update layoffs_staging2
set country= 'United States'
where country = 'United States%';

-- update layoffs_staging2
-- set country= trim (trailing '.' from country) 	 >>>>> mean deleting . if u find it
-- where country = 'United States%';             	 >>>>>> <Advance way>


select `date`, 
str_to_date(`date`, '%m/%d/%Y') -- this best format for date ( %m/%d/%Y ) capital Y
from layoffs_staging2;


update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2 
modify column `date` date;

  
 
-- Null values or Black values 


select *
from layoffs_staging2
where industry is null
 or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';  -- we know now that we can fill industry blank by anothe same record


select t1.industry , t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t2.industry='')
and t2.industry is not null;
    
update layoffs_staging2
set industry = null 
where industry = ''; -- >>>>>>>>>>>> we make all empty to null and jonig the values 

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

-- we cant fill laid null vaules , if we have the full information like total mount we can calculate them 
-- if we dont need them when they null so they will not be bineft we will delete them

select *
from layoffs_staging2;

    

-- Remove colunms

delete
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;



-- make by mohammad bader :)







