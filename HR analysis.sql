CREATE DATABASE Human_Resources;
select * from `tableconvert.com_s79v59`;




-- DATA CLEANING -- 

-- Setting data types -- 
ALTER TABLE `tableconvert.com_s79v59`
CHANGE COLUMN id emp_id VARCHAR(20),
MODIFY COLUMN first_name VARCHAR(20),
MODIFY COLUMN last_name VARCHAR(50),
MODIFY COLUMN gender VARCHAR(20),
MODIFY COLUMN race VARCHAR(50),
MODIFY COLUMN department VARCHAR(50),
MODIFY COLUMN jobtitle VARCHAR(100),
MODIFY COLUMN location VARCHAR(20),
MODIFY COLUMN location_city VARCHAR(20),
MODIFY COLUMN location_state VARCHAR(20);


DESCRIBE `tableconvert.com_s79v59`;




-- Changing format of BIRTH DATE --
SET sql_safe_updates = 0;
SELECT birthdate FROM `tableconvert.com_s79v59`;
UPDATE `tableconvert.com_s79v59`
SET birthdate =
CASE WHEN birthdate LIKE '%/%' THEN date_format(str_to_date(birthdate, '%m/%d/%Y'), '%Y-%m-%d')
	 WHEN birthdate LIKE '%-%' THEN date_format(str_to_date(birthdate, '%m-%d-%Y'), '%Y-%m-%d') 
     ELSE NULL
     END;
   



-- Converting BIRTH DATE from text to date --
ALTER TABLE `tableconvert.com_s79v59`
MODIFY COLUMN birthdate DATE;
SELECT birthdate FROM `tableconvert.com_s79v59`;
DESCRIBE `tableconvert.com_s79v59`;




-- Changing format of HIRE DATE --
UPDATE `tableconvert.com_s79v59`
SET hire_date =
CASE WHEN hire_date LIKE '%/%' THEN date_format(str_to_date(hire_date, '%m/%d/%Y'), '%Y-%m-%d')
	 WHEN hire_date LIKE '%-%' THEN date_format(str_to_date(hire_date, '%m-%d-%Y'), '%Y-%m-%d') 
     ELSE NULL
END;

SELECT hire_date from `tableconvert.com_s79v59`;




-- Converting HIRE DATE from text to date --
ALTER TABLE `tableconvert.com_s79v59`
MODIFY COLUMN hire_date DATE;
SELECT hire_date FROM `tableconvert.com_s79v59`;
DESCRIBE `tableconvert.com_s79v59`;




-- Removing timestamp from termdate --
UPDATE `tableconvert.com_s79v59`
SET termdate = 
CASE WHEN termdate = '' THEN NULL
	 ELSE DATE_FORMAT(STR_TO_DATE(termdate, '%Y-%m-%d %H:%i:%s UTC'), '%Y-%m-%d')
END
WHERE termdate IS NOT NULL AND termdate != '';

SELECT termdate FROM `tableconvert.com_s79v59`;




-- Converting TERM DATE from text to date --

UPDATE `tableconvert.com_s79v59`
SET termdate = NULL
WHERE termdate = '';
ALTER TABLE `tableconvert.com_s79v59`
CHANGE COLUMN termdate termdate DATE;

DESCRIBE `tableconvert.com_s79v59`;


-- Removing incorrect TERM DATE values --

SET sql_safe_updates = 0;
DELETE FROM `tableconvert.com_s79v59` WHERE YEAR(TERMDATE) > 2021;
SELECT termdate FROM `tableconvert.com_s79v59`;
SET sql_safe_updates = 1;

DESCRIBE `tableconvert.com_s79v59`;

-- Adding AGE column -- 
ALTER TABLE `tableconvert.com_s79v59` ADD COLUMN age INT;
UPDATE `tableconvert.com_s79v59`
SET age = timestampdiff(YEAR, birthdate, CURDATE());
SELECT age FROM `tableconvert.com_s79v59`;




-- Removing incorrect Age values --

SELECT COUNT(*) FROM `tableconvert.com_s79v59` WHERE AGE<18;
DELETE FROM `tableconvert.com_s79v59` WHERE AGE < 18;
SELECT age FROM `tableconvert.com_s79v59`;
SELECT min(AGE) AS youngest, max(AGE) AS oldest FROM `tableconvert.com_s79v59`;






-- Questions for analysis --

-- What are the top 5 most common job titles in the dataset? --

WITH JobTitleCounts AS 
(
    SELECT jobtitle, COUNT(*) AS title_count
    FROM `tableconvert.com_s79v59`
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
)
SELECT * FROM JobTitleCounts;



-- Provide a breakdown of the distribution of races among employees for diversity and inclusion analysis -- 

WITH RaceDistribution AS 
(
    SELECT race, COUNT(*) AS race_count
    FROM `tableconvert.com_s79v59`
    GROUP BY 1
    ORDER BY 2 DESC
)
SELECT * FROM RaceDistribution;



-- What is the average age of employees in each department? --

WITH AvgAgeByDepartment AS 
(
    SELECT department, AVG(age) AS avg_age
    FROM `tableconvert.com_s79v59`
    GROUP BY 1
    ORDER BY 2 DESC
)
SELECT * FROM AvgAgeByDepartment;



-- How many employees were hired in each year, and what's the trend? --

WITH HireTrend AS 
(
    SELECT YEAR(hire_date) AS hire_year, COUNT(*) AS hire_count
    FROM `tableconvert.com_s79v59`
    GROUP BY 1
    ORDER BY 1 DESC
)
SELECT * FROM HireTrend;



-- What is the retention rate of employees based on gender? -- 

WITH RetentionRate AS (
    SELECT gender,
           SUM(CASE WHEN termdate IS NULL THEN 1 ELSE 0 END) AS active_in_company,
           COUNT(*) AS total_count
    FROM `tableconvert.com_s79v59`
    GROUP BY gender
)
SELECT gender, active_in_company,total_count, (active_in_company / total_count)*100 AS retention_rate  FROM RetentionRate;



-- How many employees have been hired for each location over the years? -- 

WITH HireLocationTrend AS (
    SELECT YEAR(hire_date) AS hire_year, location, COUNT(*) AS hire_count
    FROM `tableconvert.com_s79v59`
    GROUP BY hire_year, location
    ORDER BY hire_year DESC, location
)
SELECT * FROM HireLocationTrend;



-- What is the distribution of employees' ages by gender? -- 

WITH AgeDistribution AS (
    SELECT gender, 
           SUM(CASE WHEN age >= 18 AND age <= 25 THEN 1 ELSE 0 END) AS age_18_25,
           SUM(CASE WHEN age > 25 AND age <= 32 THEN 1 ELSE 0 END) AS age_26_32,
		   SUM(CASE WHEN age > 32 AND age <= 39 THEN 1 ELSE 0 END) AS age_33_39,
           SUM(CASE WHEN age > 39 AND age <= 46 THEN 1 ELSE 0 END) AS age_40_46,
           SUM(CASE WHEN age >46  AND age <= 52 THEN 1 ELSE 0 END) AS age_47_52,
           SUM(CASE WHEN age > 52 AND age <= 59 THEN 1 ELSE 0 END) AS age_53_59,
           COUNT(*) AS total_count
    FROM `tableconvert.com_s79v59`
    GROUP BY gender
)
SELECT * FROM AgeDistribution;



-- How many employees have been hired in each city, and what's the trend? --

WITH HireCityTrend AS (
    SELECT YEAR(hire_date) AS hire_year, location_city, COUNT(*) AS hire_count
    FROM `tableconvert.com_s79v59`
    GROUP BY hire_year, location_city
    ORDER BY hire_year DESC, location_city, hire_count DESC
)
SELECT * FROM HireCityTrend;



-- Identify the top 5 most common combination of department and job title --

WITH DepartmentJobTitleCombo AS (
    SELECT department, jobtitle, COUNT(*) AS combo_count
    FROM `tableconvert.com_s79v59`
    GROUP BY department, jobtitle
    ORDER BY combo_count DESC
    LIMIT 5
)
SELECT * FROM DepartmentJobTitleCombo;



-- How many employees have left each year? --

WITH EmployeeTurnover AS (
    SELECT YEAR(termdate) AS turnover_year, COUNT(*) AS turnover_count
    FROM `tableconvert.com_s79v59`
    WHERE termdate IS NOT NULL
    GROUP BY turnover_year
    ORDER BY turnover_count DESC
)
SELECT * FROM EmployeeTurnover;



-- What is the average length of employment for terminated employees? --

SELECT COUNT(*) AS terminated_employees, AVG(DATEDIFF(termdate, hire_date))/365 AS avg_length_of_employment_years
FROM `tableconvert.com_s79v59`
WHERE termdate IS NOT NULL;



-- Which departments have the top 5 highest turnover rate -- 

WITH DepartmentTurnover AS 
(
    SELECT department,
           SUM(CASE WHEN termdate IS NOT NULL THEN 1 ELSE 0 END) AS terminated_count,
           COUNT(*) AS total_count
    FROM `tableconvert.com_s79v59`
    GROUP BY department
)
SELECT department,
       terminated_count,
       total_count,
       (terminated_count / total_count) * 100 AS turnover_rate_percentage
FROM DepartmentTurnover
ORDER BY turnover_rate_percentage DESC
LIMIT 5;



-- What is the tenure distribution for each department -- 

WITH EmployeeTenure AS (
    SELECT department,
           DATEDIFF(NOW(), hire_date) AS tenure_days
    FROM `tableconvert.com_s79v59`
)
SELECT department,
       AVG(tenure_days)/365 AS avg_tenure_years,
       MIN(tenure_days)/365 AS min_tenure_years,
       MAX(tenure_days)/365 AS max_tenure_years
FROM EmployeeTenure
GROUP BY department;
















