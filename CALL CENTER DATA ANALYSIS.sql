CREATE DATABASE SQL_PROJECTS;
USE SQL_PROJECTS;
CREATE table calls (ID char(50), cust_name CHAR (50),
sentiment CHAR (50), csat_score INT, call_timestamp CHAR (10),
reason CHAR (20), city CHAR (20), state CHAR (20), channel CHAR (20), 
response_time CHAR (20), call_duration_minutes INT, call_center CHAR (20)
);
select * from calls;

SET SQL_safe_updates = 0 ;
UPDATE calls SET call_timestamp = str_to_date(call_timestamp , "%m/%d/%Y");
UPDATE calls SET csat_score = NULL where csat_score=0;
SET SQL_safe_updates = 1 ;
SELECT * FROM calls LIMIT 10;
 
-- what is the shape of our data 
select * from calls;
USE SQL_PROJECTS;
SELECT 
  (SELECT COUNT(*) FROM calls) AS number_of_rows,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'calls') AS number_of_columns;


SELECT DISTINCT sentiment FROM calls;
SELECT DISTINCT reason FROM calls;
SELECT DISTINCT channel FROM calls;
SELECT DISTINCT response_time FROM calls;
SELECT DISTINCT call_center FROM calls;


-- What is the count and percentage of each distinct values of the above 5 columns
USE SQL_PROJECTS;

SELECT sentiment, COUNT(*), ROUND ((COUNT(*) / (select COUNT(*) FROM calls))*100 ,1) AS percentage
FROM calls 
GROUP BY 1 ORDER BY 3 DESC;

SELECT DISTINCT reason, COUNT(*), ROUND ((COUNT(*)/(SELECT COUNT(*) FROM calls))*100, 1) AS percentage
FROM calls
GROUP BY 1 ORDER BY 3 DESC;

SELECT DISTINCT channel, COUNT(*), ROUND ((COUNT(*)/(SELECT COUNT(*) FROM calls))*100, 1) AS percentage
FROM calls
GROUP BY 1 ORDER BY 3 DESC;

SELECT DISTINCT response_time, COUNT(*), ROUND((COUNT(*)/(SELECT COUNT(*) FROM calls))*100, 1) AS percentage
FROM calls
GROUP BY 1 ORDER BY 3 DESC;

SELECT DISTINCT call_center, COUNT(*), ROUND((COUNT(*)/(SELECT COUNT(*) FROM calls))*100, 1) AS percentage
FROM calls
GROUP BY 1 ORDER BY 3 DESC;


-- Which states attend the most calls and least calls? 

WITH top_10 AS (
  SELECT state AS top_10_states, COUNT(*) AS top_10_count,
         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS top_10_rank
  FROM calls 
  GROUP BY state
  ORDER BY COUNT(*) DESC
  LIMIT 10
),
bottom_10 AS (
  SELECT state AS bottom_10_states, COUNT(*) AS bottom_10_count,
         ROW_NUMBER() OVER (ORDER BY COUNT(*) ASC) AS bottom_10_rank
  FROM calls 
  GROUP BY state
  ORDER BY COUNT(*) ASC
  LIMIT 10
)
SELECT top_10_states, top_10_count, bottom_10_states, bottom_10_count
FROM top_10
JOIN bottom_10 ON top_10.top_10_rank = bottom_10.bottom_10_rank;


-- Which day has the most calls?
SELECT DAYNAME(call_timestamp) AS day_of_call, COUNT(*) AS number_of_calls 
FROM calls 
GROUP BY 1 ORDER BY 2 DESC;

-- aggregations

SELECT MIN(csat_score) AS min_score, MAX(csat_score) AS max_score, ROUND (AVG(csat_score),1) AS avg_score
FROM calls
WHERE csat_score != 0;

SELECT MIN(call_timestamp) AS earliest_date, MAX(call_timestamp) AS latest_date 
FROM calls;

SELECT MIN(call_duration_minutes) AS min_call_duration, MAX(call_duration_minutes) AS max_call_duration, 
	  AVG(call_duration_minutes) AS avg_call_duration
FROM calls;

SELECT * FROM calls WHERE call_duration_minutes >= 45 AND sentiment NOT IN ('POSITIVE', 'VERY POSITIVE', 'NEUTRAL'); 


SELECT reason, COUNT(*) FROM calls 
WHERE call_duration_minutes >= 45 AND sentiment NOT IN ('POSITIVE', 'VERY POSITIVE', 'NEUTRAL')
GROUP BY 1 ORDER BY 2 DESC;


SELECT call_center, response_time, COUNT(*) AS count
FROM calls 
GROUP BY 1,2 ORDER BY 1,3 DESC; 

SELECT call_center, AVG(call_duration_minutes) FROM calls
GROUP BY 1 ORDER BY 2 DESC;


USE SQL_PROJECTS;
SELECT call_center, channel
FROM (
  SELECT call_center, channel,
         ROW_NUMBER() OVER (PARTITION BY call_center ORDER BY COUNT(*) DESC) as channel_rank
  FROM calls
  GROUP BY call_center, channel
) ranked_channel
WHERE channel_rank = 1;




SELECT channel, AVG(call_duration_minutes) FROM calls
GROUP BY 1 ORDER BY 2 DESC;


SELECT state, sentiment, COUNT(*) FROM calls
GROUP BY 1, 2 ORDER BY 1, 3 DESC;

SELECT state, avg_csat_Score,sentiment FROM 
(
SELECT state, AVG(csat_score) as avg_csat_score, sentiment FROM calls WHERE csat_score!= 0 GROUP BY 1,3 ORDER BY 1,2 DESC
)
AS subquery
WHERE sentiment = "very negative" OR sentiment = "very positive";

SELECT sentiment, AVG(call_duration_minutes) FROM calls GROUP BY 1 ORDER BY 2 DESC;





WITH daily_call_duration AS (
  SELECT
  DATE(call_timestamp) AS busy_day, SUM(call_duration_minutes) AS total_call_duration
  FROM calls
  GROUP BY 1
),
top_10 AS (
  SELECT
   busy_day, total_call_duration, ROW_NUMBER() OVER (ORDER BY total_call_duration DESC) AS top_10_rank
  FROM daily_call_duration
  ORDER BY 2 DESC LIMIT 10
),
bottom_10 AS (
  SELECT 
  busy_day, total_call_duration, ROW_NUMBER() OVER (ORDER BY total_call_duration ASC) AS bottom_10_rank
  FROM daily_call_duration
  ORDER BY total_call_duration ASC LIMIT 10
)
SELECT
  top_10.busy_day AS top_10_busy_days,
  top_10.total_call_duration AS top_10_busy_days_total_call_duration,
  bottom_10.busy_day AS bottom_10_busy_days,
  bottom_10.total_call_duration AS bottom_10_busy_days_total_call_duration
FROM top_10
JOIN bottom_10 ON top_10.top_10_rank = bottom_10.bottom_10_rank;


SELECT sentiment, AVG(csat_score) AS avg_csat_score FROM calls
GROUP BY 1 ORDER BY 2 DESC;

  
  
SELECT state, AVG(CSAT_SCORE) FROM calls
GROUP BY 1 ORDER BY 2 DESC;




