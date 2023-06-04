/*
Skills used: Joins, CTE's, Sub-Queries, Windows Functions, Aggregate Functions, Converting Data Types

The data is related to the credit card transactions in India from between 2013 and 2015.
Table:
credit_card_transactions(transaction_id,city,transaction_date,card_type,exp_type,gender,amount) 

1- Write a query to print top 5 cities with highest spends and their percentage contribution of total 
credit card spends.
*/

WITH CTE1 AS (
SELECT city,
SUM(amount) AS spend
FROM credit_card_transactions
GROUP BY city),
CTE2 AS (
SELECT SUM(CAST(amount AS bigint)) AS total_spend
FROM credit_card_transactions)
SELECT TOP 5 city,spend
,ROUND(100.0*spend/total_spend,2) AS percent_spend 
FROM CTE1 INNER JOIN CTE2
ON 1=1
ORDER BY percent_spend DESC;

/*
2- Write a query to print highest spend month and amount spent in that month for each card type.
*/

WITH total_spend AS (
SELECT card_type
,SUM(amount) AS amount_spend
,DATEPART(MONTH,transaction_date) AS highest_spend_month
,DATEPART(YEAR,transaction_date) AS yr
FROM credit_card_transactions
GROUP BY card_type,DATEPART(YEAR,transaction_date),DATEPART(MONTH,transaction_date))
SELECT card_type,yr,highest_spend_month,amount_spend
FROM (SELECT *
,DENSE_RANK() OVER(PARTITION BY card_type ORDER BY amount_spend DESC) AS ranking
FROM total_spend)a
WHERE ranking=1;

/*
3- Write a query to print the transaction details(all columns from the table) for each card type when 
it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type.
*/

WITH agg_data AS (
SELECT * 
,SUM(amount) OVER(PARTITION BY card_type ORDER BY transaction_date,transaction_id) AS rolling_sum
FROM credit_card_transactions)
SELECT *
FROM ( SELECT *,DENSE_RANK() OVER(PARTITION BY card_type ORDER BY rolling_sum) AS ranking
FROM agg_data WHERE rolling_sum>1000000) a
WHERE ranking=1;

--4- Write a query to find city which had lowest percentage spend for gold card type.

WITH agg_data AS (
SELECT city,card_type
,SUM(amount) AS total_amount
,SUM(CASE WHEN card_type='Gold' THEN amount END) AS gold_amount
FROM credit_card_transactions
GROUP BY city,card_type)
SELECT TOP 1 city, SUM(gold_amount)*1.0/SUM(total_amount) AS ratio
FROM agg_data
GROUP BY city
HAVING COUNT(gold_amount)>0
ORDER BY ratio;

/*
5- Write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type 
(example format : Delhi , bills, Fuel)
*/

WITH agg_data AS (
SELECT city,exp_type,SUM(amount) AS amount
FROM credit_card_transactions
GROUP BY city,exp_type),
rank_data AS (
SELECT *
,DENSE_RANK() OVER(PARTITION BY city ORDER BY amount DESC) AS rank1
,DENSE_RANK() OVER(PARTITION BY city ORDER BY amount) AS rank2
FROM agg_data)
SELECT city
,MAX(CASE WHEN rank1=1 THEN exp_type END) AS high_expense_type 
,MIN(CASE WHEN rank2=1 THEN exp_type END) AS lowest_expense_type
FROM rank_data
GROUP BY city;

--6- Write a query to find percentage contribution of spends by females for each expense type.

WITH agg_data AS (
SELECT exp_type
,SUM(CASE WHEN gender='M' THEN amount END) AS females_spend
,SUM(amount) AS total_spend
FROM credit_card_transactions
GROUP BY exp_type)
SELECT *
,100.0*(total_spend-females_spend)/total_spend AS females_spend_percentage
FROM agg_data
ORDER BY females_spend_percentage DESC;

--7- Which card and expense type combination saw highest month over month growth in Jan-2014.

WITH agg_data AS (
SELECT card_type,exp_type,SUM(amount) AS amount,DATEPART(YEAR,transaction_date) AS yr
,DATEPART(MONTH,transaction_date) AS mnth
FROM credit_card_transactions
GROUP BY card_type,exp_type,DATEPART(YEAR,transaction_date),DATEPART(MONTH,transaction_date)),
rolling_data AS (
SELECT *
,LAG(amount,1) OVER(PARTITION BY card_type,exp_type ORDER BY yr,mnth) AS pvs_amount
FROM agg_data)
SELECT TOP 1 *,
(amount-pvs_amount) AS mom_growth
FROM rolling_data
WHERE pvs_amount IS NOT NULL AND yr=2014 AND mnth=1
ORDER BY mom_growth DESC;

--8-- During weekends which city has highest total spend to total no of transcations ratio.
SELECT TOP 1 city,sum(amount)*1.0/count(1) AS spend
FROM credit_card_transactions
WHERE DATENAME(WEEKDAY,transaction_date) IN ('Saturday','Sunday')
GROUP BY city
ORDER BY spend DESC;

/*
9--Which city took least number of days to reach its 500th transaction after the first transaction 
in that city
*/

WITH CTE1 AS (
SELECT *
,ROW_NUMBER() OVER(PARTITION BY city ORDER BY transaction_date,transaction_id) AS ranking
FROM credit_card_transactions)
SELECT TOP 1 city
,DATEDIFF(DAY,MIN(transaction_date),MAX(transaction_date)) AS days_req
FROM CTE1
WHERE ranking IN (1,500)
GROUP BY city
HAVING COUNT(city)=2
ORDER BY days_req;