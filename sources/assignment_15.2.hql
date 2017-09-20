DROP DATABASE IF EXISTS assignment_15_2 CASCADE;

CREATE DATABASE IF NOT EXISTS assignment_15_2;

USE assignment_15_2;

DROP TABLE IF EXISTS olympics_data;
-- create the olympics_data table
create EXTERNAL table IF NOT EXISTS olympics_data
(
	athlete string,
	age int,
	country string,
	year int,
	closing_date string,
	sport_name string,
	num_gold_medals int,
	num_silver_medals int,
	num_bronze_medals int,
	num_total_medals int
)
row format delimited
fields terminated by '\t'
LOCATION '/user/arvind/hive/acadgild/assignments/assignment_15.2/input/';

DROP TABLE IF EXISTS olympics_data_partitioned;
-- create the olympics_data_partitioned table
create table IF NOT EXISTS olympics_data_partitioned
(
	athlete string,
	age int,
	year int,
	closing_date date,
	sport_name string,
	num_gold_medals int,
	num_silver_medals int,
	num_bronze_medals int,
	num_total_medals int
)
partitioned by (country string);

-- set hive.exec.dynamic.partition.mode as nonstrict to support dynamic partition
SET hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=150;
SET hive.exec.max.dynamic.partitions=300;
-- insert data from olympics_data to olympics_data_partitioned
insert overwrite table olympics_data_partitioned
partition(country)
select athlete, age, year, TO_DATE(from_unixtime(unix_timestamp(closing_date, 'dd-MM-yy'))) AS closing_date, sport_name, num_gold_medals, num_silver_medals, num_bronze_medals, num_total_medals, country from olympics_data;

-- set the number of reduce tasks to 2
SET mapred.reduce.tasks=2;

-- Use CLUSTER BY to control the output that goes to a reducer
-- If GROUP BY and CLUSTER BY is done on the same column, good parallelism can be achieved
-- because your aggregations in a single group will run only in separate reducers

-- in task 1 we need to find number of medals for Swimming for each country
-- so we filter by Swimming in where clause and GROUP BY country
-- store the output in a hdfs directory
INSERT OVERWRITE DIRECTORY '/user/arvind/hive/acadgild/assignments/assignment_15.2/output/task_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select country, SUM(num_total_medals) AS medal_count from olympics_data_partitioned WHERE sport_name='Swimming' GROUP BY country CLUSTER BY country;

-- in task 2 we need to find number of medals won by India per year
-- so we filter by country India in where clause and GROUP BY year
-- store the output in a hdfs directory
INSERT OVERWRITE DIRECTORY '/user/arvind/hive/acadgild/assignments/assignment_15.2/output/task_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select year, SUM(num_total_medals) AS medal_count from olympics_data_partitioned WHERE country='India' GROUP BY year CLUSTER BY year;

-- in task 3 we need to find number of medals won by each country
-- so we GROUP BY country and for each group we sum the column num_total_medals
-- store the output in a hdfs directory
INSERT OVERWRITE DIRECTORY '/user/arvind/hive/acadgild/assignments/assignment_15.2/output/task_3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select country, SUM(num_total_medals) AS medal_count from olympics_data_partitioned GROUP BY country CLUSTER BY country;

-- in task 4 we need to find number of gold medals won by each country
-- so we GROUP BY country and for each group we sum the column num_gold_medals
-- store the output in a hdfs directory
INSERT OVERWRITE DIRECTORY '/user/arvind/hive/acadgild/assignments/assignment_15.2/output/task_4'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select country, SUM(num_gold_medals) AS gold_medal_count from olympics_data_partitioned GROUP BY country CLUSTER BY country;
