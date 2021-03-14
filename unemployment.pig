/*Preparation*/
sh ls Unempl*.csv
fs -mkdir /unemploymentAnalysis
fs -ls /
fs -copyFromLocal Unemployment_in_India.csv /unemploymentAnalysis
fs -ls /unemploymentAnalysis

/*Loading Dataset*/
unemployment = load '/unemploymentAnalysis/Unemployment_in_India.csv' using PigStorage(',') as (region : chararray, date:chararray, est_unemp_rate :double, est_emp_pop :int, est_labour_part_rate :double, area: chararray);
describe unemployment
dump unemployment

/*Query1: Display all the regions (along with their counts) to which observations of this dataset belong.*/
group_region = group unemployment by region;
describe group_region
unemployment1 = foreach group_region generate group,COUNT(unemployment.region);
dump unemployment1

/*Query2: Display all the information from the unemployment.csv file in descending order of Estimated unemployment rate(%) and store it in unemployment directory.*/
unemployment2 = order unemployment by est_unemp_rate desc;
dump unemployment2
store unemployment2 into '/unemploymentAnalysis/order1' using PigStorage(',');

/*Query3: Display all the tuples having region name starts 'A' and has estimated employed population greater than 1000000.*/
unemployment3 = filter unemployment by STARTSWITH(region,'A') and est_emp_pop >1000000;
dump unemployment3

/*Query4: Display all the tuples having Estimated Unemployment Rate(%) greater than 5.0 and belong to urban area.*/
unemployment4 = filter unemployment by est_unemp_rate >5.0 and area =='Urban';
dump unemployment4

/*Query5a: Display the region having maximum number of estimated employed population.*/
group_all = group unemployment all;
unemployment5 = foreach group_all generate MAX(unemployment.est_emp_pop);
dump unemployment5
unemployment6 = filter unemployment by est_emp_pop == 59433759;
unemployment7 = foreach unemployment6 generate region;
dump unemployment7

/*Query5b: Display the region having minimum number of estimated employed population*/
unemployment8 = foreach group_all generate MIN(unemployment.est_emp_pop);
unemployment9 = filter unemployment by est_emp_pop == 49420;
unemployment10 = foreach unemployment9 generate region;
dump unemployment10

/*Query6a: Display the region having maximum number of estimated labour participation rate(%).*/
unemployment11 = foreach group_all generate MAX(unemployment.est_labour_part_rate);
dump unemployment11
unemployment12 = filter unemployment by est_labour_part_rate==72.57;
unemployment13 = foreach unemployment12 generate region;
dump unemployment13

/*Query6b: Display the region having minimum number of estimated labour participation rate(%)*/

unemployment14 = foreach group_all generate MIN(unemployment.est_labour_part_rate);
dump unemployment14
unemployment15 = filter unemployment by est_labour_part_rate==13.33;
unemployment16 = foreach unemployment15 generate region;
dump unemployment16

/*Query7: Order the relation in descending order of estimated labour participation rate(%)*/
unemployment17 = order unemployment by est_labour_part_rate desc;
dump unemployment17

/*Query8: Display region name, estimated labour participation rate(%) and area of tuples whose estimated unemployment rate(%) is greater than 15%.*/
unemployment18 = filter unemployment by est_unemp_rate > 15.0;
unemployment19 = foreach unemployment18 generate region,est_labour_part_rate,area;
dump unemployment19

/*Converting date column to actual DateTime.*/
unemployment = foreach unemployment generate region, date, est_unemp_rate, est_emp_pop, est_labour_part_rate, area, ToDate(date, 'dd-MM-yyyy') as (dt:DateTime);
dump unemployment

/*Storing updated dataset in /unemploymentAnalysis directory of HDFS.*/
store unemployment into '/unemploymentAnalysis/updated_Unemployment_in_India.csv' using PigStorage(',');

/*Query9: Display the number of observations recorded for each month.*/
group_month = group unemployment by GetMonth(dt);
describe group_month
unemployment20 = foreach group_month generate group,COUNT(unemployment.region);
dump unemployment20

/*Query10: Order the relation by estimated unemployment rate(%) in the month of feb 2020.*/
unemployment21 = filter unemployment by GetMonth(dt)==2 and GetYear(dt)==2020;
describe unemployment21
order_est_unemp_rate = order unemployment by est_unemp_rate;
dump order_est_unemp_rate

/*Query11: What is the average estimated unemployment rate (%) and average estimated employed for rural and urban.*/
group_area = group unemployment by area;
unemployment22 = foreach group_area generate group, AVG(unemployment.est_unemp_rate),AVG(unemployment.est_emp_pop);
dump unemployment22

/*Query12: Rank the relation with respet to estimated employed in the year of 2020.*/
unemployment23 = filter unemployment by GetYear(dt)==2020;
describe unemployment23
unemployment24 = rank unemployment23 by est_emp_pop desc dense;
dump unemployment24

/*Query13: What is the average estimated unemployment rate(%) and average estimated labour participation rate(%) for each year.*/
group_year = group unemployment by GetYear(dt);
describe group_year
unemployment25 = foreach group_year generate group,AVG(unemployment.est_unemp_rate);
dump unemployment25

/*Query14: Generate 2% sample of this dataset and display and store it.*/
unemployment26 = sample unemployment 0.02;
dump unemployment26

/*Query15: Rank the relation by decreasing estimated unemployment rate(%) and increasing region.*/
unemployment27 = rank unemployment by est_unemp_rate desc,region dense;
dump unemployment27

/*Query16: Split this dataset into 3 relations, one containing data with estimated unemployment rate(%) < 5.0, another with estimated unemployment rate(%) >= 5.0 and estimated unemployment rate(%) < 10.0, remaining in the third relation. Store all the files in the directory /unemploymentAnalysis of HDFS.*/
unemployment28 = filter unemployment by est_unemp_rate < 5.0 ;
unemployment29 = filter unemployment by est_unemp_rate >= 5.0 and est_unemp_rate < 10.0;
unemployment30 = filter unemployment by est_unemp_rate > 10.0;
store unemployment28 into '/unemploymentAnalysis/split1' using PigStorage(',');
store unemployment29 into '/unemploymentAnalysis/split2' using PigStorage(',');
store unemployment30 into '/unemploymentAnalysis/split3' using PigStorage(',');

/*Query17: Display the region and estimated unemployment rate of records in which region name starts with ‘M’ in the month of july.*/
unemployment31 = filter unemployment by STARTSWITH(region,'M') and GetMonth(dt)==7;
describe unemployment31
unemployment32 = foreach unemployment31 generate region,est_unemp_rate;
dump unemployment32

/*Query18: Split the relation in two halves, one with records belonging to rural area and other with records belonging to urban area. Also store these relations in the directory /unemploymentAnalysis of HDFS.*/
rural = filter unemployment by area=='Rural';
urban = filter unemployment by area=='Urban';
store rural into '/unemploymentAnalysis/rural' using PigStorage(',');
store urban into '/unemploymentAnalysis/urban' using PigStorage(',');
fs -ls /unemploymentAnalysis

/*Query19: Rank the relation containing records which belong to rural area, with respect to decreasing estimated unemployment rate.*/
unemployment33 = rank rural by est_unemp_rate desc dense;
dump unemployment33

/*Query20: Display all the regions whose estimated employed population is in the range of 8000000 and 10000000 which belongs to urban area.*/
unemployment34 = filter urban by est_emp_pop>8000000 and est_emp_pop<10000000;
dump unemployment34

/*Query21a: Display the month having maximum estimated unemployment rate in 2020 in urban area.*/
urban_group_all = group urban all;
describe urban_group_all
max_est_unemp_rate = foreach urban_group_all generate MAX(urban.est_unemp_rate);
dump max_est_unemp_rate
unemployment35 = filter urban by est_unemp_rate==76.74;
unemployment36 = foreach unemployment35 generate GetMonth(dt),est_unemp_rate;
dump unemployment36

/*Query21b: Display the month having minimum estimated unemployment rate in 2020 in urban area.*/
describe urban_group_all
min_est_unemp_rate = foreach urban_group_all generate MIN(urban.est_unemp_rate);
dump min_est_unemp_rate
unemployment37 = filter urban by est_unemp_rate==0.0;
unemployment38 = foreach unemployment37 generate GetMonth(dt),est_unemp_rate;
dump unemployment38

/*Query22: Order the relation alphabetically with respect to region names. Also the records belong to rural area.*/
unemployment39 = order rural by region;
dump unemployment39

/*Query23: Rank the rural relation with respect to decreasing estimated employed population.*/
unemployment40 = rank rural by est_emp_pop desc dense;
dump unemployment40
