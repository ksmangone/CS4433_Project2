rmf task3Out
pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') 
         AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
citizensCountByCountry = GROUP pages BY CountryCode;
citizensCountByCountry = FOREACH citizensCountByCountry GENERATE group AS CountryCode, COUNT(pages) AS CitizensCount;
-- This is to get rid of a ghost output at the end of the file that is typically ",0"
result = FILTER citizensCountByCountry by CitizensCount > 0;
STORE result INTO 'hdfs://localhost:9000/project2/TaskC.csv' USING PigStorage(',');