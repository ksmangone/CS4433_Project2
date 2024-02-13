rmf task1Out
pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') 
         AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
cleanNationality = FILTER pages by Nationality == 'Nigeria';
selectName = FOREACH cleanNationality GENERATE Name, Hobby;
STORE selectName INTO 'hdfs://localhost:9000/project2/TaskA.csv' USING PigStorage(',');