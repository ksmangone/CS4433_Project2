pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',')
         AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
pages_clean1 = FOREACH pages GENERATE PersonID, Nationality;
pages_clean = FILTER pages_clean1 BY Nationality != 'Nationality';

citizensCountByCountry = GROUP pages_clean BY Nationality;
citizensCountByCountry = FOREACH citizensCountByCountry GENERATE group AS Nationality, COUNT(pages_clean) AS CitizensCount;

STORE citizensCountByCountry INTO 'hdfs://localhost:9000/project2/TaskC.csv' USING PigStorage(',');