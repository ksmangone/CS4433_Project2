access_logs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',') AS (
    AccessId: int,
    ByWho: int,
    WhatPage: int,
    TypeOfAccess: chararray,
    AccessTime: chararray
);
filtered_access_logs = FILTER access_logs BY TypeOfAccess != 'TypeOfAccess';
filtered_access_logs1 = FOREACH filtered_access_logs GENERATE ByWho AS PersonID,AccessTime;

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') AS (
    ID: int,
    Name: chararray,
    Nationality: chararray,
    CountryCode: int,
    Hobby: chararray
);
filtered_pages = FILTER pages BY Name != 'Name';
filtered_pages1 = FOREACH filtered_pages GENERATE ID AS ID,Name;

logs_group = GROUP filtered_access_logs1 BY PersonID;

latest_access_time = FOREACH logs_group GENERATE group AS PersonID,ToDate(MAX(filtered_access_logs1.AccessTime),'yyyy-MM-dd HH:mm:ss') AS LatestAccessTime:datetime;

disconnected_people = FILTER latest_access_time BY ABS(DaysBetween(LatestAccessTime,CurrentTime())) > (long)14;

disconnected_people_filter = FOREACH disconnected_people GENERATE PersonID AS PersonID;

people = JOIN disconnected_people_filter BY PersonID, filtered_pages1 BY ID;

output_people = FOREACH people GENERATE disconnected_people_filter::PersonID, filtered_pages1::Name;

STORE output_people INTO 'hdfs://localhost:9000/project2/TaskG.csv' USING PigStorage(',');