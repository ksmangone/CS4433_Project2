access_logs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',') AS (
    AccessId: int,
    ByWho: int,
    WhatPage: int,
    TypeOfAccess: chararray,
    AccessTime: chararray
);

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') AS (
    PersonID: int,
    Name: chararray,
    Nationality: chararray,
    CountryCode: int,
    Hobby: chararray
);


filtered_access_logs = FOREACH access_logs GENERATE ByWho AS PersonID, AccessTime;

filtered_access_logs = FILTER filtered_access_logs BY PersonID > -1;

unix_access_logs = FOREACH filtered_access_logs GENERATE PersonID, ABS(DaysBetween(ToDate(AccessTime, 'yyyy-MM-dd HH:mm:ss'), ToDate('2023-09-04 13:44:58', 'yyyy-MM-dd HH:mm:ss'))) as DaysSince:long;

disconnected_people = FILTER unix_access_logs BY DaysSince > 14;
connected_people  = FILTER unix_access_logs BY DaysSince < 14;

disconnected_people = FOREACH disconnected_people GENERATE PersonID;
connected_people = FOREACH connected_people GENERATE PersonID;

joined_data = JOIN disconnected_people BY PersonID LEFT OUTER, connected_people BY PersonID;

sub_res = FILTER joined_data BY connected_people::PersonID IS NULL;

filtered_pages = FOREACH pages GENERATE PersonID, Name;

result = JOIN sub_res BY disconnected_people::PersonID LEFT OUTER, filtered_pages BY PersonID;

result = FOREACH result GENERATE filtered_pages::PersonID, filtered_pages::Name;

result = group result By filtered_pages::PersonID;

deduplicated_data = FOREACH result {
    unique_tuple = LIMIT result 1;
    GENERATE FLATTEN(unique_tuple);
}

STORE deduplicated_data INTO 'hdfs://localhost:9000/project2/TaskG.csv' USING PigStorage(',');