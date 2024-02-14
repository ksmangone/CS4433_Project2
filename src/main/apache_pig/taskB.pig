accessLogs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',')
            AS (AccessId: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);
accessLogs_clean = FOREACH accessLogs GENERATE ByWho, WhatPage;

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') 
            AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
pages_clean = FOREACH pages GENERATE PersonID, Name, Nationality;

accessLogs_group = GROUP accessLogs_clean BY WhatPage;

accessLogs_counts = FOREACH accessLogs_group GENERATE group AS PageID,COUNT(accessLogs_clean) AS AccessCount;

rankedPages = ORDER accessLogs_counts BY AccessCount DESC;

top10Pages = LIMIT rankedPages 10;

joinedData = JOIN top10Pages BY PageID, pages_clean BY PersonID;

result = FOREACH joinedData GENERATE PageID,Name,Nationality;

STORE result INTO 'hdfs://localhost:9000/project2/TaskB.csv' USING PigStorage(',');