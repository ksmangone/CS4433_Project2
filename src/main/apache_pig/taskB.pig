rmf task2Out
accessLogs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',') 
            AS (AccessId: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);
pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') 
            AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
joinedData = JOIN accessLogs BY WhatPage, pages BY PersonID;
pageAccessCount = GROUP joinedData BY pages::PersonID;
pageAccessCount = FOREACH pageAccessCount GENERATE group AS PageID, COUNT(joinedData) AS AccessCount;
rankedPages = RANK pageAccessCount BY AccessCount DESC;
top10Pages = LIMIT rankedPages 10;
top10PagesDetails = JOIN top10Pages BY PageID, pages BY PersonID;
result = FOREACH top10PagesDetails GENERATE pages::PersonID AS PageID, pages::Name, pages::Nationality;
STORE result INTO 'hdfs://localhost:9000/project2/TaskB.csv' USING PigStorage(',');