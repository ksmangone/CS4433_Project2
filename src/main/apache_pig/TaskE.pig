access_logs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, TypeOfAccess, AccessTime);
access_logs_clean1 = FOREACH access_logs GENERATE ByWho,WhatPage;
access_logs_clean = FILTER access_logs_clean1 BY ByWho != 'ByWho';

access_logs_group = GROUP access_logs_clean BY ByWho;

final_output = FOREACH access_logs_group {
    distinctA = DISTINCT access_logs_clean.WhatPage;
    GENERATE
        group AS PersonID,
        COUNT(access_logs_clean) AS totalAccesses,
        COUNT(distinctA) AS distinctPages;
}

STORE final_output INTO 'hdfs://localhost:9000/project2/TaskE.csv' USING PigStorage(',');