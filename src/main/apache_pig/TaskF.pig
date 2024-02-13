friends = LOAD 'hdfs://localhost:9000/project2/friends.csv' USING PigStorage(',') AS (FriendRel, PersonID, MyFriend, DateOfFriendship, Descr);
friends_clean = FOREACH friends GENERATE PersonID,MyFriend;
friends_clean1 = FILTER friends_clean BY PersonID != 'PersonID';

access_logs = LOAD 'hdfs://localhost:9000/project2/access_logs.csv' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, TypeOfAccess, AccessTime);
access_logs_clean = FOREACH access_logs GENERATE ByWho,WhatPage;
access_logs_clean1 = FILTER access_logs_clean BY ByWho != 'ByWho';

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') AS (ID, Name, Nationality, CountryCode, Hobby);
pages_clean = FOREACH pages GENERATE ID,Name;
pages_clean1 = FILTER pages_clean BY Name != 'Name';

join_output = JOIN friends_clean1 BY (PersonID,MyFriend) LEFT OUTER, access_logs_clean1 BY (ByWho,WhatPage);

filter_output = FILTER join_output BY access_logs_clean1::ByWho is null;

distinct_pages = FOREACH (GROUP filter_output BY PersonID) GENERATE group AS PersonID;

join_pages = JOIN distinct_pages BY PersonID, pages_clean1 BY ID;

final_output = FOREACH join_pages GENERATE PersonID,Name;

STORE final_output INTO 'hdfs://localhost:9000/project2/TaskF.csv' USING PigStorage(',');