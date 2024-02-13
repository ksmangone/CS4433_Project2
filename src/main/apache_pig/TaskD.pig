friends = LOAD 'hdfs://localhost:9000/project2/friends.csv' USING PigStorage(',') AS (FriendRel, PersonID, MyFriend, DateOfFriendship, Descr);
friends_clean = FOREACH friends GENERATE FriendRel,PersonID,MyFriend;

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') AS (ID, Name, Nationality, CountryCode, Hobby);
pages_clean = FOREACH pages GENERATE ID,Name;
pages_clean1 = FILTER pages_clean BY Name != 'Name';

friends_group = GROUP friends_clean BY MyFriend;

friends_count = FOREACH friends_group GENERATE group,COUNT(friends_clean) AS followers;

join_output = join pages_clean1 BY ID LEFT OUTER, friends_count BY group;

final_output = FOREACH join_output GENERATE Name AS ownerName,(followers is not null ? followers : 0) AS count;

STORE final_output INTO 'hdfs://localhost:9000/project2/TaskD.csv' USING PigStorage(',');