friends = LOAD 'hdfs://localhost:9000/project2/friends.csv' USING PigStorage(',') AS (
    FriendRel: int,
    PersonID: int,
    MyFriend: int,
    DateofFriendship: int,
    Descr: chararray
);
friends_clean = FILTER friends BY Descr != 'Desc';
friends_clean1 = FOREACH friends_clean GENERATE PersonID,MyFriend;

friends_group = GROUP friends_clean1 BY MyFriend;

friend_counts = FOREACH friends_group GENERATE group AS ID,COUNT(friends_clean1) AS FriendCount;

average_data = GROUP friend_counts ALL;

friends_average = FOREACH average_data GENERATE AVG(friend_counts.FriendCount) AS Average;

popular_people = FILTER friend_counts BY FriendCount > friends_average.Average;

report_data = FOREACH popular_people GENERATE ID;

STORE report_data INTO 'hdfs://localhost:9000/project2/TaskH.csv' USING PigStorage(',');