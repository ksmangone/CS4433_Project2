friends = LOAD 'hdfs://localhost:9000/project2/friends.csv' USING PigStorage(',') AS (
    FriendRel: int,
    PersonID: int,
    MyFriend: int,
    DateofFriendship: int,
    Descr: chararray
);

friend_counts = FOREACH (GROUP friends BY PersonID) GENERATE group AS PersonID, COUNT(friends) AS FriendCount;

avg_friend_counts = FOREACH (GROUP friend_counts ALL) GENERATE AVG(friend_counts.FriendCount) AS avgFriendCount;

pages = LOAD 'hdfs://localhost:9000/project2/pages.csv' USING PigStorage(',') AS (
    PersonID: int,
    Name: chararray,
    Nationality: chararray,
    CountryCode: int,
    Hobby: chararray
);

joined_data = JOIN pages BY PersonID, friend_counts BY PersonID;

popular_people = FILTER friend_counts BY FriendCount > avg_friend_counts.avgFriendCount;

report_data = FOREACH popular_people GENERATE pages::PersonID;

STORE report_data INTO 'hdfs://localhost:9000/project2/TaskH.csv' USING PigStorage(',');