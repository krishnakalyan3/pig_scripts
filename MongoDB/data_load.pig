REGISTER /home/cloudera/JAR/mongo-2.10.1.jar
REGISTER /home/cloudera/JAR/mongo-hadoop-core_cdh4.3.0-1.2.0.jar
REGISTER /home/cloudera/JAR/mongo-hadoop-pig_cdh4.3.0-1.2.0.jar

-- Credentials Of Mongodb
A = LOAD 'mongodb://<IP>/twitter.tweets' using com.mongodb.hadoop.pig.MongoLoader();
-- Transfer Data From MongoDB to HDFS
-- This is a mapreduce task
STORE A INTO 'hdfs://<IP>:8020/user/cloudera/mongodb/out' USING PigStorage('\t');
 


