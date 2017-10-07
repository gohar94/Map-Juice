# This is for making a directory on HDFS and putting some data into it
hadoop fs -mkdir -p /user/hadoop
hadoop dfs -copyFromLocal /home/hadoop/data /user/hadoop