# This is for running mapreduce wordcount on the hadoop cluster
hadoop jar /home/hadoop/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar wordcount /user/hadoop/data /user/hadoop/output
hadoop dfs -getmerge /user/hadoop/output/ /home/hadoop/output/