#
# Makefile
# alikuotw, 2016-11-30 16:52
#

all:
	javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.3.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar repeat.java
	jar cf repeat.jar repeat*.class

run:
	hadoop jar repeat.jar repeat /data2 /output2
	hdfs dfs -cat /output2/part-r-00000

clean:
	rm repeat.jar


delete:
	hdfs dfs -rm -r /output2

# vim:ft=make
#
