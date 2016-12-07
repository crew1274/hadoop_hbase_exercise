#
# Makefile
# hduser, 2016-11-26 22:18
#

all:
	hadoop com.sun.tools.javac.Main wordCount.java
	jar cf wc.jar wordCount*.class

delete: 
	hdfs dfs -rm -r /output

run:
	hadoop jar wc.jar wordCount /data /output
	hdfs dfs -cat /output/part-r-00000

clean:
	rm wc.jar
