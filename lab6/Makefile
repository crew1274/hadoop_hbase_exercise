all:
	hadoop com.sun.tools.javac.Main wordCount.java
	jar cf wc.jar wordCount*.class
	hadoop jar wc.jar wordCount /data1 /output

delete:
	hdfs dfs -rm -r /output

clean:
	rm wc.jar

run:
	hdfs dfs -cat /output/part-r-00000

