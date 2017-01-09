all:
	hadoop com.sun.tools.javac.Main Final.java
	jar -cvf final.jar Final*.class

delete:
	rm -rf *.jar

run:
	hadoop jar ./final.jar Final /final 
