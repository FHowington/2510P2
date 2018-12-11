all: *.java
	javac -classpath /opt/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar *.java -source 1.7 -target 1.7
	jar -cf WC.jar *.class
