JAVA_HOME=/usr
ETL_HOME=../../lib

CLASSPATH=$ETL_HOME/imdbetl.jar:$ETL_HOME/log4j-1.2.17.jar:$ETL_HOME/mongo-java-driver-2.11.4.jar

$JAVA_HOME/bin/java -cp $CLASSPATH -Xms256m -Xmx1024m org.jude.bigdata.recroom.movies.etl.ETLController  -props=controller.properties -clean=$1 -jobs=$2 -log4j=log4j.properties


