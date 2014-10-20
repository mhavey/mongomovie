setlocal

title controller
set JAVA_HOME=C:\tibco\soa_2014_03\tibcojre64\1.7.0
set ETL_HOME=..\..\lib

set CLASSPATH=%ETL_HOME%\imdbetl.jar;%ETL_HOME%\log4j-1.2.17.jar;%ETL_HOME%\mongo-java-driver-2.11.4.jar

"%JAVA_HOME%\bin\java" -Xms256m -Xmx1024m org.jude.bigdata.recroom.movies.etl.ETLController  -props=controller.properties  -clean=none -jobs=all -log4j=log4j.properties

pause

endlocal