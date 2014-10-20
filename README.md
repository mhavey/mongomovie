mongomovie
==========
The following describes how to use the MongoDB movie code. 

A) To view the ETL code:
In Eclipse, import the project in the src/etl/MovieETL directory.

B) To run the ETL job on a UNIX platform:
(i) In the bin/etl directory, modify controller.properties. Change the MongoHost and MongoPort properties to point to your MongoDB instance.
(ii) In the bin/etl directory, modify controller.sh. Change JAVA_HOME to point to the location of the JRE on your machine.
(iii) In a shell, cd to the bin/etl directory and run the following:
./controller_m.sh
./controller_d.sh
./controller_r.sh
./controller_c.sh
(iv) Check controller.log and rejects.log for errors. Some rejects are to be expected. No errors should show in controller.log.

You can run the ETL from any machine that has network connectivity to the MongoDB instance. But we recommend you run it on the same machine as the instance. 
Copy the release directory (including bin, src, and lib directories) to that machine.

C) To run the queries on a UNIX platform:
(i) Copy the release directory (including bin, src, and lib directories) to the machine hosting your MongoDB instance.
(ii) Modify the bin/queries/movieshell.sh file. Change MONGO_HOME to point to your MongodDB home directory. 
(iiI) In a shell, cd to the bin/queries directory and run the following:
./movieshell.sh
