package org.jude.bigdata.recroom.movies.etl.test;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLException;
import org.jude.bigdata.recroom.movies.etl.ETLProperties;
import org.jude.bigdata.recroom.movies.etl.MongoDBConnection;

import com.mongodb.BasicDBObject;

public class MongoDBConnectionTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws ETLException {

		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.DEBUG);

		ETLProperties props = new ETLProperties();
		props.loadProperties("C:\\mike\\tibco\\2\\self_train\\packt\\release\\bin\\etl\\controllertest.properties");
		MongoDBConnection conn = new MongoDBConnection();
		conn.configure(props);
		conn.connect();
		BasicDBObject doc = new BasicDBObject().append("name", "MongoDB4")
				.append("type", "database").append("count", 1)
				.append("info", new BasicDBObject("x", 203).append("y", 102));

		if (1 == 2) {
			System.out.println("\n1\n");
			conn.insertToCollection("test", doc, "name");

			System.out.println("\n2\n");
			conn.updateCollection(
					"test",
					new BasicDBObject().append("name", "MongoDB4").append("z",
							1), new String[] { "name" }, false, false, null);

			System.out.println("\n3\n");
			conn.insertToCollection("test", doc, "name"); // should fail?
		} else if (1 == 3) {
			System.out.println("Test 0 "
					+ conn.checkExists("test",
							new BasicDBObject().append("name", "MongoDB4"),
							new String[] { "name" }, false));

			System.out.println("Test 1 "
					+ conn.checkExists("test",
							new BasicDBObject().append("name", "Mongie"),
							new String[] { "name" }, false));

			System.out.println("Test 2 "
					+ conn.checkExists("test",
							new BasicDBObject().append("name", "Mongie"),
							new String[] { "name" }, true));
		} else if (1 == 4) {

			conn.insertToCollection("utst", new BasicDBObject().append("a", 1)
					.append("c", 3), "a");
			System.out.println(conn.checkExists("utst",
					new BasicDBObject().append("a", 1), new String[] { "a" },
					false));

			conn.updateCollection("utst", new BasicDBObject().append("a", 1)
					.append("b.b1", 2), new String[] { "a" }, false, false,
					null);
			System.out.println(conn.checkExists("utst",
					new BasicDBObject().append("a", 1), new String[] { "a" },
					false));

			conn.updateCollection("utst", new BasicDBObject().append("a", 1)
					.append("d", new BasicDBObject().append("d1", 42)),
					new String[] { "a" }, false, false, null);
			System.out.println(conn.checkExists("utst",
					new BasicDBObject().append("a", 1), new String[] { "a" },
					false));

			conn.updateCollection("utst", new BasicDBObject().append("a", 1)
					.append("d", new BasicDBObject().append("d2", 6)),
					new String[] { "a" }, false, true, null);
			System.out.println(conn.checkExists("utst",
					new BasicDBObject().append("a", 1), new String[] { "a" },
					false));

			conn.updateCollection("utst", new BasicDBObject().append("a", 1)
					.append("b.b2", 3), new String[] { "a" }, false, false,
					null);
			// expect to see {a:1, c:3, b: {b1:2, b2:3}}
			System.out.println(conn.checkExists("utst",
					new BasicDBObject().append("a", 1), new String[] { "a" },
					false));
		} else {
			conn.updateCollection("xtst", new BasicDBObject().append("a", 1)
					.append("b", 2), new String[] { "a" }, true, false, "a");
			conn.updateCollection("xtst", new BasicDBObject().append("a", 2)
					.append("b", 3), new String[] { "a" }, true, false, null);

		}
	}

}
