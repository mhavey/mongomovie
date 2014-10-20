package org.jude.bigdata.recroom.movies.etl;

import java.net.UnknownHostException;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * Provides an interface to connect to a MongoDB and insert/update/find docs.
 * 
 * @author user
 * 
 */
public class MongoDBConnection {

	String host = null;
	int port = 0;
	String dbName;
	MongoClient client = null;
	DB movieDB = null;

	WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

	Logger logger = Logger.getLogger(MongoDBConnection.class);

	/**
	 * Default constructor does nothing,
	 */
	public MongoDBConnection() {

	}

	/**
	 * Gets configuration from specific properties
	 * 
	 * @param props
	 * @throws ETLException
	 */
	public void configure(ETLProperties props) throws ETLException {
		this.host = props.getString(ETLConstants.PROP_MONGOHOST);
		this.port = props.getInt(ETLConstants.PROP_MONGOPORT);
		this.dbName = props.getString(ETLConstants.PROP_MONGODB);
	}

	/**
	 * Connects to MongoDB
	 * 
	 * @param props
	 * @throws ETLException
	 */
	public void connect() throws ETLException {
		// 1. connection to mongo server
		logger.info("Connecting to *" + this.host + "*" + this.port + "*");
		try {
			this.client = new MongoClient(this.host, this.port);
			logger.info("Connected " + this.client.toString());
		} catch (UnknownHostException e) {
			throw new ETLException(ETLConstants.ERR_MONGO, "Error connecting ",
					e);
		}

		// 2. obtain db
		try {
			this.movieDB = this.client.getDB(this.dbName);
		} catch (MongoException e) {
			try {
				this.disconnect();
			} catch (ETLException ee) {
				ETLException.logError(logger,
						"Error disconnecting after connection error", ee);
			}
			throw new ETLException(ETLConstants.ERR_MONGO, "Error getting DB *"
					+ this.dbName + "*", e);
		}

		// 3. some indexes
		try {
			grabCollection(ETLConstants.COLLECTION_MOVIE).ensureIndex(
					new BasicDBObject().append(ETLConstants.FIELD_MOVIEID, 1),
					"MovieIdx", true);
			grabCollection(ETLConstants.COLLECTION_MOVIE).ensureIndex(
					new BasicDBObject().append(ETLConstants.FIELD_SERIESID, 1),
					"SeriesIdx", false);
			grabCollection(ETLConstants.COLLECTION_MOVIEDOC).ensureIndex(
					new BasicDBObject().append(ETLConstants.FIELD_MOVIEID, 1),
					"DocIdx", false);
			grabCollection(ETLConstants.COLLECTION_CONTRIBUTOR).ensureIndex(
					new BasicDBObject().append(ETLConstants.FIELD_CONTRIB_NAME,
							1), "ContribIdx", true);
			grabCollection(ETLConstants.COLLECTION_MOVIEROLE).ensureIndex(
					new BasicDBObject()
							.append(ETLConstants.FIELD_CONTRIB_NAME, 1)
							.append(ETLConstants.FIELD_CONTRIB_CLASS, 1)
							.append(ETLConstants.FIELD_MOVIEID, 1), "RoleIdx",
					true);
		} catch (MongoException e) {
			throw new ETLException(ETLConstants.ERR_MONGO,
					"Error creating indexes ", e);
		}
	}

	/**
	 * Disconnects from MongoDB
	 * 
	 * @throws ETLException
	 */
	public void disconnect() throws ETLException {
		if (this.client != null) {
			try {
				this.client.close();
			} catch (MongoException e) {
				throw new ETLException(ETLConstants.ERR_MONGO,
						"Error disconnecting", e);
			}
		}
	}

	/**
	 * Private method to grab a collection for use in inserting or updating
	 * 
	 * @param collName
	 * @throws ETLException
	 */
	DBCollection grabCollection(String collName) throws ETLException {
		DBCollection coll = null;
		try {
			coll = this.movieDB.getCollection(collName);
		} catch (MongoException e) {
			throw new ETLException(ETLConstants.ERR_MONGO,
					"Error getting collection *" + collName + "*", e);
		}
		if (coll == null) {
			throw new ETLException(ETLConstants.ERR_MONGO, "No collection *"
					+ collName + "*");
		}
		return coll;
	}

	/**
	 * Drop a collection (if cleanup needed)
	 */
	public void dropCollection(String collName) throws ETLException {
		try {
			grabCollection(collName).drop();
		} catch (MongoException e) {
			throw new ETLException(ETLConstants.ERR_MONGO,
					"Error dropping collection *" + collName + "*", e);
		}
	}

	/**
	 * Insert to a collection
	 */
	public void insertToCollection(String collName, BasicDBObject doc,
			String idFieldName) throws ETLException {
		try {

			// use the specified ID; if not specified Mongo will assign one
			if (idFieldName != null) {
				doc.append("_id", getMandatory(doc, idFieldName));
			}

			WriteResult res = grabCollection(collName).insert(doc,
					this.writeConcern);
		} catch (MongoException e) {
			throw new ETLException(ETLConstants.ERR_MONGO,
					"Error inserting doc " + doc + " in *" + collName + "*", e);
		}
	}

	/**
	 * Update a collection
	 */
	public void updateCollection(String collName, BasicDBObject doc,
			String ids[], boolean upsert, boolean updateSubdoc,
			String idFieldToUseIfInsert) throws ETLException {

		boolean multi = false;

		try {

			// if we're inserting, use the specified ID; if not specified Mongo
			// will assign one
			if (upsert && idFieldToUseIfInsert != null) {
				doc.append("_id", getMandatory(doc, idFieldToUseIfInsert));
			}

			// build my where condition as the AND of id[i] = doc.get(id[i] for
			// each specified id.
			BasicDBObject whereObject = new BasicDBObject();
			for (int i = 0; i < ids.length; i++) {
				whereObject.append(ids[i], getMandatory(doc, ids[i]));
				if (!upsert)
					doc.removeField(ids[i]);
			}

			// A most spurious feature. Allows me to update some of the fields
			// in a subdoc without clobbering the existing subdoc. If my doc
			// contains {b, {c1: x1, c2:
			// x2}}. express set doc as {b.c1=x1,b.c2=x2}
			// Really need to generalize it; but this will do for now
			if (updateSubdoc) {
				if (doc.keySet().size() != 1) {
					throw new ETLException(ETLConstants.ERR_RECORD_SEMANTIC,
							"Unexpected subdoc size " + doc);
				}
				String theOneKey = doc.keySet().iterator().next();
				Object osubdoc = doc.get(theOneKey);
				if (!(osubdoc instanceof BasicDBObject)) {
					throw new ETLException(ETLConstants.ERR_RECORD_SEMANTIC,
							"Unexpected subdoc type " + osubdoc);
				}
				BasicDBObject subdoc = (BasicDBObject) osubdoc;
				Iterator<String> subkeys = subdoc.keySet().iterator();
				BasicDBObject doc2 = new BasicDBObject();
				while (subkeys.hasNext()) {
					String subkey = subkeys.next();
					Object subval = subdoc.get(subkey);
					doc2.append(theOneKey + "." + subkey, subval);
				}

				doc = doc2;
			}

			// the "Set" part of the update.
			BasicDBObject setObject = new BasicDBObject().append("$set", doc);
			logger.debug("Using where " + whereObject + " and set " + setObject);

			// ok, do the update
			WriteResult res = grabCollection(collName).update(whereObject,
					setObject, upsert, multi, this.writeConcern);
		} catch (MongoException e) {
			throw new ETLException(ETLConstants.ERR_MONGO,
					"Error updating doc " + doc + " in *" + collName + "*", e);
		}
	}

	/**
	 * Update a collection
	 */
	public DBObject checkExists(String collName, BasicDBObject basis,
			String ids[], boolean faultIfNotFound) throws ETLException {
		try {

			BasicDBObject whereObject = new BasicDBObject();
			for (int i = 0; i < ids.length; i++) {
				whereObject.append(ids[i], getMandatory(basis, ids[i]));
			}

			DBObject ret = grabCollection(collName).findOne(whereObject);
			if (ret == null) {
				if (faultIfNotFound) {
					throw new ETLException(ETLConstants.ERR_RECORD_NOT_FOUND,
							"In coll " + collName + " could not find " + basis);
				} else {
					return ret;
				}
			} else {
				return ret;
			}
		} catch (MongoException e) {
			throw new ETLException(ETLConstants.ERR_MONGO,
					"Error searching doc " + basis + " in *" + collName + "*",
					e);
		}
	}

	/**
	 * Convenience method to obtain string value from JSON. If not there,
	 * exception
	 * 
	 * @param json
	 * @param fieldName
	 * @return
	 * @throws ETLException
	 */
	public static String getMandatoryString(BasicDBObject json, String fieldName)
			throws ETLException {
		return (String) getMandatory(json, fieldName);
	}

	public static Object getMandatory(BasicDBObject json, String fieldName)
			throws ETLException {
		Object s = json.get(fieldName);
		if (s == null) {
			throw new ETLException(ETLConstants.ERR_FIELD_NOT_FOUND,
					"Mandatory field *" + fieldName + "* not found in JSON "
							+ json);
		}
		return s;
	}
}
