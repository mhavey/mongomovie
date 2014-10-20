package org.jude.bigdata.recroom.movies.etl.parser;

import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parse a file where the contents of the record are split over multiple lines.
 * Two modes: forceMode=true - Concrete class tells when to return back. Used
 * when there are muliple jsons per key, and thus only the concrete class knows
 * when to generate jsons. forceMode=false - This class decides when to return
 * back based on change of key
 * 
 * @author user
 * 
 */
public abstract class MultilineFileParser extends ImdbLineParser {

	/**
	 * Return value for concrete parse
	 * 
	 * @author user
	 * 
	 */
	public static class ParseResult {
		BasicDBObject json = null;
		BasicDBObject newCurrentJSON = null;
		boolean returnNow = false;

		public ParseResult(BasicDBObject json, boolean returnNow) {
			super();
			this.json = json;
			this.returnNow = returnNow;
		}

		/**
		 * Return the new json now and set current json to the one specified
		 * 
		 */
		public ParseResult(BasicDBObject json, BasicDBObject newCurrentJSON) {
			super();
			this.json = json;
			this.newCurrentJSON = newCurrentJSON;
			this.returnNow = true; // always true when we do this
		}

		public BasicDBObject getJSON() {
			return this.json;
		}

		public boolean isReturnNow() {
			return returnNow;
		}

		public BasicDBObject getNewCurrentJSON() {
			return this.newCurrentJSON;
		}
	}

	String lastKey = null;
	BasicDBObject lastJSON = null;
	String keyFieldName = null;
	boolean forceMode = false;

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public MultilineFileParser(String path, String keyFieldName,
			boolean forceMode, String sourceName, String preHeaderLine,
			String headerLine, String endLine) {
		super(path, sourceName, preHeaderLine, headerLine, endLine);
		this.keyFieldName = keyFieldName;
		this.forceMode = forceMode;
	}

	/**
	 * Concrete class implements this. Takes line and currentJSON. Concrete
	 * class parses line and returns a ParseResult. Outcomes: (a) no forceMode;
	 * will return new result only if change in key; (b) forceMode enabled and
	 * ParseResult.returnNode=true; will return back now; (c) forceMode enabled
	 * and ParseResult.returnMode=false; will continue until we have outcome (b)
	 * 
	 * @param line
	 * @return
	 * @throws ETLException
	 */
	protected abstract ParseResult parseOneLine(String line,
			BasicDBObject currentJSON) throws ETLException;

	/**
	 * Implementation of ImdbLineParser parseLine(). Should not need to be
	 * refined by subclasses.
	 */
	@Override
	public BasicDBObject parseLine(String line) throws ETLException {

		String savedLine = line;
		while (true) {

			// skip to first content line
			String nextLine = skipToContent(savedLine);
			savedLine = null;

			// if no next line, return what I have
			if (nextLine == null) {
				logger.info(this.getClass().getName()
						+ " returning last JSON in parseLine() after EOF "
						+ lastJSON);
				lastKey = null;
				return lastJSON;
			}

			// ask subclass to parse line and give me next json
			ParseResult result = parseOneLine(nextLine, lastJSON);
			if (result.getJSON() == null) {
				logger.info(this.getClass().getName()
						+ " returning last json in parseLine() after parseOneLine() returned null "
						+ lastJSON);
				lastKey = null;
				return lastJSON;
			}
			BasicDBObject nextJSON = result.getJSON();
			String nextKey = keyField(nextJSON);

			if (forceMode) {
				// If the concrete class wants me to use a different current
				// json, do so
				if (result.getNewCurrentJSON() != null) {
					lastJSON = result.getNewCurrentJSON();
					lastKey = keyField(lastJSON);
				} else {
					lastKey = nextKey;
					lastJSON = nextJSON;
				}
				if (result.isReturnNow()) {
					return nextJSON;
				} else {
					// keep going
				}
			} else {
				// Do I have a previous key?
				if (lastKey != null) {
					// continuing that one
					if (nextKey.equals(lastKey)) {
						lastJSON = nextJSON;
					}
					// new key, so I must return the one I just had
					else {
						lastKey = nextKey;
						BasicDBObject ret = lastJSON;
						lastJSON = nextJSON;
						return ret;
					}
				} else {
					// no previous key, so initialize our last values
					lastKey = nextKey;
					lastJSON = nextJSON;
				}
			}
		}
	}

	/**
	 * If I still have a JSON after the file is EOF, return it. And blank it
	 * out.
	 */
	@Override
	public BasicDBObject checkMore(String line) throws ETLException {
		if (lastKey == null) {
			return null;
		}
		logger.info(this.getClass().getName()
				+ " returning last jason in checkMore " + lastJSON);
		lastKey = null;
		return lastJSON;
	}

	/**
	 * Allows concrete classes using forced mode to validate that the key in
	 * currentJSON matches lastKey
	 * 
	 * @param currentJSON
	 */
	protected void validateKey(BasicDBObject currentJSON) throws ETLException {
		if (lastKey == null || currentJSON.getString(keyFieldName) == null
				|| !lastKey.equals(currentJSON.getString(keyFieldName))) {
			throw new ETLException(ETLConstants.ERR_UNEXPECTED_LINE,
					"Error in lineno " + getLineNumber() + " compare key *"
							+ lastKey + "*" + currentJSON);
		}
	}

	/**
	 * Get key field from json
	 * 
	 * @param j
	 * @return
	 * @throws ETLException
	 */
	String keyField(BasicDBObject j) throws ETLException {
		String k = j.getString(keyFieldName);
		if (k == null) {
			throw new ETLException(ETLConstants.ERR_FIELD_NOT_FOUND,
					"Key not found in " + j + " for line " + getLineNumber());
		}
		return k;
	}

	/**
	 * Read lines from the file until I get a valid content line (i.e., not a
	 * blank line, not a line with ----, and not EOF. If I reach EOF, return
	 * null. If input "line" is not null, begin with it. Otherwise, begin by
	 * reading next line from file.
	 * 
	 * @param line
	 * @return
	 * @throws ETLException
	 */
	String skipToContent(String line) throws ETLException {

		String nextLine = line;
		while (true) {
			if (nextLine == null) {
				nextLine = readNextLine(true);
			}
			if (nextLine == null) {
				return null;
			} else if (nextLine.trim().length() == 0
					|| nextLine.trim().startsWith("---------")) {
				nextLine = null;
				continue;
			}
			return nextLine;
		}
	}
}
