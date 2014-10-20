package org.jude.bigdata.recroom.movies.etl.parser;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parses the Aka-Titles list
 * 
 * @author user
 * 
 */
public class GoofsFileParser extends MultilineFileParser {
	static final String SOURCE_NAME = "goofs";
	static final String PRE_HEADER_LINE = "GOOFS LIST";
	static final String HEADER_LINE = "==========";
	static final String END_LINE = null;

	Logger logger = Logger.getLogger(GoofsFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public GoofsFileParser(String path) {
		super(path, ETLConstants.FIELD_MOVIEID, true, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {

		// This is the movie line
		if (line.startsWith("#")) {
			// consider it a new movie
			BasicDBObject newGoof = new BasicDBObject();
			String movieID = line.substring(1).trim();
			newGoof.append(this.keyFieldName, movieID);
			newGoof.append(ETLConstants.FIELD_DOC_TYPE, sourceName);

			// did I have a previous goof? flush it
			if (currentJSON != null) {
				return new ParseResult(currentJSON, newGoof);
			}
			return new ParseResult(newGoof, false);
		} else if (line.startsWith("-")) {
			validateKey(currentJSON);

			// start building this goof
			line = line.substring(1).trim();
			int colonIdx = line.indexOf(":");
			if (colonIdx != 4) {
				throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
						"Malformed line in lineno " + getLineNumber() + "*"
								+ line + "*");
			}
			String goofType = line.substring(0, 4);
			String docText = line.substring(5).trim();
			BasicDBObject newJSON = new BasicDBObject();
			newJSON.append(keyFieldName, currentJSON.getString(keyFieldName));
			newJSON.append(ETLConstants.FIELD_DOC_TYPE,
					currentJSON.getString(ETLConstants.FIELD_DOC_TYPE));
			newJSON.append(ETLConstants.FIELD_DOC_SUBTYPE, goofType);
			newJSON.append(ETLConstants.FIELD_DOC_TEXT, docText);

			// did I have a previous goof with a subtype already defined? flush
			// it
			if (currentJSON.getString(ETLConstants.FIELD_DOC_SUBTYPE) != null) {
				return new ParseResult(currentJSON, newJSON);
			}
			return new ParseResult(newJSON, false);
		} else {
			validateKey(currentJSON);

			// validate doc fields are there
			if (currentJSON.getString(ETLConstants.FIELD_DOC_SUBTYPE) == null
					|| currentJSON.getString(ETLConstants.FIELD_DOC_TEXT) == null) {
				throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
						"Error in lineno " + getLineNumber()
								+ " no doc fields *" + currentJSON);
			}

			// just keep going, appending doc text
			String docText = line.trim();
			String existingDocText = currentJSON
					.getString(ETLConstants.FIELD_DOC_TEXT);
			currentJSON.append(ETLConstants.FIELD_DOC_TEXT, existingDocText
					+ " " + docText);
			return new ParseResult(currentJSON, false);
		}
	}
}
