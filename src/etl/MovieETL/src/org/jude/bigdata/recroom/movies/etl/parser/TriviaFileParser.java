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
public class TriviaFileParser extends MultilineFileParser {
	static final String SOURCE_NAME = "trivia";
	static final String PRE_HEADER_LINE = "FILM TRIVIA";
	static final String HEADER_LINE = "===========";
	static final String END_LINE = null;

	Logger logger = Logger.getLogger(TriviaFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public TriviaFileParser(String path) {
		super(path, ETLConstants.FIELD_MOVIEID, false, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {

		// This is the movie line
		if (line.startsWith("#")) {
			// consider it a new movie
			BasicDBObject ret = new BasicDBObject();
			ret.append(this.keyFieldName, line.substring(1).trim());
			ret.append(ETLConstants.FIELD_DOC_TYPE, sourceName);
			return new ParseResult(ret, false);
		} else {
			line = line.trim();
			String existingText = currentJSON
					.getString(ETLConstants.FIELD_DOC_TEXT);
			if (existingText == null) {
				existingText = "";
			}
			existingText += line;
			currentJSON.append(ETLConstants.FIELD_DOC_TEXT, existingText);
			return new ParseResult(currentJSON, false);
		}
	}
}
