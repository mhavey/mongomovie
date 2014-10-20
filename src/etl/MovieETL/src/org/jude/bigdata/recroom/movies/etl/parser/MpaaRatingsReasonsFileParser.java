package org.jude.bigdata.recroom.movies.etl.parser;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parses the MPAA Ratings Reasons list
 * 
 * @author user
 * 
 */
public class MpaaRatingsReasonsFileParser extends MultilineFileParser {
	static final String SOURCE_NAME = "mpaa-ratings-reasons";
	static final String PRE_HEADER_LINE = "MPAA RATINGS REASONS LIST";
	static final String HEADER_LINE = "-------------------------------------------";
	static final String END_LINE = null;

	/*
	 * --------------------------------------------------------------------------
	 * ----- MV: "The Hunger" (1997) RE: Rated R for strong sexual content,
	 * violence and language RE: (Creature Comforts compilation)
	 * 
	 * 
	 * 
	 * 
	 * 
	 * --------------------------------------------------------------------------
	 * ----- MV: "The Langoliers" (1995) RE: Rated PG-13 for violence and some
	 * sci-fi terror
	 * 
	 * 
	 * 
	 * 
	 * 
	 * --------------------------------------------------------------------------
	 * -----
	 */

	Logger logger = Logger.getLogger(MpaaRatingsReasonsFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public MpaaRatingsReasonsFileParser(String path) {
		super(path, ETLConstants.FIELD_MOVIEID, false, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {
		// if line starts with MV:, it's a new movie. Return it.
		if (line.startsWith("MV:")) {
			BasicDBObject ret = new BasicDBObject();
			ret.append(this.keyFieldName, line.substring(3).trim()).append(
					ETLConstants.FIELD_PARENTAL,
					new BasicDBObject().append(
							ETLConstants.FIELD_MPAA_RATING_REASON, ""));
			return new ParseResult(ret, false);
		}

		else if (line.startsWith("RE:")) {
			String currentRating = ((BasicDBObject) (currentJSON
					.get(ETLConstants.FIELD_PARENTAL)))
					.getString(ETLConstants.FIELD_MPAA_RATING_REASON);
			if (currentRating == null) {
				currentRating = "";
			}
			currentRating += " " + line.substring(3).trim();
			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_PARENTAL)))
					.put(ETLConstants.FIELD_MPAA_RATING_REASON, currentRating);
			return new ParseResult(currentJSON, false);
		}

		else {
			throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
					"Unexpected line at lineno " + getLineNumber() + " line *"
							+ line + "*");
		}

	}
}
