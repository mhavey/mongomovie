package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parses the ratings list
 * 
 * @author user
 * 
 */
public class RatingsFileParser extends ImdbLineParser {
	static final String SOURCE_NAME = "ratings";
	static final String PRE_HEADER_LINE = "MOVIE RATINGS REPORT";
	static final String HEADER_LINE = "New  Distribution  Votes  Rank  Title";
	static final String END_LINE = "------------------------------";

	// 0000000124 779116 8.9 Pulp Fiction (1994)
	// dist can have * and . in it!!!!
	static final String REGEX = "([\\*0123456789\\.\\*]+)(\\s+)(\\d+)(\\s+)(\\d+\\.\\d)(\\s+)(\\S+.+)";

	Logger logger = Logger.getLogger(RatingsFileParser.class);
	static Pattern pattern = Pattern.compile(REGEX);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public RatingsFileParser(String path) {
		super(path, SOURCE_NAME, PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	public BasicDBObject parseLine(String line) throws ETLException {
		List<String> toks = getPatternToks(pattern, line.trim());
		if (toks == null || toks.size() < 2) {
			throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
					"Illegal line *" + line + "*");
		}

		// Return our record
		BasicDBObject json = new BasicDBObject();
		json.append(ETLConstants.FIELD_MOVIEID, toks.get(3));
		json.append(
				ETLConstants.FIELD_RATING,
				new BasicDBObject()
						.append(ETLConstants.FIELD_RATING_DIST, toks.get(0))
						.append(ETLConstants.FIELD_RATING_F, toks.get(2))
						.append(ETLConstants.FIELD_RATING_VOTES_I, toks.get(1)));
		return json;
	}
}
