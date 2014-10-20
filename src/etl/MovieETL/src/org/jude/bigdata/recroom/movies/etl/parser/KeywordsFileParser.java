package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;


/**
 * Parses the keywords list
 * 
 * @author user
 * 
 */
public class KeywordsFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "keywords";
	static final String PRE_HEADER_LINE = "8: THE KEYWORDS LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = null;

	// Butterfly Rising (2010) religious
	// Butterfly Rising (2010) road-trip
	// Butterfly Rising (2010) spiritual
	// Butterfly on a Wheel (2007) abductor
	// Butterfly on a Wheel (2007) anxiety

	Logger logger = Logger.getLogger(KeywordsFileParser.class);
	static Pattern pattern = Pattern
			.compile(MultivalFileParser.REGEX_MOVIE_WORD);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public KeywordsFileParser(String path) {
		super(pattern, ETLConstants.FIELD_KEYWORDS, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
