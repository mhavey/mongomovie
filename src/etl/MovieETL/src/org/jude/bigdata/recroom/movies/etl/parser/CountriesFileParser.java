package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;


/**
 * Parses the countries list
 * 
 * @author user
 * 
 */
public class CountriesFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "countries";
	static final String PRE_HEADER_LINE = "COUNTRIES LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = "----------------------";

	// ÏMaten a ese hijo de la chingada! (2008) (V) Mexico
	// Ìslenska, on the Road to Unearth Iceland's Secrets... (2007) USA static
	// final String REGEX = "([^\\t]+)(\\s+)(\\S+)";

	Logger logger = Logger.getLogger(CountriesFileParser.class);
	static Pattern pattern = Pattern
			.compile(MultivalFileParser.REGEX_MOVIE_PHRASE);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public CountriesFileParser(String path) {
		super(pattern, ETLConstants.FIELD_COUNTRIES, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
