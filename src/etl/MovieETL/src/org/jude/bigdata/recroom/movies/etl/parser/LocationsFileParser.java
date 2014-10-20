package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;


/**
 * Parses the locations list
 * 
 * @author user
 * 
 */
public class LocationsFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "locations";
	static final String PRE_HEADER_LINE = "LOCATIONS LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = "----------------------";

	static Pattern pattern = Pattern
			.compile(MultivalFileParser.REGEX_MOVIE_PHRASE);

	// Child 44 (2014) Prague, Czech Republic
	// Child 44 (2014) Ostrava, Czech Republic
	// Child Abuse (1976) (TV) Ambassador Hotel - 3400 Wilshire Boulevard, Los
	// Angeles, California, USA

	Logger logger = Logger.getLogger(LocationsFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public LocationsFileParser(String path) {
		super(pattern, ETLConstants.FIELD_LOCATIONS, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
