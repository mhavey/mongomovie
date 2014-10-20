package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;


/**
 * Parses the languages list
 * 
 * @author user
 * 
 */
public class LanguageFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "language";
	static final String PRE_HEADER_LINE = "LANGUAGE LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = "----------------------";

	// Kililheun cheolsae (1967) Korean
	// Kilimanjaro (2000) Korean
	// Kilimanjaro (2012) Spanish
	// Kilimanjaro (2013/I) English
	static Pattern pattern = Pattern
			.compile(MultivalFileParser.REGEX_MOVIE_WORD);

	Logger logger = Logger.getLogger(LanguageFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public LanguageFileParser(String path) {
		super(pattern, ETLConstants.FIELD_LANGUAGES, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
