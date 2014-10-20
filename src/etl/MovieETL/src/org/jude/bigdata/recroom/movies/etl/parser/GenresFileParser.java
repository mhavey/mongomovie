package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;

/**
 * Parses the genres list
 * 
 * @author user
 * 
 */
public class GenresFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "genres";
	static final String PRE_HEADER_LINE = "8: THE GENRES LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = null;

	// "'Til Death Do Us Part" (2006) Crime
	// "'Til Death Do Us Part" (2006) Drama
	// "'Til Death Do Us Part" (2006) Fantasy
	// "'Til Death Do Us Part" (2006) Romance

	Logger logger = Logger.getLogger(GenresFileParser.class);
	static Pattern pattern = Pattern
			.compile(MultivalFileParser.REGEX_MOVIE_WORD);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public GenresFileParser(String path) {
		super(pattern, ETLConstants.FIELD_GENRES, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
