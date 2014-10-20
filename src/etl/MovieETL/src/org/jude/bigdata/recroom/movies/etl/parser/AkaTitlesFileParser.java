package org.jude.bigdata.recroom.movies.etl.parser;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;

/**
 * Parses the Aka-Titles list
 * 
 * @author user
 * 
 */
public class AkaTitlesFileParser extends AkaFileParser {
	static final String SOURCE_NAME = "aka-titles";
	static final String PRE_HEADER_LINE = "AKA TITLES LIST";
	static final String HEADER_LINE = "===============";
	static final String END_LINE = "-------------------------";

	Logger logger = Logger.getLogger(AkaTitlesFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public AkaTitlesFileParser(String path) {
		super(path, ETLConstants.FIELD_MOVIEID, ETLConstants.FIELD_ALTTITLES,
				SOURCE_NAME, PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
