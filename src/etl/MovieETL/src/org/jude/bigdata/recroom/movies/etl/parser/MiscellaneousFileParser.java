package org.jude.bigdata.recroom.movies.etl.parser;

import org.apache.log4j.Logger;

/**
 * Parses the Aka-Names list
 * 
 * @author user
 * 
 */
public class MiscellaneousFileParser extends RoleFileParser {
	static final String SOURCE_NAME = "miscellaneous";
	static final String PRE_HEADER_LINE = "Name                    Titles";
	static final String HEADER_LINE = "----                    ------";
	static final String END_LINE = "---------------------";

	Logger logger = Logger.getLogger(MiscellaneousFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public MiscellaneousFileParser(String path) {
		super(path, SOURCE_NAME, PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
