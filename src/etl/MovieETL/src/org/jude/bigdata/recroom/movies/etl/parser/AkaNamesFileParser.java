package org.jude.bigdata.recroom.movies.etl.parser;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;

import com.mongodb.BasicDBObject;

/**
 * Parses the Aka-Names list
 * 
 * @author user
 * 
 */
public class AkaNamesFileParser extends AkaFileParser {
	static final String SOURCE_NAME = "aka-names";
	static final String PRE_HEADER_LINE = "AKA NAMES LIST";
	static final String HEADER_LINE = "==============";
	static final String END_LINE = null;

	Logger logger = Logger.getLogger(AkaNamesFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public AkaNamesFileParser(String path) {
		super(path, ETLConstants.FIELD_CONTRIB_NAME,
				ETLConstants.FIELD_CONTRIB_ALIASES, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
