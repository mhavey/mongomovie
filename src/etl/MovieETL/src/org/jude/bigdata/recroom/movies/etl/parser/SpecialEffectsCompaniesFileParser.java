package org.jude.bigdata.recroom.movies.etl.parser;

import org.apache.log4j.Logger;

/**
 * Parses the Aka-Names list
 * 
 * @author user
 * 
 */
public class SpecialEffectsCompaniesFileParser extends CompanyRoleFileParser {
	static final String SOURCE_NAME = "special-effects-companies";
	static final String PRE_HEADER_LINE = "SPECIAL EFFECTS COMPANIES LIST";
	static final String HEADER_LINE = "==============================";
	static final String END_LINE = "---------------------";

	Logger logger = Logger.getLogger(SpecialEffectsCompaniesFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public SpecialEffectsCompaniesFileParser(String path) {
		super(path, SOURCE_NAME, PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
}
