package org.jude.bigdata.recroom.movies.etl.parser;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parses the Aka-Titles list
 * 
 * @author user
 * 
 */
public class LiteratureFileParser extends MultilineFileParser {
	static final String SOURCE_NAME = "literature";
	static final String PRE_HEADER_LINE = "LITERATURE LIST";
	static final String HEADER_LINE = "===============";
	static final String END_LINE = null;

	static final String MOVIE_PREFIX = "MOVI:";
	static final int MOVIE_PREFIX_LEN = MOVIE_PREFIX.length();

	/*
	 * MOVI: Üç maymun (2008)
	 * 
	 * CRIT: Gentele, Jeanette. "De tre aporna (6/6)". In: "Svenska Dagbladet"
	 * (Sweden), 18 September 2009, (NP) CRIT: Kois, Dan.
	 * "Turkish Import 'Three Monkeys': Noir and Light". In:
	 * "The Washington Post" (USA), Vol. 132, Iss. 84, 27 February 2009, Pg. c4,
	 * (NP) CRIT: Romney, Jonathan.
	 * "Jailed for money, but the sentence is for life". In:
	 * "The Independent on Sunday" (UK), Independent News & Media Limited, Vol.
	 * 990, 15 February 2009, Pg. 56, (WNP)
	 */

	Logger logger = Logger.getLogger(LiteratureFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public LiteratureFileParser(String path) {
		super(path, ETLConstants.FIELD_MOVIEID, false, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {

		// This is an AKA line
		if (line.startsWith(MOVIE_PREFIX)) {
			// consider it a new movie
			String movieID = line.substring(MOVIE_PREFIX_LEN).trim();
			BasicDBObject ret = new BasicDBObject();
			ret.append(this.keyFieldName, movieID);
			ret.append(ETLConstants.FIELD_DOC_TYPE, sourceName);
			return new ParseResult(ret, false);
		} else {
			line = line.trim();
			String existingText = currentJSON
					.getString(ETLConstants.FIELD_DOC_TEXT);
			if (existingText == null) {
				existingText = "";
			}
			existingText += line;
			currentJSON.append(ETLConstants.FIELD_DOC_TEXT, existingText);
			return new ParseResult(currentJSON, false);
		}
	}
}
