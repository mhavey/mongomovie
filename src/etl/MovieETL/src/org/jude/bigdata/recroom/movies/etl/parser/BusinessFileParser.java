package org.jude.bigdata.recroom.movies.etl.parser;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parses the Business list
 * 
 * @author user
 * 
 */
public class BusinessFileParser extends MultilineFileParser {
	static final String SOURCE_NAME = "business";
	static final String PRE_HEADER_LINE = "ftp.sunet.se  in  /pub/tv+movies/imdb/tools/w32/";
	static final String HEADER_LINE = "=============";
	static final String END_LINE = "                                    =====";

	static final String BTUSD_PREFIX = "BT: USD";
	static final int BTUSD_PREFIX_LEN = BTUSD_PREFIX.length();
	static final String GRUSD_PREFIX = "GR: USD";
	static final int GRUSD_PREFIX_LEN = GRUSD_PREFIX.length();

	/*
	 * MV: Fictional Title, A (1996)
	 * 
	 * BT: $43,000,000 (USA)
	 * 
	 * OW: $5,400,000 (USA) (3 March 1996) (450 screens)
	 * 
	 * GR: $15,340,000 (USA) (10 March 1996) GR: $35,405,000 (Non-USA) (10 March
	 * 1996) GR: $50,745,000 (Worldwide) (10 March 1996)
	 * 
	 * RT: $25,130,000
	 * 
	 * AD: 330,150 (USA) AD: 21,000 (UK)
	 * 
	 * PD: 21 December 1995 - 7 February 1996
	 * 
	 * ST: Shepperton Studios, Shepperton (UK) ST: Cinecitta', Rome (Italy)
	 * 
	 * CP: Foobar Productions, Inc. CP: 1234 Wilshire Blvd. CP: 90210 Beverly
	 * Hills, CA, U.S.A. CP: Phone: 301-555-1234
	 */

	Logger logger = Logger.getLogger(BusinessFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public BusinessFileParser(String path) {
		super(path, ETLConstants.FIELD_MOVIEID, false, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {

		// if line starts with MV:, it's a new movie. Return it.
		if (line.startsWith("MV:")) {
			BasicDBObject ret = new BasicDBObject();
			ret.append(this.keyFieldName, line.substring(3).trim());
			return new ParseResult(ret, false);
		}

		// BT (budget) - handle USD only
		else if (line.startsWith(BTUSD_PREFIX)) {
			line = line.substring(BTUSD_PREFIX_LEN).trim();
			line = line.replaceAll(",", ""); // get rid of commas, such as
			// 12,000,000
			if (currentJSON.get(ETLConstants.FIELD_BUSINESS) == null) {
				currentJSON.append(ETLConstants.FIELD_BUSINESS, new BasicDBObject());
			}

			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_BUSINESS)))
					.append(ETLConstants.FIELD_BUDGET_I, line);
			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_BUSINESS)))
					.append(ETLConstants.FIELD_BUDGETCURRENCY, "USD");
			return new ParseResult(currentJSON, false);
		}
		// GR (gross box office) - handle USD only
		else if (line.startsWith(GRUSD_PREFIX)
				&& currentJSON.getString(ETLConstants.FIELD_HIGHGBO_I) == null) {
			line = line.substring(GRUSD_PREFIX_LEN).trim();

			String toks[] = line.split("\\(");
			String value = toks[0].trim().replaceAll(",", ""); // get rid of
			// commas, such
			// as
			String country = "";
			if (toks.length > 1) {
				String toks2[] = toks[1].split("\\)");
				country = toks2[0];
			}
			// 12,000,000
			if (currentJSON.get(ETLConstants.FIELD_BUSINESS) == null) {
				currentJSON.append(ETLConstants.FIELD_BUSINESS, new BasicDBObject());
			}
			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_BUSINESS)))
					.append(ETLConstants.FIELD_HIGHGBO_I, value);
			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_BUSINESS)))
					.append(ETLConstants.FIELD_HIGHGBOCOUNTRY, country);
			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_BUSINESS)))
					.append(ETLConstants.FIELD_HIGHGBOCURRENCY, "USD");

			// won't bother with date, as it's a pain to parse and frequently
			// omitted
			// currentTuple.putString(ETLConstants.FIELD_HIGHGBODATE_D, "");
			return new ParseResult(currentJSON, false);
		} else {
			return new ParseResult(currentJSON, false);
		}
	}
}
