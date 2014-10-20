package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;

import com.mongodb.BasicDBObject;

/**
 * Parses the main movie list
 * 
 * @author user
 * 
 */
public class CertificatesFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "certificates";
	static final String PRE_HEADER_LINE = "CERTIFICATES LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = "----------------------";

	protected static final String REGEX = "([^\\t]+)(\\s+)(.+)";

	// Inside Paris (2001) (V) USA:X
	// Inside Passage (2005) (TV) USA:G
	// Inside Peaches (2007) (V) UK:R18
	static Pattern pattern = Pattern.compile(REGEX);

	Logger logger = Logger.getLogger(CertificatesFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public CertificatesFileParser(String path) {
		super(pattern, ETLConstants.FIELD_CERTIFICATES, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	/**
	 * Override
	 */
	public BasicDBObject valuesJSON(String movieID, List<String> values) {
		BasicDBObject json = new BasicDBObject();

		json.append(ETLConstants.FIELD_MOVIEID, movieID);
		json.append(ETLConstants.FIELD_PARENTAL,
				new BasicDBObject().append(valuesFieldName, values.toArray()));
		return json;
	}

}
