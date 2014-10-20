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
public class PlotFileParser extends MultilineFileParser {
	static final String SOURCE_NAME = "plot";
	static final String PRE_HEADER_LINE = "PLOT SUMMARIES LIST";
	static final String HEADER_LINE = "===================";
	static final String END_LINE = null;

	Logger logger = Logger.getLogger(PlotFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public PlotFileParser(String path) {
		super(path, ETLConstants.FIELD_MOVIEID, true, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {

		// if line starts with MV:, it's a new movie. Return it.
		if (line.startsWith("MV:")) {
			BasicDBObject ret = new BasicDBObject();
			String movieID = line.substring(3).trim();
			ret.append(this.keyFieldName, movieID);
			ret.append(ETLConstants.FIELD_DOC_TYPE, sourceName);
			// return back but do not force
			return new ParseResult(ret, false);
		}

		else if (line.startsWith("PL:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			String latestPlot = currentJSON
					.getString(ETLConstants.FIELD_DOC_TEXT);
			String latestAuthor = currentJSON
					.getString(ETLConstants.FIELD_DOC_AUTHOR);
			// we'll clear out
			if (latestPlot == null
					|| (latestAuthor != null && latestAuthor.length() > 0)) {
				latestPlot = "";
				currentJSON.remove(ETLConstants.FIELD_DOC_AUTHOR);
			}
			latestPlot += line + " ";
			currentJSON.append(ETLConstants.FIELD_DOC_TEXT, latestPlot);
			return new ParseResult(currentJSON, false);
		} else if (line.startsWith("BY:")) {
			validateKey(currentJSON);
			String author = line.substring(3).trim();
			currentJSON.append(ETLConstants.FIELD_DOC_AUTHOR, author);
			return new ParseResult(currentJSON, true);
		} else {
			throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
					"Illegal line in lineno " + getLineNumber() + " line is *"
							+ line + "*");
		}
	}
}
