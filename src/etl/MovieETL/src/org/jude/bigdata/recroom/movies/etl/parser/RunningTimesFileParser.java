package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parses the running times list
 * 
 * @author user
 * 
 */
public class RunningTimesFileParser extends ImdbLineParser {
	static final String SOURCE_NAME = "running-times";
	static final String PRE_HEADER_LINE = "RUNNING TIMES LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = "------------------------------";

	// Órbitas (2013) 8
	// Órói (2010) 93 (DVD)
	// Óscar Rosmano (1964) Portugal:20
	static final String REGEX = "([^\\t]+)(\\s+)(\\S+.*)";

	Logger logger = Logger.getLogger(RunningTimesFileParser.class);
	static Pattern pattern = Pattern.compile(REGEX);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public RunningTimesFileParser(String path) {
		super(path, SOURCE_NAME, PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	public BasicDBObject parseLine(String line) throws ETLException {
		List<String> toks = getPatternToks(pattern, line.trim());
		if (toks == null || toks.size() != 2) {
			throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
					"Illegal line *" + line + "*");
		}

		String movieID = toks.get(0);
		String runningTime = "";

		// Running is complicated. Split by space or : and look for a single one
		// that is just an int
		String rtoks[] = toks.get(1).split("\\s|:");
		for (int i = 0; i < rtoks.length; i++) {
			String candidate = this.validateInt(rtoks[i], false, false);
			if (candidate.equals(rtoks[i])) {
				runningTime = candidate;
				break;
			}
		}

		// Too numerous is this error to log.
		if (runningTime.length() == 0) {
			logger.debug("Unable to obtain running time for movie in line "
					+ getLineNumber() + " Movie is *" + movieID + "*");
		}

		// Return our record
		BasicDBObject json = new BasicDBObject();
		json.append(ETLConstants.FIELD_MOVIEID, movieID);
		json.append(ETLConstants.FIELD_RUNNING_TIME_I, runningTime);
		return json;
	}
}
