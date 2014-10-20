package org.jude.bigdata.recroom.movies.etl;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.parser.ImdbLineParser;

import com.mongodb.BasicDBObject;

/**
 * Encapsulates next() loop to read from IMDB line parser. Key method is
 * nextJSON(). Class will cleanup implicitly - no need for caller to do it
 * 
 * @author user
 * 
 */
public class ImdbIterator {
	int numIter = 0;
	int numReject = 0;
	int numFail = 0;
	ImdbLineParser parser = null;
	boolean eof = false;
	String source;
	Logger logger = Logger.getLogger(ImdbIterator.class);
	Logger rejectLogger = null;
	BasicDBObject currentJSON;

	public static final int PROGRESS_AT = 100000;

	/**
	 * Constructor opens the parser for the given soruce name
	 * 
	 * @param source
	 * @param path
	 * @throws ETLException
	 */
	public ImdbIterator(String source, String path) throws ETLException {
		this.source = source;
		rejectLogger = Logger.getLogger("reject." + source);
		parser = ImdbLineParser.getParser(source, path);
		parser.openReader();
	}

	/**
	 * Get next json. If null, it means either EOF or an error in the json.
	 * Check isEOF() to determine which.
	 * 
	 * @return
	 */
	public BasicDBObject nextJSON() {
		try {
			BasicDBObject t = parser.next();
			if (t == null) {
				eof = true;
				parser.closeReader();
				return null;
			}
			numIter++;

			// progress tick
			if ((numIter % PROGRESS_AT) == 0) {
				logger.info(this.source + " " + numIter);
			}
			currentJSON = t;
			return currentJSON;
		} catch (ETLException x) {
			// This error is extremely rare. Has never occurred for me.s
			ETLException.logError(logger, "Error getting next json", x);
			numFail++;
			return null;
		}
	}

	/**
	 * Is EOF on file?
	 * 
	 * @return
	 */
	public boolean isEOF() {
		return eof;
	}

	public int getNumIter() {
		return numIter;
	}

	public int getNumFail() {
		return numFail;
	}

	public int getNumReject() {
		return numReject;
	}

	/**
	 * If I get a valid JSON but decide it's not good, I can mark it as
	 * rejected.
	 */
	public void logReject(ETLException e) {
		numReject++;
		String debugMsg = "REJECT|" + source + "|" + e + "|json is "
				+ currentJSON;
		rejectLogger.error(debugMsg);
	}

	public void logResult() {
		logger.info("Iteration of *" + source + "* iter " + numIter + " fail "
				+ numFail + " reject " + numReject);
	}
}
