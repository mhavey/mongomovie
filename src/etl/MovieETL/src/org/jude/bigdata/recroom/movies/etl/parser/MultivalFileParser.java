package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parses a file where movie is mapped to multiple items, one per line. Concrete
 * classes fill in the fine points.
 * 
 * @author user
 * 
 */
public abstract class MultivalFileParser extends ImdbLineParser {

	String lastMovieID = null;
	List<String> lastValues = null;
	String valuesFieldName = null;
	Pattern pattern = null;

	// Most subclasses will use this pattern - movie<whitepace>word
	protected static final String REGEX_MOVIE_WORD = "([^\\t]+)(\\s+)(\\S+)";

	// Some will use this pattern - movie<whitepace>phrase
	protected static final String REGEX_MOVIE_PHRASE = "([^\\t]+)(\\s+)(.+)";

	Logger logger = Logger.getLogger(MultivalFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public MultivalFileParser(Pattern pattern, String valuesFieldName,
			String path, String sourceName, String preHeaderLine,
			String headerLine, String endLine) {
		super(path, sourceName, preHeaderLine, headerLine, endLine);
		this.pattern = pattern;
		this.valuesFieldName = valuesFieldName;
	}

	/**
	 * Implementation of ImdbLineParser parseLine(). Should not need to be
	 * refined by subclasses.
	 */
	@Override
	public BasicDBObject parseLine(String line) throws ETLException {
		// parse next line
		String ret[] = parseOneLine(line);
		String movieID = ret[0];
		String currVal = ret[1];

		// Do I have a previous movie ID?
		if (lastMovieID != null) {
			// continuing that one
			if (movieID.equals(lastMovieID)) {
				lastValues.add(currVal);
			}
			// new movie, so I must return the one I just had
			else {
				BasicDBObject myRet = valuesJSON(lastMovieID, lastValues);
				lastMovieID = movieID;
				lastValues = null;
				lastValues = new ArrayList<String>();
				lastValues.add(currVal);
				return myRet;
			}
		} else {
			lastMovieID = movieID;
			lastValues = null;
			lastValues = new ArrayList<String>();
			lastValues.add(currVal);
		}

		// keep reading lines until I have all the values for this movie
		while (true) {
			// look ahead to next line
			String nextLine = readNextLine(true);

			// if no next line, return what I have
			if (nextLine == null) {
				lastMovieID = null;
				return valuesJSON(movieID, lastValues);
			}

			// parse next line
			String nextRet[] = parseOneLine(nextLine);

			// same movie; update vals and keep looping
			if (nextRet[0].equals(movieID)) {
				lastValues.add(nextRet[1]);
			}

			// different movie; return what I have and keep track of what I
			// found in this line
			else {
				BasicDBObject json = valuesJSON(lastMovieID, lastValues);
				lastMovieID = nextRet[0];
				lastValues = null;
				lastValues = new ArrayList<String>();
				lastValues.add(nextRet[1]);
				return json;
			}
		}
	}

	/**
	 * If I still have a json under build after the file is EOF, return it. And
	 * blank it out.
	 */
	@Override
	public BasicDBObject checkMore(String line) throws ETLException {
		if (lastMovieID == null) {
			return null;
		}
		BasicDBObject json = valuesJSON(lastMovieID, lastValues);
		lastMovieID = null;
		lastValues = null;
		logger.info(this.getClass().getName()
				+ " returning last json in checkMore " + json);
		return json;
	}

	/**
	 * Construct json from movie and values.
	 * Subclasses can override if they need control
	 * 
	 * @param movieID
	 * @param vales
	 * @return
	 */
	public BasicDBObject valuesJSON(String movieID, List<String> values) {
		BasicDBObject json = new BasicDBObject();

		// seems like this is only for movies, so movieID is the key????
		json.append(ETLConstants.FIELD_MOVIEID, movieID);
		json.append(valuesFieldName, values.toArray());
		return json;
	}

	/**
	 * Get one movieID,values from one line in file
	 * 
	 * @param line
	 * @return
	 * @throws ETLException
	 */
	String[] parseOneLine(String line) throws ETLException {
		List<String> toks = getPatternToks(pattern, line.trim());
		if (toks == null || toks.size() != 2) {
			throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
					"Illegal line *" + line + "*");
		}
		String ret[] = { toks.get(0), toks.get(1) };
		return ret;
	}
}
