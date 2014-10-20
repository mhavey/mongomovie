package org.jude.bigdata.recroom.movies.etl.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import com.mongodb.BasicDBObject;

/**
 * Parses an IMDB file. next() method allows caller to iterate through the file
 * line by line.
 * 
 * @author user
 * 
 */
public abstract class ImdbLineParser {
	String path;
	String sourceName;
	String fileName;

	BasicDBObject lastLine = null;
	String headerLine;
	String preHeaderLine;
	String endLine;
	boolean foundHeaderLine = false;
	boolean isEOF = false;
	long lastLineNumber = 0;

	GZIPInputStream gzis;
	FileInputStream fis;
	InputStreamReader isr;
	LineNumberReader lnr;

	public static final String CSV_DELIMITER = "|";

	Logger logger = Logger.getLogger(ImdbLineParser.class);

	/**
	 * Default constructor
	 */
	public ImdbLineParser() {

	}

	/**
	 * Typical constructor. Would not be used by the caller. Rather, called from
	 * constructor of a concrete subclass.
	 * 
	 * @param preHeaderLine
	 * @param headerLine
	 * @param endLine
	 */
	protected ImdbLineParser(String path, String source, String preHeaderLine,
			String headerLine, String endLine) {
		this.path = path;
		this.sourceName = source;
		this.preHeaderLine = preHeaderLine;
		this.headerLine = headerLine;
		this.endLine = endLine;
	}

	/**
	 * Opens movie file. You should call this after constructing.
	 * 
	 * @throws Throwable
	 */
	public void openReader() throws ETLException {
		isEOF = false;
		this.fileName = this.path + File.separator + sourceName + ".list.gz";
		try {
			fis = new FileInputStream(fileName);
			gzis = new GZIPInputStream(fis);
			isr = new InputStreamReader(gzis);
			lnr = new LineNumberReader(isr);
		} catch (IOException e) {
			try {
				closeReader();
			} catch (ETLException de) {
				ETLException.logError(logger, 
						"Error disconnecting after connection error", de);
			}
			throw new ETLException(ETLConstants.ERR_FILE,
					"Error opening file *" + this.fileName + "*", e);
		}
	}

	/**
	 * Closes movie file. This is done implicitly on EOF.
	 */
	public void closeReader() throws ETLException {
		ETLException savedError = null;
		if (lnr != null) {
			try {
				lnr.close();
				lnr = null;
			} catch (Throwable t) {
				ETLException.logError(logger, ETLConstants.ERR_FILE, "Closing lnr", t);
				savedError = new ETLException(ETLConstants.ERR_FILE,
						"Error closing lnr ", t);
			}
		}
		if (isr != null) {
			try {
				isr.close();
				isr = null;
			} catch (Throwable t) {
				ETLException.logError(logger, ETLConstants.ERR_FILE, "Closing isrs", t);
				savedError = new ETLException(ETLConstants.ERR_FILE,
						"Error closing isr ", t);
			}
		}
		if (gzis != null) {
			try {
				gzis.close();
				gzis = null;
			} catch (Throwable t) {
				ETLException.logError(logger, ETLConstants.ERR_FILE, "Closing gzis", t);
				savedError = new ETLException(ETLConstants.ERR_FILE,
						"Error closing gzis ", t);
			}
		}
		if (fis != null) {
			try {
				fis.close();
				fis = null;
			} catch (Throwable t) {
				ETLException.logError(logger, ETLConstants.ERR_FILE, "Closing fis", t);
				savedError = new ETLException(ETLConstants.ERR_FILE,
						"Error closing fis ", t);
			}
		}
		if (savedError != null) {
			throw savedError;
		}
	}

	/**
	 * Reads next non-blank line from the movie file. Returns null if EOF or if
	 * endline is reached.
	 * 
	 * Can be used by concrete parsers to look ahead to the next line
	 * 
	 * @return
	 * @throws IOException
	 */
	protected String readNextLine(boolean checkEndLine) throws ETLException {
		if (isEOF) {
			return null;
		}

		try {
			while (true) {
				String nextLine = lnr.readLine();
				if (nextLine == null) {
					closeReader();
					isEOF = true;
					return null;
				}
				lastLineNumber = lnr.getLineNumber();
				if (nextLine.trim().length() == 0) {
					continue;
				}
				if (checkEndLine
						&& (endLine != null && nextLine.trim().indexOf(endLine) >= 0)) {
					logger.info("Got end line at " + getLineNumber());
					isEOF = true;
					return null;
				}

				return nextLine;
			}
		} catch (IOException e) {
			throw new ETLException(ETLConstants.ERR_FILE,
					"Error advancing to next line " + e, e);
		}
		// is it an end line?

	}

	/**
	 * Gets line number in file
	 * 
	 * @return
	 */
	protected long getLineNumber() {
		return lnr.getLineNumber();
	}

	/**
	 * How many lines was the file?
	 * 
	 * @return
	 */
	public long getLastLineNumber() {
		return lastLineNumber;
	}

	/**
	 * Skips past lines until it gets to a line with the contained text. Returns
	 * that line. Throws exception if EOF (since we expect to find the line or
	 * else it's an error). Use this to skip past preamble in a movie file.
	 * 
	 * @param line
	 * @return
	 * @throws IOException
	 */
	String huntLine(String containedText) throws ETLException {
		while (true) {
			String nextLine = readNextLine(false);
			if (nextLine == null) {
				throw new ETLException(ETLConstants.ERR_UNEXPECTED_LINE,
						"Unable to find specified line " + containedText);
			}
			if (nextLine.indexOf(containedText) >= 0) {
				return nextLine;
			}
		}
	}

	/**
	 * Moves to header line in the movie file. Sometimes there is a pre-header
	 * line too, although if this is null, pre-header check is skipped. Throws
	 * exception is EOF before header found. After this the next line can be
	 * read. It will be the first line of real data in the file.
	 * 
	 * @throws IOException
	 */
	void skipPremable() throws ETLException {
		// find header line if required
		if (!foundHeaderLine) {
			if (preHeaderLine != null) {
				huntLine(preHeaderLine);
			}
			huntLine(headerLine);
			foundHeaderLine = true;
		}
	}

	/**
	 * The concrete class that extends this returns the next data line as json.
	 * 
	 * @return
	 * @throws IOException
	 */
	public BasicDBObject next() throws ETLException {
		// skip past header if not already done
		skipPremable();

		// get the line
		String nextLine = readNextLine(true);

		// is it an end line?
		if (nextLine == null) {
			return checkMore(nextLine);
		}

		return parseLine(nextLine);
	}

	/**
	 * Parse the line into a record structure. Concrete classes implement this.
	 * 
	 * @param line
	 * @return
	 * @throws IOException
	 */
	protected abstract BasicDBObject parseLine(String line) throws ETLException;

	/**
	 * Check if there still a json to send back after EOF. Default behavior is
	 * no. Concrete classes that use lookahead logic may override.
	 * 
	 * @param line
	 * @return
	 * @throws IOException
	 */
	protected BasicDBObject checkMore(String line) throws ETLException {
		return null;
	}

	/**
	 * Helper used by most concrete classes to parse from regex
	 * 
	 * @param pattern
	 * @param line
	 * @return
	 */
	protected List<String> getPatternToks(Pattern pattern, String line) {
		return getPatternToks(pattern, line, true);
	}

	/**
	 * Helper used by most concrete classes to parse from regex
	 * 
	 * @param pattern
	 * @param line
	 * @return
	 */
	protected List<String> getPatternToks(Pattern pattern, String line,
			boolean removeBlanks) {
		Matcher matcher = pattern.matcher(line);
		if (!matcher.find()) {
			return null;
		}
		List<String> list = new ArrayList<String>();
		int numGroups = matcher.groupCount();
		for (int i = 1; i <= numGroups; i++) {
			if (removeBlanks
					&& (matcher.group(i) == null || matcher.group(i).trim()
							.length() == 0)) {
				continue;
			}
			list.add(matcher.group(i));
		}
		return list;
	}

	/**
	 * Validate sint is a valid int. If so, return it again as a string. Else if
	 * failIfBad, throw exception. Otherwise, return log info and return empty
	 * string.
	 * 
	 * @param sint
	 * @param failIfBad
	 * @return
	 * @throws ETLException
	 */
	protected String validateInt(String sint, boolean failIfBad,
			boolean logIfBad) throws ETLException {
		try {
			Integer.parseInt(sint);
			return sint;
		} catch (NumberFormatException e) {
			if (failIfBad) {
				throw new ETLException(ETLConstants.ERR_CONVERSION,
						"Illegal int *" + sint + "*" + e, e);
			}
			if (logIfBad) {
				logger.debug("Illegal int *" + sint + "* in line "
						+ getLineNumber() + " " + e);
			}
			return "";
		}
	}

	/**
	 * Add field to existing CSV line and return result. ETLUtilities also has
	 * one, but this is quite differently implemented.
	 * 
	 * @param s
	 * @return
	 */
	public String addToCSV(String csv, String field) {
		if (csv == null) {
			csv = "";
		}
		// fix field that has our delimiter
		if (field.contains(CSV_DELIMITER)) {
			logger.info("Replacing delim in *" + field + "* in line "
					+ getLineNumber());
			field = field.replaceAll(CSV_DELIMITER, "").trim();
		}
		// skip if field already in csv
		if (csv.equals(field) || csv.startsWith(field + CSV_DELIMITER)
				|| csv.endsWith(CSV_DELIMITER + field)
				|| csv.contains(CSV_DELIMITER + field + CSV_DELIMITER)) {
			logger.debug("CSV already contains field in line "
					+ getLineNumber() + " csv*" + csv + "* field *" + field
					+ "*");
			return csv;
		}
		if (csv.length() == 0) {
			return field.trim();
		} else {
			return csv + CSV_DELIMITER + field.trim();
		}
	}

	
	/**
	 * Create an ImdbLineParser for the given sourceName. Instantiate it,
	 * passing it the file path in imdbFilePath.
	 */
	public static ImdbLineParser getParser(String sourceName,
			String imdbFilePath) throws ETLException {

		// Determine class name for parser:
		// packageName.<SourceNameInCamel>FileParser
		String toks[] = sourceName.split("-");
		String camelSourceName = "";
		for (int i = 0; i < toks.length; i++) {
			camelSourceName += toks[i].substring(0, 1).toUpperCase()
					+ toks[i].substring(1);
		}

		String pkg = ImdbLineParser.class.getPackage().getName();
		String className = pkg + "." + camelSourceName + "FileParser";

		// Load the class
		try {
			Class c;
			c = Class.forName(className);

			// Create an instance of it, invoking its constructor
			Constructor ctor;
			ctor = c.getConstructor(String.class);
			return (ImdbLineParser) (ctor
					.newInstance(new String[] { imdbFilePath }));
		} catch (ClassNotFoundException e) {
			throw new ETLException(ETLConstants.ERR_FACTORY,
					"Unable to find parser for *" + sourceName + "*", e);
		} catch (SecurityException e) {
			throw new ETLException(ETLConstants.ERR_FACTORY,
					"Security error creating parser for *" + sourceName + "*",
					e);
		} catch (NoSuchMethodException e) {
			throw new ETLException(ETLConstants.ERR_FACTORY,
					"Constructor not found for parser *" + sourceName + "*", e);
		} catch (IllegalArgumentException e) {
			throw new ETLException(ETLConstants.ERR_FACTORY,
					"Illegal argument creating parser *" + sourceName + "*", e);
		} catch (InstantiationException e) {
			throw new ETLException(ETLConstants.ERR_FACTORY,
					"Error creating parser *" + sourceName + "*", e);
		} catch (IllegalAccessException e) {
			throw new ETLException(ETLConstants.ERR_FACTORY,
					"Illegal access  creating parser *" + sourceName + "*", e);
		} catch (InvocationTargetException e) {
			throw new ETLException(ETLConstants.ERR_FACTORY,
					"Invocation target error creating parser *" + sourceName
							+ "*", e);
		}
	}
}
