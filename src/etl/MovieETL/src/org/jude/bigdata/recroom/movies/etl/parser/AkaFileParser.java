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
public class AkaFileParser extends MultilineFileParser {
	static final String AKA_PREFIX = "   (aka ";
	static final int AKA_PREFIX_LEN = AKA_PREFIX.length();

	Logger logger = Logger.getLogger(AkaFileParser.class);
	String valuesFieldName = null;

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public AkaFileParser(String path, String keyFieldName,
			String valuesFieldName, String sourceName, String preHeaderLine,
			String headerLine, String endLine) {
		super(path, keyFieldName, true, sourceName, preHeaderLine, headerLine,
				endLine);
		this.valuesFieldName = valuesFieldName;
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {

		// This is an AKA line
		if (line.startsWith(AKA_PREFIX)) {
			line = line.substring(AKA_PREFIX_LEN).trim();
			int firstTab = line.indexOf("\t");
			String thisAka = line;
			if (firstTab == 0) {
				throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
						"Illegal entry in lineno " + getLineNumber()
								+ " line is *" + line + "*");
			} else if (firstTab > 0) {
				thisAka = line.substring(0, firstTab - 1);
			} else {
				thisAka = line.substring(0, line.length() - 1);
				// nothing to do. no tab, so thisAka is just the line
			}
			String existingAka[] = (String[]) (currentJSON.get(valuesFieldName));
			validateKey(currentJSON);
			if (thisAka.equals("...") || thisAka.startsWith("?")) {
				thisAka = "";
			}
			if (thisAka.contains("...")) {
				thisAka = thisAka.replaceAll("\\.\\.\\.", " ").trim();
			}

			if (thisAka.length() > 0) {
				if (existingAka == null) {
					existingAka = new String[1];
					existingAka[0] = thisAka;
				} else {
					String newAka[] = new String[existingAka.length + 1];
					System.arraycopy(existingAka, 0, newAka, 0,
							existingAka.length);
					newAka[newAka.length - 1] = thisAka;
					existingAka = newAka;
				}

				setAkaValue(currentJSON, existingAka);
			}
			return new ParseResult(currentJSON, false);
		} else {
			// consider it a new movie - skip if it has garbage characters in it
			BasicDBObject newMovie = new BasicDBObject();
			newMovie.append(this.keyFieldName, line.trim());
			if (currentJSON != null && checkContainsAKA(currentJSON)) {
				return new ParseResult(currentJSON, newMovie);
			}
			if (currentJSON != null) {
				logger.info("Abandoning " + currentJSON);
			}
			return new ParseResult(newMovie, false);
		}
	}
	
	/**
	 * Public method allows subclasses to override where to place the AKA array in the JSON
	 * @param currentJSON
	 * @param aka
	 */
	public void setAkaValue(BasicDBObject currentJSON, String aka[]) {
		currentJSON.append(valuesFieldName, aka);
	}
	
	/**
	 * Public method allows subclass to override check of whether JSON contains AKS
	 * @param currentJSON
	 * @return
	 */
	public boolean checkContainsAKA(BasicDBObject currentJSON) {
		return currentJSON.containsKey(valuesFieldName);
	}
}
