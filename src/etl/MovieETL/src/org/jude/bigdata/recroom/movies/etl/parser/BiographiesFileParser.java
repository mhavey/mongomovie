package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.List;
import java.util.regex.Pattern;

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
public class BiographiesFileParser extends MultilineFileParser {
	static final String SOURCE_NAME = "biographies";
	static final String PRE_HEADER_LINE = "BIOGRAPHY LIST";
	static final String HEADER_LINE = "==============";
	static final String END_LINE = null;

	Logger logger = Logger.getLogger(BiographiesFileParser.class);

	static final String YEAR_PATTERN = "(\\d\\d\\d\\d)";
	Pattern yearPattern = Pattern.compile(YEAR_PATTERN);

	// height: feet to metres, or is it the other way around
	static double CONVERSION = 0.39370;

	static final String HEIGHT_PATTERN = "(\\d+(\\.\\d)?)(\\s*cm\\.*)";
	static final String HEIGHT_PATTERN_FTIN = "(\\d+)(\\')(\\s*)((\\d+)(\\s1/2)?(\\\"))?";
	Pattern heightPattern = Pattern.compile(HEIGHT_PATTERN);
	Pattern heightPatternFtin = Pattern.compile(HEIGHT_PATTERN_FTIN);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public BiographiesFileParser(String path) {
		super(path, ETLConstants.FIELD_CONTRIB_NAME, false, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {

		String origLine = line;

		if (line.startsWith("NM:")) {
			// consider it a new contrib
			BasicDBObject newContrib = new BasicDBObject();
			String contribName = line.substring(3).trim();
			newContrib.append(this.keyFieldName, contribName).append(
					ETLConstants.FIELD_PERSON_DATA, new BasicDBObject());
			return new ParseResult(newContrib, false);
		} else if (line.startsWith("DB:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_PERSON_DATA)))
					.append(ETLConstants.FIELD_CONTRIB_BIRTH, line);

			Integer iYear = this.getYear(line);
			if (iYear != null) {
				((BasicDBObject) (currentJSON
						.get(ETLConstants.FIELD_PERSON_DATA))).append(
						ETLConstants.FIELD_CONTRIB_BIRTHYEAR, iYear.toString());
			}

			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("DD:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_PERSON_DATA)))
					.append(ETLConstants.FIELD_CONTRIB_DEATH, line);

			Integer iYear = this.getYear(line);
			if (iYear != null) {
				((BasicDBObject) (currentJSON
						.get(ETLConstants.FIELD_PERSON_DATA))).append(
						ETLConstants.FIELD_CONTRIB_DEATHYEAR, iYear.toString());
			}

			// death cause is difficult - try getting from first bit in
			// parenetheses
			int lastParen = line.indexOf("(");
			if (lastParen >= 0) {
				int lastParenClose = line.indexOf(")", lastParen);
				if (lastParenClose > 0) {
					String cause = line
							.substring(lastParen + 1, lastParenClose);
					if (!cause.startsWith("age ")) {
						((BasicDBObject) (currentJSON
								.get(ETLConstants.FIELD_PERSON_DATA))).append(
								ETLConstants.FIELD_CONTRIB_DEATHCAUSE, cause);
					}
				}
			}

			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("HT:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();

			String height = "";

			// try to get height in cm
			List<String> toks = super.getPatternToks(heightPattern, line);
			if (toks != null && toks.size() > 1) {
				try {
					Double.parseDouble(toks.get(0));
					height = toks.get(0);
				} catch (NumberFormatException e) {
					; // ignore
				}
			} else {

				// try ft and in.
				toks = super.getPatternToks(heightPatternFtin, line);
				if (toks != null) {
					int feet = Integer.parseInt(toks.get(0));
					double cm = feet * 12 / CONVERSION;
					if (toks.size() == 5) {
						cm += ((double) Integer.parseInt(toks.get(3)) / CONVERSION);
					} else if (toks.size() == 6) {
						cm += ((double) Integer.parseInt(toks.get(3)) + 0.5)
								/ CONVERSION;
					} else {
						if (toks.size() != 2) {
							logger.info("BAD INCHES " + toks);
						}
					}
					height = "" + cm;
				}
			}

			if (height.length() > 0) {
				((BasicDBObject) (currentJSON
						.get(ETLConstants.FIELD_PERSON_DATA))).append(
						ETLConstants.FIELD_CONTRIB_HEIGHT, height);

			} else {
				System.out.println(line);
			}

			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("RN:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			((BasicDBObject) (currentJSON.get(ETLConstants.FIELD_PERSON_DATA)))
					.append(ETLConstants.FIELD_CONTRIB_REALNAME, line);
			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("NK:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			addArrayItem(currentJSON, line,
					ETLConstants.FIELD_CONTRIB_NICKNAMES);
			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("SP:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			addArrayItem(currentJSON, line, ETLConstants.FIELD_CONTRIB_SPOUSES);
			return new ParseResult(currentJSON, false);
		} else {
			validateKey(currentJSON);
			addBio(currentJSON, origLine);
			return new ParseResult(currentJSON, false);
		}
	}

	void addArrayItem(BasicDBObject json, String val, String fieldName) {
		String existing[] = (String[]) ((BasicDBObject) (json
				.get(ETLConstants.FIELD_PERSON_DATA))).get(fieldName);
		String newarr[] = null;
		if (existing == null || existing.length == 0) {
			newarr = new String[1];
		} else {
			newarr = new String[existing.length + 1];
			System.arraycopy(existing, 0, newarr, 0, existing.length);
		}
		newarr[newarr.length - 1] = val;
		((BasicDBObject) (json.get(ETLConstants.FIELD_PERSON_DATA)))
				.removeField(fieldName);
		((BasicDBObject) (json.get(ETLConstants.FIELD_PERSON_DATA))).append(
				fieldName, newarr);
	}

	void addBio(BasicDBObject t, String newText) {
		String currText = t.getString(ETLConstants.FIELD_CONTRIB_BIO);
		if (currText == null) {
			currText = "";
		}
		currText += newText + " ";
		t.append(ETLConstants.FIELD_CONTRIB_BIO, currText);
	}

	Integer getYear(String line) {
		List<String> toks = super.getPatternToks(yearPattern, line);
		if (toks != null && toks.size() == 1) {
			return new Integer(toks.get(0));
		}
		return null;
	}
}
