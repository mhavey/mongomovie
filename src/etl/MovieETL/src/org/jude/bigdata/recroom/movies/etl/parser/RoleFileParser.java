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
public class RoleFileParser extends MultilineFileParser {

	Logger logger = Logger.getLogger(RoleFileParser.class);

	String prevMovieID = null;

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public RoleFileParser(String path, String sourceName, String preHeaderLine,
			String headerLine, String endLine) {
		super(path, ETLConstants.FIELD_CONTRIB_NAME, true, sourceName,
				preHeaderLine, headerLine, endLine);
	}

	@Override
	protected ParseResult parseOneLine(String line, BasicDBObject currentJSON)
			throws ETLException {

		// This is a just a role line
		if (line.startsWith("\t")) {
			line = line.substring(1).trim();
			validateKey(currentJSON);

			// build new role
			BasicDBObject newRole = buildRole(line,
					currentJSON.getString(ETLConstants.FIELD_CONTRIB_NAME));
			prevMovieID = newRole.getString(ETLConstants.FIELD_MOVIEID);

			String currMovieID = currentJSON
					.getString(ETLConstants.FIELD_MOVIEID);
			if (!currMovieID.equals(prevMovieID)) {
				// if change in movie, flush the old one
				return new ParseResult(currentJSON, newRole);
			} else {
				// append role desc to current
				newRole.append(
						ETLConstants.FIELD_CONTRIB_ROLE,
						addToCSV(
								currentJSON
										.getString(ETLConstants.FIELD_CONTRIB_ROLE),
								newRole.getString(ETLConstants.FIELD_CONTRIB_ROLE)));
				newRole.append(
						ETLConstants.FIELD_CONTRIB_ROLEDETAIL,
						addToCSV(
								currentJSON
										.getString(ETLConstants.FIELD_CONTRIB_ROLEDETAIL),
								newRole.getString(ETLConstants.FIELD_CONTRIB_ROLEDETAIL)));
				return new ParseResult(newRole, false);
			}

		} else {
			// consider it a new contributor
			line = line.trim();
			int firstTab = line.indexOf("\t");
			if (firstTab <= 0) {
				throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
						"Illegal line in lineno " + getLineNumber() + " line *"
								+ line + "*");
			}
			BasicDBObject newRole = buildRole(line.substring(firstTab).trim(),
					line.substring(0, firstTab).trim());
			prevMovieID = newRole.getString(ETLConstants.FIELD_MOVIEID);
			if (currentJSON != null) {
				// flush the old one
				return new ParseResult(currentJSON, newRole);
			}

			// otherwise, return our new one, but hold until we conclude it
			return new ParseResult(newRole, false);
		}
	}

	// Just movie: Madre nana (1991)
	// Movie plus credit <>: Ulan, init at hamog (1987) <8>
	// Movie plus role []: All Americana (2009) [Joy]
	// Movie plus role [] amd credit <>: Ang tanging ina (2003) [Jenny] <33>
	// Weird because "(voice)": Argentine, les 500 bébés volés de la dictature
	// (2013) (TV) (voice) [Narrator]
	BasicDBObject buildRole(String roleDesc, String contrib)
			throws ETLException {

		BasicDBObject ret = new BasicDBObject();

		// My theory is that two spaces splits fields
		String toks[] = roleDesc.split("  ");
		if (toks == null || toks.length == 0) {
			throw new ETLException(ETLConstants.ERR_UNEXPECTED_LINE,
					"Unexpected movie role in lineno " + getLineNumber() + " *"
							+ roleDesc + "*");
		}
		String movieID = toks[0].trim();
		String role = "";
		String roleDetail = "";
		for (int i = 1; i < toks.length; i++) {
			toks[i] = toks[i].trim();
			if (toks[i].startsWith("[") && toks[i].endsWith("]")) {
				role = toks[i].substring(1, toks[i].length() - 1);
			} else if (toks[i].startsWith("(") && toks[i].endsWith(")")) {
				roleDetail += toks[i].substring(1, toks[i].length() - 1) + " ";
			} else if (toks[i].startsWith("<") && toks[i].endsWith(">")) {
				roleDetail += toks[i].substring(1, toks[i].length() - 1) + " ";
			} else {
				roleDetail += toks[i];

			}
		}

		ret.append(this.keyFieldName, contrib);
		ret.append(ETLConstants.FIELD_CONTRIB_CLASS, this.sourceName);
		ret.append(ETLConstants.FIELD_MOVIEID, movieID);
		ret.append(ETLConstants.FIELD_CONTRIB_ROLE, addToCSV(null, role));
		ret.append(ETLConstants.FIELD_CONTRIB_ROLEDETAIL,
				addToCSV(null, roleDetail));

		return ret;
	}
}
