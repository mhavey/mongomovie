package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;

import com.mongodb.BasicDBObject;


/**
 * Parses the sound mix list
 * 
 * @author user
 * 
 */
public class SoundMixFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "sound-mix";
	static final String PRE_HEADER_LINE = "SOUND-MIX LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = "----------------------";

	static Pattern pattern = Pattern.compile(MultivalFileParser.REGEX_MOVIE_PHRASE);


	//Dick Head (2000)					Stereo
	//Dick Henderson (1926)					De Forest Phonofilm
	//Dick Henderson (1930)					Mono

	Logger logger = Logger.getLogger(SoundMixFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public SoundMixFileParser(String path) {
		super(pattern, ETLConstants.FIELD_SOUNDS, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}
	
	/**
	 * Override
	 */
	public BasicDBObject valuesJSON(String movieID, List<String> values) {
		BasicDBObject json = new BasicDBObject();

		json.append(ETLConstants.FIELD_MOVIEID, movieID);
		json.append(ETLConstants.FIELD_TECHNICAL,
				new BasicDBObject().append(valuesFieldName, values.toArray()));
		return json;
	}
}
