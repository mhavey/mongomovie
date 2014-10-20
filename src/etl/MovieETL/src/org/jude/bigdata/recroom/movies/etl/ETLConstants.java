package org.jude.bigdata.recroom.movies.etl;

public class ETLConstants {
	public final static String FIELD_MOVIEID = "MovieID";
	public final static String FIELD_SOURCE = "Source";
	public final static String FIELD_SERIESID = "SeriesID";
	public final static String FIELD_SERIESTYPE = "SeriesType";
	public final static String FIELD_RELEASEYEAR_I = "ReleaseYear";
	public final static String FIELD_SERIESENDYEAR_I = "SeriesEndYear";
	public final static String FIELD_ALTTITLES = "AltTitles";
	public final static String FIELD_BUDGET_I = "Budget";
	public final static String FIELD_BUDGETCURRENCY = "BudgetCurrency";
	public final static String FIELD_HIGHGBO_I = "HighGBO";
	public final static String FIELD_HIGHGBOCOUNTRY = "HighGBOCountry";
	public final static String FIELD_HIGHGBODATE_D = "HighGBODate";
	public final static String FIELD_HIGHGBOCURRENCY = "HighGBOCurrency";
	public final static String FIELD_CERTIFICATES = "Certificates";
	public final static String FIELD_MPAA_RATING_REASON = "MPAARatingReason";
	public final static String FIELD_COLORS = "Colors";
	public final static String FIELD_SOUNDS = "Sounds";
	public final static String FIELD_COUNTRIES = "Countries";
	public final static String FIELD_LANGUAGES = "Languages";
	public final static String FIELD_GENRES = "Genres";
	public final static String FIELD_KEYWORDS = "Keywords";
	public final static String FIELD_LOCATIONS = "Locations";
	public final static String FIELD_RATING_F = "Rating";
	public final static String FIELD_RATING_VOTES_I = "RatingVotes";
	public final static String FIELD_RATING_DIST = "RatingDist";
	public final static String FIELD_RUNNING_TIME_I = "RunningTime";

	public final static String FIELD_DOC_SEQ_I = "DocSeq";
	public final static String FIELD_DOC_TYPE = "DocType";
	public final static String FIELD_DOC_TEXT = "DocText";
	public final static String FIELD_DOC_SUBTYPE = "DocSubtype";
	public final static String FIELD_DOC_AUTHOR = "DocAuthor";

	public final static String FIELD_CONTRIB_NAME = "ContribName";
	public final static String FIELD_CONTRIB_ALIASES = "ContribAliases";
	public final static String FIELD_CONTRIB_BIRTH = "ContribBirth";
	public final static String FIELD_CONTRIB_BIRTHYEAR = "ContribBirthYear";
	public final static String FIELD_CONTRIB_DEATH = "ContribDeath";
	public final static String FIELD_CONTRIB_DEATHYEAR = "ContribDeathYear";
	public final static String FIELD_CONTRIB_DEATHCAUSE = "ContribDeathCause";
	public final static String FIELD_CONTRIB_HEIGHT = "ContribHeight";
	public final static String FIELD_CONTRIB_REALNAME = "ContribRealname";
	public final static String FIELD_CONTRIB_SPOUSES = "ContribSpouses";
	public final static String FIELD_CONTRIB_NICKNAMES = "ContribNicknames";
	public final static String FIELD_CONTRIB_BIO = "ContribBio";
	public final static String FIELD_CONTRIB_BIO_VISITED = "ContribBioVisited";

	public final static String FIELD_CONTRIB_CLASS = "ContribClass";
	public final static String FIELD_CONTRIB_ROLE = "ContribRole";
	public final static String FIELD_CONTRIB_ROLEDETAIL = "ContribRoleDetail";

	public final static String FIELD_BUSINESS = "Business";
	public final static String FIELD_TECHNICAL = "Technical";
	public final static String FIELD_RATING = "Rating";
	public final static String FIELD_PARENTAL = "Parental";
	public final static String FIELD_PERSON_DATA = "PersonData";

	public static final String SERIES_FEATURE = "F";
	public static final String SERIES_SERIES = "S";
	public static final String SERIES_EPISODE = "E";

	public static final String COLLECTION_MOVIE = "Movie";
	public static final String COLLECTION_MOVIEDOC = "MovieDoc";
	public static final String COLLECTION_CONTRIBUTOR = "Contributor";
	public static final String COLLECTION_MOVIEROLE = "MovieRole";
	
	public static final String PROP_MONGOHOST = "MongoHost";
	public static final String PROP_MONGOPORT = "MongoPort";
	public static final String PROP_MONGODB = "MongoDB";
	public static final String PROP_IMDBPATH = "IMDBPath";
	public static final String PROP_LOG4J = "LOG4J";
	
	public static final String ERR_MONGO = "MongoDB";
	public static final String ERR_USAGE = "ProgramUsage";
	public static final String ERR_MALFORMED_LINE = "MalformedLine";
	public static final String ERR_UNEXPECTED_LINE = "UnexpectedLine";
	public static final String ERR_FILE = "FileError";
	public static final String ERR_CONVERSION = "DataConversion";
	public static final String ERR_FACTORY = "ParserFactory";
	public static final String ERR_DUPE = "DuplicateRecord";
	public static final String ERR_FIELD_NOT_FOUND = "FieldNotFound";
	public static final String ERR_RECORD_NOT_FOUND = "RecordNotFound";
	public static final String ERR_RECORD_SEMANTIC = "SemanticRecordError";
}
