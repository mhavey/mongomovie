package org.jude.bigdata.recroom.movies.etl;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.mongodb.BasicDBObject;

/**
 * Here's where we run ETL jobs to get data from IMDB files and write to MongoDB
 * 
 * @author Tibcouser
 * 
 */
public class ETLController {

	static final char JOB_MOVIES = 'm';
	static final char JOB_DOCS = 'd';
	static final char JOB_ROLES = 'r';
	static final char JOB_CONTRIBUTORS = 'c';
	static final char[] JOBS_ALL = { JOB_MOVIES, JOB_DOCS, JOB_ROLES,
			JOB_CONTRIBUTORS };

	static final String USAGE = "USAGE: ETLController -props=propsFile -clean=none|all|("
			+ new String(JOBS_ALL)
			+ ") -jobs=none|all|("
			+ new String(JOBS_ALL) + ")+ -log4=path";
	static final String CLEANOPT = "-clean=";
	static final int CLEANOPT_LEN = CLEANOPT.length();
	static final String JOBOPT = "-jobs=";
	static final int JOBOPT_LEN = JOBOPT.length();
	static final String PROPSOPT = "-props=";
	static final int PROPSOPT_LEN = PROPSOPT.length();
	static final String LOG4JOPT = "-log4j=";
	static final int LOG4JOPT_LEN = LOG4JOPT.length();

	String jobs = "";
	String cleans = "";
	MongoDBConnection mConnection = null;
	String imdbFilePath = null;

	Logger logger = Logger.getLogger(ETLController.class);

	/**
	 * Default constructor
	 */
	public ETLController() {
	}

	/**
	 * Configure the controller by lining up its job sequence and configuring
	 * its connectivity to Mongo.
	 * 
	 * @param propsFile
	 * @param jobs
	 * @throws IOException
	 */
	public void configure(String propsFile, String ajobs, String acleans) {

		try {
			// determine order of jobs and if any anomalies

			// 1. Am I doing movies? If so, goes first.
			if (ajobs.indexOf(JOB_MOVIES) >= 0) {
				this.jobs += JOB_MOVIES;
			}

			// 2. And now for all the others
			int jobLen = ajobs.length();
			for (int i = 0; i < jobLen; i++) {
				switch (ajobs.charAt(i)) {
				case JOB_MOVIES:
					break;
				case JOB_DOCS:
				case JOB_ROLES:
				case JOB_CONTRIBUTORS:
					this.jobs += ajobs.charAt(i);
					break;
				default:
					throw new ETLException(ETLConstants.ERR_USAGE,
							"Illegal job " + ajobs.charAt(i));
				}
			}

			logger.info("Controller jobs: *" + jobs + "*");

			cleans = acleans;
			logger.info("Controller cleans: *" + cleans + "*");

			// Load props and setup AS Connection
			ETLProperties props = new ETLProperties();
			props.loadProperties(propsFile);
			mConnection = new MongoDBConnection();
			mConnection.configure(props);
			if (jobs.length() > 0) {
				imdbFilePath = props.getString(ETLConstants.PROP_IMDBPATH);
			}
			logger.info("Controller config OK");
		} catch (ETLException e) {
			ETLException.logError(logger, "Config error", e);
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/**
	 * We have a go! Do the jobs.
	 * 
	 */
	public void go() {
		try {
			// connect to Mongo
			mConnection.connect();

			// run cleans if any
			for (int i = 0; i < cleans.length(); i++) {
				switch (cleans.charAt(i)) {
				case JOB_MOVIES:
					logger.info("clean movies start");
					runClean(ETLConstants.COLLECTION_MOVIE);
					logger.info("clean movies end");
					break;
				case JOB_DOCS:
					logger.info("clean docs start");
					runClean(ETLConstants.COLLECTION_MOVIEDOC);
					logger.info("clean docs end");
					break;
				case JOB_ROLES:
					logger.info("clean roles start");
					runClean(ETLConstants.COLLECTION_MOVIEROLE);
					logger.info("clean roles end");
					break;
				case JOB_CONTRIBUTORS:
					logger.info("clean contrib start");
					runClean(ETLConstants.COLLECTION_CONTRIBUTOR);
					logger.info("clean contrib end");
					break;
				default:
					throw new RuntimeException("Illegal job " + jobs.charAt(i));
				}
			}

			// run the jobs
			for (int i = 0; i < jobs.length(); i++) {
				switch (jobs.charAt(i)) {
				case JOB_MOVIES:
					logger.info("Job CreateMovies start");
					runCreateMovies();
					logger.info("Job CreateMovies end");
					break;
				case JOB_DOCS:
					logger.info("Job AddDocs start");
					runAddDocs();
					logger.info("Job AddDocs end");
					break;
				case JOB_ROLES:
					logger.info("Job AddRoles start");
					runAddRoles();
					logger.info("Job AddRoles end");
					break;
				case JOB_CONTRIBUTORS:
					logger.info("Job CreateContributors start");
					runCreateContributors();
					logger.info("Job CreateContributors end");
					break;
				default:
					throw new RuntimeException("Illegal job " + jobs.charAt(i));
				}
			}
		} catch (ETLException e) {
			ETLException.logError(logger, "Error executing jobs", e);
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			// disconnect from Mongo
			try {
				mConnection.disconnect();
			} catch (ETLException de) {
				ETLException.logError(logger, "Error disconnecting from Mongo",
						de);
				throw new RuntimeException(de.getMessage(), de);
			}
		}
	}

	void runClean(String collName) throws ETLException {
		mConnection.dropCollection(collName);
	}

	/**
	 * Run the create movies job
	 * 
	 * @throws ETLException
	 */
	void runCreateMovies() throws ETLException {

		String movieKey[] = { ETLConstants.FIELD_MOVIEID };

		// first, the main movie file; read it and dump to movie stage space
		// also do an RI check of episode vs. series
		ImdbIterator creationIterator = new ImdbIterator("movies", imdbFilePath);
		String lastSeries = null;
		while (!creationIterator.isEOF()) {
			BasicDBObject next = creationIterator.nextJSON();
			if (next != null) {
				try {
					String series = MongoDBConnection.getMandatoryString(next,
							ETLConstants.FIELD_SERIESID);
					String type = MongoDBConnection.getMandatoryString(next,
							ETLConstants.FIELD_SERIESTYPE);
					if (lastSeries == null || !lastSeries.equals(series)) {
						lastSeries = series;
						if (type.equals(ETLConstants.SERIES_SERIES)
								|| type.equals(ETLConstants.SERIES_FEATURE)) {
							mConnection.insertToCollection(
									ETLConstants.COLLECTION_MOVIE, next,
									ETLConstants.FIELD_MOVIEID);
						} else {
							throw new ETLException(
									ETLConstants.ERR_RECORD_SEMANTIC,
									"Movie introducing new series must be series or feature");
						}
					} else {
						if (type.equals(ETLConstants.SERIES_EPISODE)) {
							mConnection.insertToCollection(
									ETLConstants.COLLECTION_MOVIE, next,
									ETLConstants.FIELD_MOVIEID);
						} else {
							throw new ETLException(
									ETLConstants.ERR_RECORD_SEMANTIC,
									"Movie in same series must be an episode");
						}
					}
				} catch (ETLException e) {
					creationIterator.logReject(e);
				}
			}
		}
		creationIterator.logResult();

		// next, process each update file;
		// these files have movie update
		String[] files = { "aka-titles", null, "business", null,
				"certificates", null, "color-info", null, "countries", null,
				"genres", null, "keywords", null, "language", null,
				"locations", null, "mpaa-ratings-reasons", "true", "ratings",
				null, "running-times", null, "sound-mix", "true" };

		for (int i = 0; i < files.length; i += 2) {
			ImdbIterator updateIterator = new ImdbIterator(files[i],
					imdbFilePath);
			boolean updateSubdoc = (files[i + 1] != null);
			while (!updateIterator.isEOF()) {
				BasicDBObject next = updateIterator.nextJSON();
				if (next != null) {
					try {
						boolean upsert = false;
						mConnection.updateCollection(
								ETLConstants.COLLECTION_MOVIE, next, movieKey,
								upsert, updateSubdoc, null);
					} catch (ETLException e) {
						updateIterator.logReject(e);
					}
				}
			}
			updateIterator.logResult();
		}
	}

	/**
	 * Run the adddocs job
	 * 
	 * @throws ERLException
	 */
	void runAddDocs() throws ETLException {

		String movieKey[] = { ETLConstants.FIELD_MOVIEID };

		// these files have docs data
		String[] files = { "alternate-versions", "crazy-credits", "goofs",
				"literature", "plot", "quotes", "soundtracks", "taglines",
				"trivia" };

		for (int i = 0; i < files.length; i++) {
			ImdbIterator iterator = new ImdbIterator(files[i], imdbFilePath);
			while (!iterator.isEOF()) {
				BasicDBObject next = iterator.nextJSON();
				if (next != null) {

					try {
						// check if movie exists
						mConnection.checkExists(ETLConstants.COLLECTION_MOVIE,
								next, movieKey, true);

						// insert the moviedox
						mConnection.insertToCollection(
								ETLConstants.COLLECTION_MOVIEDOC, next, null);
					} catch (ETLException e) {
						iterator.logReject(e);
					}
				}
			}
			iterator.logResult();
		}
	}

	/**
	 * Run the addroles job
	 * 
	 * @throws ERLException
	 */
	void runAddRoles() throws ETLException {
		String movieKey[] = { ETLConstants.FIELD_MOVIEID };
		String contribKey[] = { ETLConstants.FIELD_CONTRIB_NAME };

		// these files have docs data
		String[] files = { "actors", "actresses", "cinematographers",
				"composers", "costume-designers", "directors", "distributors",
				"editors", "miscellaneous-companies", "miscellaneous",
				"producers", "production-companies", "production-designers",
				"special-effects-companies", "writers" };

		for (int i = 0; i < files.length; i++) {

			// open the next file
			ImdbIterator creationIterator = new ImdbIterator(files[i],
					imdbFilePath);
			while (!creationIterator.isEOF()) {
				BasicDBObject next = creationIterator.nextJSON();
				if (next != null) {
					try {
						// check if movie exists
						mConnection.checkExists(ETLConstants.COLLECTION_MOVIE,
								next, movieKey, true);

						// add it to collection; it has a unique index check on
						// movie+contrib+contribclass
						mConnection.insertToCollection(
								ETLConstants.COLLECTION_MOVIEROLE, next, null);

						// Contributor RI check? - no! IMDB has no master list
						// of contributors, so let's
						// upsert one now
						String contribName = MongoDBConnection
								.getMandatoryString(next,
										ETLConstants.FIELD_CONTRIB_NAME);
						mConnection.updateCollection(
								ETLConstants.COLLECTION_CONTRIBUTOR,
								new BasicDBObject().append(
										ETLConstants.FIELD_CONTRIB_NAME,
										contribName), contribKey, true, false,
								ETLConstants.FIELD_CONTRIB_NAME);

					} catch (ETLException e) {
						creationIterator.logReject(e);
					}

				}

			}
			creationIterator.logResult();
		}
	}

	/**
	 * Run the create contributors job
	 * 
	 * @throws ERLException
	 */
	void runCreateContributors() throws ETLException {
		String contribKey[] = { ETLConstants.FIELD_CONTRIB_NAME };

		// 1. let's do aka-names first; it's the skinnier one
		ImdbIterator creationIterator = new ImdbIterator("aka-names",
				imdbFilePath);
		while (!creationIterator.isEOF()) {
			BasicDBObject next = creationIterator.nextJSON();
			if (next != null) {
				try {
					mConnection.updateCollection(
							ETLConstants.COLLECTION_CONTRIBUTOR, next,
							contribKey, true, false,
							ETLConstants.FIELD_CONTRIB_NAME);

				} catch (ETLException e) {
					creationIterator.logReject(e);
				}
			}
		}
		creationIterator.logResult();

		// 2. next we'll add biographies, merging with aliases
		ImdbIterator updateIterator = new ImdbIterator("biographies",
				imdbFilePath);
		while (!updateIterator.isEOF()) {
			BasicDBObject next = updateIterator.nextJSON();
			if (next != null) {
				try {
					mConnection.updateCollection(
							ETLConstants.COLLECTION_CONTRIBUTOR, next,
							contribKey, true, false,
							ETLConstants.FIELD_CONTRIB_NAME);

				} catch (ETLException e) {
					updateIterator.logReject(e);
				}
			}
		}
		updateIterator.logResult();

	}

	/**
	 * 
	 * Mainline for ETL controller.
	 * 
	 * @param args
	 * @throws ETLException
	 */
	public static void main(String[] args) {

		if (args.length != 4 || !args[0].startsWith(PROPSOPT)
				|| !args[1].startsWith(CLEANOPT) || !args[2].startsWith(JOBOPT)
				|| !args[3].startsWith(LOG4JOPT)) {
			throw new RuntimeException(USAGE);
		}

		// Determine props file
		String propsFile = args[0].substring(PROPSOPT_LEN).trim();

		// Determine clean
		String cleanOpt = args[1].substring(CLEANOPT_LEN).trim();
		if (cleanOpt.equals("all")) {
			cleanOpt = new String(JOBS_ALL);
		}
		if (cleanOpt.equals("none")) {
			cleanOpt = "";
		}

		// Determine jobs
		String jobsOpt = args[2].substring(JOBOPT_LEN).trim();
		if (jobsOpt.equals("all")) {
			jobsOpt = new String(JOBS_ALL);
		}
		if (jobsOpt.equals("none")) {
			jobsOpt = "";
		}

		// Configure log4j
		PropertyConfigurator.configure(args[3].substring(LOG4JOPT_LEN).trim());

		// ready to go
		ETLController controller = new ETLController();
		controller.configure(propsFile, jobsOpt, cleanOpt);
		controller.go();
	}
}
